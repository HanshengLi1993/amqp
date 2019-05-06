package util

import (
	"log"
	"sync"
	"time"
	"bytes"
	"errors"
	"strconv"
	"crypto/md5"
	"encoding/hex"
	"github.com/streadway/amqp"
)

const (
	reconnectDelay = 5 * time.Second // 连接断开后多久重连
)

type ConnectionContext struct {
	Lock        sync.RWMutex
	Connection  *amqp.Connection
	BrokerUrl   string
	notifyClose chan *amqp.Error
}
type ChannelContext struct {
	Fingerprint   string
	Exchange      string
	ExchangeType  string
	RoutingKey    string
	Queue         string
	Reliable      bool
	Durable       bool
	kill          chan bool
	IsConnected   chan bool
	Channel       *amqp.Channel
	notifyClose   chan *amqp.Error
	notifyConfirm chan amqp.Confirmation
}
type AMQPClient struct {
	PrefixClientId    string
	ConnectionContext *ConnectionContext
	ChannelContexts   chan *ChannelContext // channel cache
	isConnected       bool
	MAXChannel        uint
	inUsedChannel     chan *ChannelContext
	done              chan bool
}

type MessageHandler func(message amqp.Delivery) bool
type MessageConfirmation func(ack bool) bool

// url = "amqp://guest:guest@127.0.0.1:5672/"
func NewClient(url, prefixClientId string, num uint) *AMQPClient {
	amqpClient := AMQPClient{
		ConnectionContext: &ConnectionContext{
			BrokerUrl: url,
		},
		ChannelContexts: make(chan *ChannelContext, num),
		MAXChannel:      num,
		inUsedChannel:   make(chan *ChannelContext, num),
		done:            make(chan bool),
		PrefixClientId:  prefixClientId,
	}

	go amqpClient.refreshConnect()
	return &amqpClient
}

func (ac *AMQPClient) refreshConnect() {
	for {
		ac.isConnected = false
		log.Println("Attempting to connect RabbitMQ server")
		for !ac.connect() {
			log.Println("Connect to RabbitMQ server failed. Retrying...")
			time.Sleep(reconnectDelay)
		}
		select {
		case <-ac.done:
			return
		case <-ac.ConnectionContext.notifyClose:
		}
	}
}

func (ac *AMQPClient) connect() bool {
	conn, err := amqp.Dial(ac.ConnectionContext.BrokerUrl)
	if err != nil {
		return false
	}
	ac.ConnectionContext.Connection = conn
	ac.changeConnection(ac.ConnectionContext)
	ac.isConnected = true
	log.Println("RabbitMQ connect successfuly!")
	return true
}

// 监听Rabbit channel的状态
func (ac *AMQPClient) changeConnection(connection *ConnectionContext) {
	ac.ConnectionContext = connection
	ac.ConnectionContext.notifyClose = make(chan *amqp.Error)
	ac.ConnectionContext.Connection.NotifyClose(ac.ConnectionContext.notifyClose)
}

//计算channel的指纹值
func (ac *AMQPClient) generateFingerprint(channelContext *ChannelContext) string {
	var tag bytes.Buffer
	tag.WriteString(channelContext.Exchange)
	tag.WriteString(":")
	tag.WriteString(channelContext.ExchangeType)
	tag.WriteString(":")
	tag.WriteString(channelContext.RoutingKey)
	tag.WriteString(":")
	tag.WriteString(channelContext.Queue)
	tag.WriteString(":")
	tag.WriteString(strconv.FormatBool(channelContext.Reliable))
	tag.WriteString(":")
	tag.WriteString(strconv.FormatBool(channelContext.Durable))
	tag.WriteString(":")
	hash := md5.New()
	hash.Write(tag.Bytes())
	return hex.EncodeToString(hash.Sum(nil))
}

//新建channel
func (ac *AMQPClient) newChannel(channelContext *ChannelContext) error {
	if ac.isConnected == true {
		go ac.refreshChannel(channelContext)
	} else {
		return errors.New("Loss RabbitMQ connection")
	}
	return nil
}

// 监听channel状态，自动重连
func (ac *AMQPClient) refreshChannel(channelContext *ChannelContext) {
	for {
		for {
			c, err := ac.channel(channelContext)
			if err != nil {
				if ac.isConnected {
					break
				}
				log.Println("Failed to build channel. Retrying...")
				time.Sleep(reconnectDelay)
			} else {
				channelContext = c
				channelContext.IsConnected <- true
				break
			}
		}

		select {
		case <-ac.done:
			return
		case <-ac.ConnectionContext.notifyClose:
			return
		case <-channelContext.kill:
			return
		case <-channelContext.notifyClose:
		}
	}
}

func (ac *AMQPClient) channel(channelContext *ChannelContext) (*ChannelContext, error) {
	ac.ConnectionContext.Lock.Lock()
	defer ac.ConnectionContext.Lock.Unlock()

	if !ac.isConnected {
		return nil, errors.New("Loss RabbitMQ connection")
	}
	ch, err := ac.ConnectionContext.Connection.Channel()
	if err != nil {
		log.Printf("Create RabbitMQ channel failed,caused by:%v\n", err)
		return nil, err
	}
	channelContext.Channel = ch
	channelContext.Fingerprint = ac.generateFingerprint(channelContext)

	if err := channelContext.Channel.ExchangeDeclare(
		channelContext.Exchange,     // Exchange
		channelContext.ExchangeType, // ExchangeType
		channelContext.Durable,      // Durable
		false,                       // auto-deleted
		false,                       // internal
		false,                       // noWait
		nil,                         // arguments
	); err != nil {
		log.Println("channel exchange declare failed refreshConnectionAndChannel again", err)
		return nil, err
	}

	if channelContext.Reliable {
		if err := channelContext.Channel.Confirm(false); err != nil {
			log.Println("Channel could not be put into confirm mode: %s", err)
			return nil, err
		}
		channelContext.notifyConfirm = channelContext.Channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	} else {
		if channelContext.notifyConfirm != nil {
			close(channelContext.notifyConfirm)
		}
	}
	ac.changeChannel(channelContext)

	return channelContext, nil
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func (ac *AMQPClient) confirmOne(confirms <-chan amqp.Confirmation, mc MessageConfirmation) {
	//log.Printf("waiting for confirmation of one publishing")
	if mc != nil {
		if confirmed := <-confirms; confirmed.Ack {
			//log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
			mc(true)
		} else {
			mc(false)
			//log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
		}
	} else {
		<-confirms
	}
}

func (ac *AMQPClient) getChannel(channelContext *ChannelContext) (*ChannelContext, error) {
	if !ac.isConnected {
		return nil, errors.New("Connect to RabbitMQ failed")
	}
	var availableChannel *ChannelContext

	poolDepth := len(ac.ChannelContexts)
	inUsedChannelDepth := len(ac.inUsedChannel)
	if poolDepth > 0 {
		fingerprint := ac.generateFingerprint(channelContext)
		for index := 0; index < poolDepth; index++ {
			availableChannel = <-ac.ChannelContexts
			if fingerprint == availableChannel.Fingerprint {
				ac.inUsedChannel <- availableChannel
				return availableChannel, nil
			} else {
				ac.ChannelContexts <- availableChannel
			}
		}
		if poolDepth+inUsedChannelDepth >= int(ac.MAXChannel) {
			// 指纹都不一致且channel池已满，则关闭最旧的channel，新建一个
			availableChannel = <-ac.ChannelContexts
			availableChannel.kill <- true

		}
	}

	if ac.MAXChannel-uint(inUsedChannelDepth) > 0 {
		err := ac.newChannel(channelContext)
		if err != nil {
			return nil, err
		}
		availableChannel = channelContext
		ac.inUsedChannel <- availableChannel
		select {
		case <-availableChannel.IsConnected:
			return availableChannel, nil
		}
	} else {
		return nil, errors.New("Create too many channels for RabbitMQ")
	}

}

func (ac *AMQPClient) releaseChannel(channelContext *ChannelContext) {
	if channelContext != nil && channelContext.Channel != nil {
		<-ac.inUsedChannel
		ac.ChannelContexts <- channelContext
	}
}

// 监听Rabbit channel的状态
func (ac *AMQPClient) changeChannel(c *ChannelContext) {
	c.kill = make(chan bool)
	c.IsConnected = make(chan bool)
	c.notifyClose = make(chan *amqp.Error)
	c.Channel.NotifyClose(c.notifyClose)
}

// 关闭连接/信道
func (ac *AMQPClient) Close() error {
	var err error

	if !ac.isConnected {
		return errors.New("RabbitMQ connection already closed: not connected to the producer")
	}
	for ch := range ac.ChannelContexts {
		err = ch.Channel.Close()
		if err != nil {
			return err
		}
	}
	err = ac.ConnectionContext.Connection.Close()
	if err != nil {
		return err
	}
	err = ac.ConnectionContext.Connection.Close()
	if err != nil {
		return err
	}

	close(ac.done)
	ac.isConnected = false
	return nil
}

// 发送端不尝试建立队列，直接向已存在的队列发送消息，若队列不存在，则消息丢失
// 为加强容灾能力可在此方法内加入三次重试机制
// 生产者只向交换机发送消息，不需要知道消息会被缓存到哪个队列中
func (ac *AMQPClient) Publish(newChannelContext *ChannelContext, body string, mc MessageConfirmation) error {
	var err error

	channelContext, err := ac.getChannel(newChannelContext)
	defer ac.releaseChannel(channelContext)
	if err != nil {
		log.Printf("Get RabbitMQ channel failed:caused by:%v\n", err)
		return err
	}

	if channelContext.Reliable {
		defer ac.confirmOne(channelContext.notifyConfirm, mc)
	}
	if err := channelContext.Channel.Publish(
		channelContext.Exchange,   // publish to an exchange
		channelContext.RoutingKey, // routing to 0 or more queues RoutingKey
		false,                     // mandatory
		false,                     // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "utf-8",
			Body:            []byte(body),
			DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
			Expiration:      "604800000",     //ms 7days
		},
	); err != nil {
		log.Printf("Publish message to RabbitMQ failed,caused by:%v\n", err)
		return err
	}
	return nil
}

// 订阅端尝试建立队列，若如果客户端尝试建立一个已经存在的消息队列，Rabbit MQ不会做任何事情，并返回客户端建立成功的。
// 一个消费者在一个信道中正在监听某一个队列的消息，Rabbit MQ是不允许该消费者在同一个channel去声明其他队列的
// 消费失败重试实现不做
func (ac *AMQPClient) Subscribe(channelContext *ChannelContext, handleMessage MessageHandler) error {
	var err error

	channelContext, err = ac.getChannel(channelContext)
	defer ac.releaseChannel(channelContext)
	if err != nil {
		return err
	}

	// 订阅的客户端不需要反复获取channel,保持与channel的长时间绑定
	if channelContext.Queue != "" {
		queue, err := channelContext.Channel.QueueDeclare(
			channelContext.Queue,
			channelContext.Durable,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			log.Printf("Failed to declare a queue")
			return err
		}
		err = channelContext.Channel.QueueBind(
			queue.Name,
			channelContext.RoutingKey,
			channelContext.Exchange,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	messages, err := channelContext.Channel.Consume(
		channelContext.Queue,                                    // queue
		ac.PrefixClientId+strconv.FormatInt(GetTimestamp(), 10), // consumer
		false,                                                   // auto-ack
		false,                                                   // exclusive
		false,                                                   // no-local
		false,                                                   // no-wait
		nil,                                                     // args
	)
	if err != nil {
		log.Printf("Failed to register a consumer")
		return err
	}

	forever := make(chan bool)

	go func() {
		for message := range messages {
			// 直到数据处理成功再返回，然后回复RabbitMQ ACK
			if handleMessage != nil && handleMessage(message) {
				// 确认收到本条消息，multiple必须为false
				message.Ack(false)
			}
		}
	}()

	<-forever
	return nil
}

func GetTimestamp() int64 {
	return time.Now().UnixNano() / 1e6
}
