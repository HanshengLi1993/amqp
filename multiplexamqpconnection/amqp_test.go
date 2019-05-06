package util

import (
	"testing"
	"github.com/streadway/amqp"
)

func BenchmarkPublish(b *testing.B) {
	amqpClient := NewClient("amqp://guest:guest@127.0.0.1:5672/", "test-", 100)
	amqpChannel := &ChannelContext{
		Exchange:     "test",
		ExchangeType: "topic",
		Reliable:     true, //ack
		Durable:      true,
	}
	for i := 0; i < b.N; i++ {
		amqpClient.Publish(amqpChannel, "message for test", nil)
	}
}

func TestSubscribe(t *testing.T) {
	amqpClient := NewClient("amqp://guest:guest@127.0.0.1:5672/", "test-", 100)
	amqpChannel := &ChannelContext{
		Exchange:     "test",
		ExchangeType: "topic",
		RoutingKey:   "test.update",
		Queue:        "test",
		Reliable:     true, //ack
		Durable:      true,
	}
	var handleMessage MessageHandler = func(message amqp.Delivery) bool {
		return true
	}
	for i := 0; i < 100; i++ {
		amqpClient.Subscribe(amqpChannel, handleMessage)
	}
}
