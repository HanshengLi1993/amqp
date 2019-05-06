#                   AMQP相关工具类代码封装

## 一、multiplexampqconnection

复用AMQP的TCP连接，创建复数个通道，减少流量高登峰期时的TCP连接连接资源。