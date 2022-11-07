package main

import (
	"awesomeProject/common/mqUtils"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

const (
	//交换机
	TEMP_EXCHANGE = "temp_exchange"
	//死信交换机
	DEAD_EXCHANGE = "dead_exchange"
	//普通队列
	QUEUE_QA      = "QA"
	ROUTINGKEY_XA = "XA"
	QUEUE_QB      = "QB"
	ROUTINGKEY_XB = "XB"
	//死信队列
	QUEUE_QD      = "QD"
	ROUTINGKEY_YD = "YD"

	EXCHANGE_DIRECT = "direct"
)

func main() {
	url := fmt.Sprintf("amqp://%s:%s@%s:5672/", mqUtils.MQ_USER, mqUtils.MQ_PWD, mqUtils.MQ_ADDR)
	con, err := amqp.Dial(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	ch, err := con.Channel()
	if err != nil {
		fmt.Println(err)
		return
	}

	msgs, err := ch.Consume(
		QUEUE_QD,
		"ttl_c",
		true,
		false,
		false,
		false,
		nil)

	forever := make(chan interface{})
	go func() {
		for msg := range msgs {
			fmt.Printf("%d receive msg---> %s\n", time.Now().Unix(), string(msg.Body))
		}
	}()
	<-forever
}
