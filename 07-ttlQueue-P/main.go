package main

import (
	"awesomeProject/common/mqUtils"
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

var ch *amqp.Channel

func main() {

	for {
		var val string
		fmt.Scanf("%s", &val)
		if val != "" && val != "/n" && val != "/t" {
			publishMessage(val, QUEUE_QA)
			publishMessage(val, QUEUE_QB)
		}
	}

}

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

// 初始化
func init() {
	url := fmt.Sprintf("amqp://%s:%s@%s:5672/", mqUtils.MQ_USER, mqUtils.MQ_PWD, mqUtils.MQ_ADDR)
	con, err := amqp.Dial(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	ch, err = con.Channel()
	if err != nil {
		fmt.Println(err)
		return
	}

	//声明交换机及队列
	//普通交换机
	err = ch.ExchangeDeclare(
		TEMP_EXCHANGE,
		EXCHANGE_DIRECT,
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	//死信交换机
	err = ch.ExchangeDeclare(
		DEAD_EXCHANGE,
		EXCHANGE_DIRECT,
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	//声明普通队列QA,QB和QY
	args := amqp.Table{
		"x-dead-letter-exchange":    DEAD_EXCHANGE, //死信队列交换机
		"x-dead-letter-routing-key": ROUTINGKEY_YD, //死信队列routing key
	}
	qa, err := ch.QueueDeclare(
		QUEUE_QA,
		false,
		false,
		false,
		false,
		args)
	if err != nil {
		fmt.Println(err)
		return
	}

	qb, err := ch.QueueDeclare(
		QUEUE_QB,
		false,
		false,
		false,
		false,
		args)
	if err != nil {
		fmt.Println(err)
		return
	}

	qd, err := ch.QueueDeclare(
		QUEUE_QD,
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	//队列与交换机绑定
	err = ch.QueueBind(
		qa.Name,
		ROUTINGKEY_XA,
		TEMP_EXCHANGE,
		false,
		nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = ch.QueueBind(
		qb.Name,
		ROUTINGKEY_XB,
		TEMP_EXCHANGE,
		false,
		nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = ch.QueueBind(
		qd.Name,
		ROUTINGKEY_YD,
		DEAD_EXCHANGE,
		false,
		nil)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func publishMessage(msg string, queueName string) {
	if ch == nil {
		fmt.Println("channel is nil")
		return
	}
	qa_ttl := "1000"
	qb_ttl := "4000"
	switch queueName {
	case QUEUE_QA:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := ch.PublishWithContext(
			ctx,
			TEMP_EXCHANGE,
			ROUTINGKEY_XA,
			false,
			false,
			amqp.Publishing{
				Body:        []byte(msg),
				ContentType: "text/plain",
				Expiration:  qa_ttl,
			})
		if err == nil {
			fmt.Printf("%d send to qa %s \n", time.Now().Unix(), msg)
		}
	case QUEUE_QB:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := ch.PublishWithContext(
			ctx,
			TEMP_EXCHANGE,
			ROUTINGKEY_XB,
			false,
			false,
			amqp.Publishing{
				Body:        []byte(msg),
				ContentType: "text/plain",
				Expiration:  qb_ttl,
			})
		if err == nil {
			fmt.Printf("%d send to qb %s \n", time.Now().Unix(), msg)
		}
	}
}
