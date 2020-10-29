package rabbitmq

import (
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func TestProducer(t *testing.T) {
	connSetting := Settings{
		Host:     "127.0.0.1",
		Port:     5672,
		User:     "guest",
		Password: "guest",
		Vhost:    "/",
	}

	dest := MqDestination{
		Topic:      "hello.world",
		Queue:      "i.am.the.king.of.the.world",
		DeclareAll: true,
		// AutoAck:    true,
	}

	conn, err := connSetting.Connect()
	if err != nil {
		t.Error("connect to rabbitmq failed.", err)
		t.Fail()
	}
	defer conn.Close()
	ch, err := conn.Channel()

	if err != nil {
		t.Error("setup channel failed.", err)
		t.Fail()
	}
	defer ch.Close()

	for i := 0; i < 10; i++ {
		dest.Produce(ch, amqp.Publishing{
			Body: []byte(fmt.Sprintf("Testing message at %v,message#%d", time.Now(), i)),
		})
	}

	msgs, ch2, err := dest.Consume(conn)
	if err != nil {
		t.Error("failed when try consumer.")
		t.Fail()
	}

	defer ch2.Close()

	go func() {
		for msg := range msgs {
			logrus.Info(string(msg.Body))
			msg.Ack(true)
		}
	}()

	time.Sleep(3 * time.Second)
	conn.Close()

}
