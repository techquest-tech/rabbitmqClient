package rabbitmq

import (
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var connSetting = Settings{
	Host:     "127.0.0.1",
	Port:     5672,
	User:     "guest",
	Password: "guest",
	Vhost:    "/",
	Prop: map[string]interface{}{
		"connection_name": "addon-retry-v20201106.1",
	},
}

var dest = MqDestination{
	Topic: "ping",
	Queue: "test.ping2",
	// DeclareAll: true,
	// AutoAck:    true,
}

func TestProducer(t *testing.T) {

	conn, err := connSetting.Connect()

	dest.DeclareDestination(conn, false)

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

	for i := 0; i < 100; i++ {
		dest.Produce(ch, amqp.Publishing{
			Body: []byte(fmt.Sprintf("Testing message at %v,message#%d", time.Now(), i)),
		})
	}
	conn.Close()
}

type ConsoleConsumer struct{}

func (cc ConsoleConsumer) OnReceiveMessage(msg amqp.Delivery) (string, *amqp.Publishing, error) {
	logrus.Info(string(msg.Body))
	return "", nil, nil
}

func TestStartConsumer(t *testing.T) {
	StartConsumer(&dest, ConsoleConsumer{}, &connSetting)
	time.Sleep(2 * time.Second)
}
