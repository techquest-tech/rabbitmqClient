package rabbitmq

import (
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

var democonn = Settings{
	Host:     "127.0.0.1",
	Port:     5672,
	User:     "guest",
	Password: "guest",
	Vhost:    "/",
	Prop: map[string]interface{}{
		"connection_name": "demo-v20201106.1",
	},
}

func TestDemoSend(t *testing.T) {

	dest := MqDestination{
		Topic:        "escm.po",
		ExchangeType: "headers",
		DeclareAll:   true,
	}

	cnn, err := democonn.Connect()

	if err != nil {
		t.Error("connect to rabbitmq failed.", err)
		t.Fail()
	}
	defer cnn.Close()

	err = dest.DeclareDestination(cnn, false)
	assert.Nil(t, err)

	ch, err := cnn.Channel()

	assert.Nil(t, err)

	defer ch.Close()

	err = dest.Produce(ch, amqp.Publishing{
		Body: []byte("hello world for no one"),
		Headers: amqp.Table{
			// "factory": "none",
			"brand": "b",
		},
	})

	assert.Nil(t, err)

	err = dest.Produce(ch, amqp.Publishing{
		Body: []byte("hello world for get"),
		Headers: amqp.Table{
			"factory": "get",
			"brand":   "b",
		},
		DeliveryMode: amqp.Persistent,
	})

	assert.Nil(t, err)

	err = dest.Produce(ch, amqp.Publishing{
		Body: []byte("hello world for ymg"),

		Headers: amqp.Table{
			"factory": "ymg",
			"brand":   "a",
		},
	})
	assert.Nil(t, err)
}

var ErrorEnabled bool = true

type DemoConsumer struct{}

func (d DemoConsumer) OnReceiveMessage(msg amqp.Delivery) (string, *amqp.Publishing, error) {
	logrus.Info(string(msg.Body))
	// time.Sleep(5 * time.Second)
	if ErrorEnabled {
		logrus.Error("throw exception for demo only")
		return "", nil, fmt.Errorf("demo error when process message")
	}
	return "", nil, nil
}

func TestDemoConsumer(t *testing.T) {
	dest := &MqDestination{
		Queue: "demo.helloworld",
		// AutoAck: true,
	}

	StartConsumer(dest, DemoConsumer{}, &democonn)

	time.Sleep(1 * time.Second)
}
