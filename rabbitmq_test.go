package rabbitmq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/creasty/defaults"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

var connSetting = Settings{
	// Host:     "127.0.0.1",
	// Port:     5672,
	// User:     "guest",
	// Password: "guest",
	// Vhost:    "/",
	Prop: map[string]interface{}{
		"connection_name": "addon-retry-v20201106.1",
	},
}

var dest = Destination{
	Topic:    "helloworld",
	Queue:    "demo.helloworld",
	Prefetch: 1,
	// DeclareAll: true,
	// AutoAck:    true,
}

var destRPC = Destination{
	Queue:   "test.rpc",
	AutoAck: true,
}

func TestProducer(t *testing.T) {
	defaults.Set(&connSetting)

	assert.Equal(t, "localhost", connSetting.Host)

	conn, err := connSetting.Connect()

	if err != nil {
		t.Error("connect to rabbitmq failed.", err)
		t.Fail()
	}
	defer conn.Close()
	ch, err := conn.Channel()

	dest.DeclareDestination(ch, false)

	if err != nil {
		t.Error("setup channel failed.", err)
		t.Fail()
	}
	defer ch.Close()

	for i := 0; i < 1000; i++ {
		dest.Produce(ch, amqp.Publishing{
			Body: []byte(fmt.Sprintf("Testing message at %v,message#%d", time.Now(), i)),
		})
	}
	conn.Close()
}

type ConsoleConsumer struct{}

func (cc ConsoleConsumer) OnReceiveMessage(msg amqp.Delivery) (string, *amqp.Publishing, error) {
	logrus.Info(string(msg.Body))
	// time.Sleep(5 * time.Second)
	return "", nil, nil
}

func TestStartConsumer(t *testing.T) {
	cnn, err := connSetting.Connect()
	assert.NoError(t, err)
	ch, err := cnn.Channel()
	assert.NoError(t, err)
	StartConsumer(&dest, ConsoleConsumer{}, ch, "Demo")
	time.Sleep(3 * time.Second)
}

type RPCConsumer struct{}

var sleepfor = time.Duration(2)

func (r RPCConsumer) OnReceiveMessage(msg amqp.Delivery) (string, *amqp.Publishing, error) {
	logrus.Info(string(msg.Body))
	time.Sleep(sleepfor * time.Second)
	return "", WrapRepo(msg, []byte("replied messages"), nil), nil
}

func TestRpc(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	conn, err := connSetting.Connect()
	assert.NoError(t, err)
	ch, err := conn.Channel()
	assert.NoError(t, err)

	destRPC.DeclareDestination(ch, false)

	if err != nil {
		t.Error("connect to rabbitmq failed.", err)
		t.Fail()
	}
	defer conn.Close()

	go StartConsumer(&destRPC, RPCConsumer{}, ch, "testRPC")

	reqBody := []byte("Testing RPC")

	req := amqp.Publishing{
		Body: reqBody,
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)

	defer cancel()

	ch2, _ := conn.Channel()

	sleepfor = 1
	replied, err := destRPC.RPC(ctx, ch2, req)

	assert.Nil(t, err)
	assert.NotNil(t, replied)

	ctx2, cancel2 := context.WithTimeout(context.TODO(), 2*time.Second)
	defer cancel2()

	sleepfor = 3
	ch3, _ := conn.Channel()
	replied, err = destRPC.RPC(ctx2, ch3, req)

	assert.NotNil(t, err)
	assert.Nil(t, replied)

	// time.Sleep(20 * time.Second)

}
