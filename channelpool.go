package rabbitmq

import (
	"fmt"

	"github.com/techquest-tech/go-amqp-reconnect/rabbitmq"
)

func (c Settings) Factory() (interface{}, error) {
	cnn, err := c.Connect()
	if err != nil {
		return nil, err
	}
	return cnn.Channel()
}

func (c Settings) Ping(v interface{}) error {
	ch := v.(*rabbitmq.Channel)
	bl := ch.IsClosed()
	if bl {
		return fmt.Errorf("channel closed")
	}
	return nil
}

func (c Settings) Close(v interface{}) error {
	ch := v.(*rabbitmq.Channel)
	return ch.Close()
}
