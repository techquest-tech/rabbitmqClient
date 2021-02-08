package rabbitmq

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/techquest-tech/go-amqp-reconnect/rabbitmq"
)

//Destination Rabbitmq destination
type Destination struct {
	Queue        string
	Topic        string
	ExchangeType string
	AutoAck      bool
	Exclusive    bool
	Prefetch     int
	DeclareAll   bool
}

// //default RPC timeout to 30 seconds
// var defaultTimeout = 30 * time.Second

//DeclareDestination declare Topic, queues....
func (mq *Destination) DeclareDestination(cnn *rabbitmq.Connection, createTempQueue bool) error {
	logger := logrus.WithFields(logrus.Fields{
		"topic": mq.Topic,
		"queue": mq.Queue,
	})

	channel, err := cnn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	autoDelete := false
	if createTempQueue && mq.Queue == "" {
		mq.Queue = fmt.Sprintf("tmp.%s.%d", mq.Topic, rand.Intn(10000))
		logger.Info("user tempate queue.")
		autoDelete = true
	}
	if mq.Queue != "" {
		queue, err := channel.QueueDeclare(mq.Queue,
			true,
			autoDelete,
			mq.Exclusive,
			false,
			nil)
		if err != nil {
			logger.Error("declare queue failed, ", err)
			return err
		}
		logger.Infof("declare queue %s done.", queue.Name)
	}

	if mq.Topic != "" {
		if mq.ExchangeType == "" {
			mq.ExchangeType = "topic"
		}
		err := channel.ExchangeDeclare(mq.Topic, mq.ExchangeType, true, false, false, false, nil)
		if err != nil {
			logger.Error("declare topic failed.", err)
			return err
		}
		logger.Info("declare done")
	}
	if mq.Queue != "" && mq.Topic != "" {
		//declare bind
		err := channel.QueueBind(mq.Queue, "#", mq.Topic, false, nil)
		if err != nil {
			logger.Error("declare Bind failed.", err)
			return err
		}
		logger.Infof("declare bind %s to %s done", mq.Topic, mq.Queue)
	}

	return nil
}

//Consume start consumer
func (mq *Destination) Consume(conn *rabbitmq.Connection, consumerTag string) (<-chan amqp.Delivery, *rabbitmq.Channel, error) {

	logger := logrus.WithFields(logrus.Fields{
		"topic": mq.Topic,
		"queue": mq.Queue,
	})

	logger.Info("start consumer.")

	ch, err := conn.Channel()
	if err != nil {
		logger.Errorf("build channel failed.%v", err)
		return nil, ch, err
	}
	// defer ch.Close()
	// make prefetch default =1
	if mq.Prefetch <= 0 {
		mq.Prefetch = 1
	}

	// if mq.Prefetch > 0 {
	err = ch.Qos(mq.Prefetch, 0, false)
	if err != nil {
		return nil, ch, err
	}
	logger.Info("set prefetch size = ", mq.Prefetch)
	// }

	//check if need to declare topic
	// if mq.DeclareAll || mq.Queue == "" {
	// 	err = mq.DeclareDestination(ch, true)
	// 	if err != nil {
	// 		return nil, ch, err
	// 	}
	// 	logrus.Info("declare done.")
	// }
	logrus.Info("start consumer")
	//start consumer

	data, err := ch.Consume(mq.Queue, consumerTag, mq.AutoAck, mq.Exclusive, false, false, nil)
	return data, ch, err
}

func (mq *Destination) getLogger() *logrus.Entry {
	return logrus.WithFields(logrus.Fields{
		"topic": mq.Topic,
		"queue": mq.Queue,
	})
}

//Produce publish message
func (mq *Destination) Produce(channel *rabbitmq.Channel, message amqp.Publishing) error {
	logger := mq.getLogger()
	// if mq.DeclareAll {
	// 	mq.DeclareDestination(channel, false)
	// 	logger.Info("declare done.")
	// }
	message.DeliveryMode = amqp.Persistent
	err := channel.Publish(mq.Topic, mq.Queue, false, false, message)
	if err != nil {
		logger.Error("publish message failed, ", err)
		return err
	}

	logger.Info("publish message done.")

	return nil
}

func (mq *Destination) generateCorrID() string {
	l := 32
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte('a' + rand.Intn('z'-'a'))
	}
	return string(bytes)
}

//RPC RPC over rabbitmq message. timeout setting should be ctx
func (mq *Destination) RPC(ctx context.Context, conn *rabbitmq.Connection, message amqp.Publishing) (*amqp.Delivery, error) {
	log := mq.getLogger()

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	defer ch.Close()

	replyQueue, err := ch.QueueDeclare("", false, true, false, false, nil)

	if err != nil {
		return nil, err
	}

	log.Debug("make template queue ready, queue = ", replyQueue.Name)

	msgs, err := ch.Consume(replyQueue.Name, fmt.Sprintf("%s-rpc", mq.Queue), true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	corrID := mq.generateCorrID()

	log.Debug("sending message")

	message.ReplyTo = replyQueue.Name
	message.CorrelationId = corrID

	err = ch.Publish(mq.Topic,
		mq.Queue,
		false,
		false,
		message,
	)
	log.Info("rpc send out done")

	select {
	case <-ctx.Done():
		log.Error("RPC time out or canceled. err ", ctx.Err())
		return nil, ctx.Err()
	case replied := <-msgs:
		log.Info("get replied")
		return &replied, nil
	}
}
