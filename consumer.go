package rabbitmq

import (
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// OnReceive interface for Receiver
type OnReceive interface {
	OnReceiveMessage(msg amqp.Delivery) (string, *amqp.Publishing, error)
}

// FailOnError failed if any error
func FailOnError(err error, msg string) {
	if err != nil {
		logrus.WithField("err", err).Fatal(msg)
	}
}

// StartConsumer start process.
func StartConsumer(msg *MqDestination, receiver OnReceive, connSetting *Settings) {

	conn, err := connSetting.Connect()

	FailOnError(err, "connect to rabbitmq failed.")

	if msg.Queue == "" {
		msg.DeclareDestination(conn, true)
	}

	logrus.Infof("Start consumer for %s", msg.Queue)

	msgs, ch, err := msg.Consume(conn)
	FailOnError(err, "consumer failed.")
	go func() {
		for d := range msgs {
			key, repo, err := receiver.OnReceiveMessage(d)
			if !msg.AutoAck {
				if err == nil {
					d.Ack(false)
				} else {
					logrus.Warn("receiver failed, nack message ", msg.Queue)
					d.Nack(false, false)
				}
			}

			if key != "" && repo != nil {
				ch.Publish("", key, false, false, *repo)
				logrus.Infof("Receiver replied/forward message to %s", key)
			}
		}
	}()
	logrus.Infof(" [*] Q(%s) is waiting for message", msg.Queue)
}

// WrapRepo wrap up for repo to rabbitmq
func WrapRepo(msg amqp.Delivery, body []byte, headers amqp.Table) *amqp.Publishing {
	if headers == nil {
		headers = msg.Headers
	}

	resp := &amqp.Publishing{
		Headers:         headers,
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		MessageId:       msg.MessageId,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserId:          msg.UserId,
		AppId:           msg.AppId,
		Body:            body,
	}
	return resp
}
