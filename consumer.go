package rabbitmq

import (
	"fmt"

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
func StartConsumer(destination *MqDestination, receiver OnReceive, connSetting *Settings) {
	log := destination.getLogger()

	conn, err := connSetting.Connect()

	FailOnError(err, "connect to rabbitmq failed.")

	if destination.Queue == "" {
		destination.DeclareDestination(conn, true)
	}

	log.Info("start consumer")
	// logrus.Infof("Start consumer for %s", msg.Queue)

	consumerTag := ""
	if tag, ok := connSetting.Prop["connection_name"]; ok {
		consumerTag = tag.(string)
	}

	msgs, ch, err := destination.Consume(conn, consumerTag)

	FailOnError(err, "consumer failed.")

	go func() {
		for d := range msgs {
			go func(d amqp.Delivery) {
				key, repo, err := receiver.OnReceiveMessage(d)

				if key == "" && d.ReplyTo != "" {
					key = d.ReplyTo
					log.Info("found replyTo value ", key)
				}

				if !destination.AutoAck {
					if err == nil {
						d.Ack(false)
					} else if d.ReplyTo != "" {
						log.Info("auto ack if ReplyTo is not empty")
						d.Ack(false)
					} else {
						log.Warn("receiver failed, nack message ", destination.Queue)
						d.Nack(false, false)
					}
				}

				if key != "" {
					if repo != nil {
						ch.Publish("", key, false, false, *repo)
						log.Info("Receiver replied/forward message to ", key)
					} else {
						if err != nil {
							replybody := fmt.Sprintf("{\"error\": %v, \"type\":%t}", err, err)
							headers := amqp.Table{
								"error": true,
							}
							ch.Publish("", key, false, false, amqp.Publishing{
								Headers: headers,
								Body:    []byte(replybody),
							})
							log.Info("replied/forward error to ", key)
						} else {
							log.Warn("no message replied or forward. but should be")
						}
					}
				} else {
					log.Debug("no message need to be replied or forward.")
				}
			}(d)
		}
	}()
	log.Info("waiting for message")
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
