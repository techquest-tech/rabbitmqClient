package rabbitmq

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/techquest-tech/go-amqp-reconnect/rabbitmq"
)

// Settings Settings, should include url & options
type Settings struct {
	Host     string
	Port     uint
	User     string
	Password string
	Vhost    string
	Prop     amqp.Table
}

// ConnURL return connection URL for Dial
func (r *Settings) ConnURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", r.User, r.Password, r.Host, r.Port, r.Vhost)
}

// String for log connection URL.
func (r *Settings) String() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", r.User, "****", r.Host, r.Port, r.Vhost)
}

// Connect make connection to Rabbitmq
func (r *Settings) Connect() (*rabbitmq.Connection, error) {
	rabbitmqURL := r.ConnURL()

	logrus.Infof("Dial up to %s", r.String())

	// prop := amqp.Table{
	// 	"connection_name": r.Name,
	// }
	conn, err := rabbitmq.DialConfig(rabbitmqURL, amqp.Config{
		Properties: r.Prop,
	})

	return conn, err
}
