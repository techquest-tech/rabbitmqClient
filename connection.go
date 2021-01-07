package rabbitmq

import (
	"fmt"
	"sync"

	"github.com/creasty/defaults"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/techquest-tech/go-amqp-reconnect/rabbitmq"
)

var sharedmu sync.RWMutex

//SharedConnection should shared connection? default YES
var SharedConnection = true

// Settings Settings, should include url & options
type Settings struct {
	Host     string `default:"localhost"`
	Port     uint   `default:"5672"`
	User     string `default:"guest"`
	Password string `default:"guest"`
	Vhost    string `default:"/"`
	Prop     amqp.Table

	cnn *rabbitmq.Connection
}

// ConnURL return connection URL for Dial
func (r *Settings) ConnURL() string {
	defaults.Set(r)
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", r.User, r.Password, r.Host, r.Port, r.Vhost)
}

// String for log connection URL.
func (r *Settings) String() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", r.User, "****", r.Host, r.Port, r.Vhost)
}

// Connect make connection to Rabbitmq
func (r *Settings) Connect() (*rabbitmq.Connection, error) {
	sharedmu.Lock()
	defer sharedmu.Unlock()

	if SharedConnection && r.cnn != nil {
		logrus.Debug("using cached connection")
		return r.cnn, nil
	}

	rabbitmqURL := r.ConnURL()

	logrus.Infof("Dial up to %s", r.String())

	// prop := amqp.Table{
	// 	"connection_name": r.Name,
	// }
	conn, err := rabbitmq.DialConfig(rabbitmqURL, amqp.Config{
		Properties: r.Prop,
	})
	if SharedConnection && err == nil {
		logrus.Debug("shared connection is enabled.")
		r.cnn = conn
	}
	return conn, err
}
