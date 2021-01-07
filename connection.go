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

// Settings Settings, should include url & options
type Settings struct {
	Host     string `default:"localhost"`
	Port     uint   `default:"5672"`
	User     string `default:"guest"`
	Password string `default:"guest"`
	Vhost    string `default:"/"`
	Shared   bool   `defalut:"true"`
	Prop     amqp.Table

	cnn *rabbitmq.Connection
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

	if r.Shared && r.cnn != nil {
		return r.cnn, nil
	}

	sharedmu.Lock()
	defer sharedmu.Unlock()

	defaults.Set(r)
	rabbitmqURL := r.ConnURL()

	logrus.Infof("Dial up to %s", r.String())

	// prop := amqp.Table{
	// 	"connection_name": r.Name,
	// }
	conn, err := rabbitmq.DialConfig(rabbitmqURL, amqp.Config{
		Properties: r.Prop,
	})
	if r.Shared && err != nil {
		r.cnn = conn
	}
	return conn, err
}

//Factory factory for connection pool
func (r Settings) Factory() (interface{}, error) {
	return r.Connect()
}

//Ping test connection status.
func (r Settings) Ping(v interface{}) error {
	cnn := v.(*rabbitmq.Connection)
	ch, err := cnn.Channel()
	if err != nil {
		ch.Close()
	}
	return err
}

//Close close connection
func (r Settings) Close(v interface{}) error {
	cnn := v.(*rabbitmq.Connection)
	return cnn.Close()
}
