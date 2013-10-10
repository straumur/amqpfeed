package amqpfeed

import (
	"encoding/json"
	"github.com/straumur/straumur"
	"github.com/streadway/amqp"
	"log"
	"testing"
)

const (
	uri          = "amqp://guest:guest@localhost:5672/"
	exchange     = "ha.test-exchange"
	exchangeType = "direct"
	queue        = "test-queue"
	bindingKey   = "test-key"
	consumerTag  = "simple-consumer"
	lifetime     = 0
)

func publish(amqpURI, exchange, exchangeType, routingKey string, event *straumur.Event, reliable, durable, auto_delete bool) error {

	b, err := json.Marshal(event)
	if err != nil {
		return err
	}

	connection, err := amqp.Dial(amqpURI)
	if err != nil {
		return err
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return err
	}

	if err := channel.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		durable,      // durable
		auto_delete,  // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return err
	}

	if reliable {
		if err := channel.Confirm(false); err != nil {
			return err
		}
		ack, nack := channel.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))
		defer confirmOne(ack, nack)
	}

	if err = channel.Publish(
		exchange,   // publish to an exchange
		routingKey, // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            b,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
		},
	); err != nil {
		return err
	}

	return nil
}

func confirmOne(ack, nack chan uint64) {
	select {
	case tag := <-ack:
		log.Printf("confirmed delivery with delivery tag: %d", tag)
	case tag := <-nack:
		log.Printf("failed delivery of delivery tag: %d", tag)
	}
}

func TestAMQPFeed(t *testing.T) {

	_, err := amqp.Dial(uri)
	if err != nil {
		t.Skip("Skipping amqp test, no rabbitmq available")
	}

	c, err := NewConsumer(uri, exchange, exchangeType, queue, bindingKey, consumerTag, true, true)
	if err != nil {
		t.Logf("%s", err)
	}

	t.Logf("%v", c)

	e := straumur.NewEvent(
		"foo.bar",
		nil,
		nil,
		"My event",
		3,
		"mysystem",
		[]string{"ns/foo", "ns/bar"},
		[]string{"someone"},
		nil,
		nil)

	err = publish(uri, exchange, exchangeType, bindingKey, e, true, true, true)

	if err != nil {
		t.Logf("%s", err)
	}

	i := <-c.Updates()

	t.Logf("%v", i)

	if c.Close() != nil {
		t.Logf("%s", err)
	}
}
