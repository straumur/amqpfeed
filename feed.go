package amqpfeed

import (
	"bytes"
	"encoding/json"
	"github.com/straumur/straumur"
	"github.com/streadway/amqp"
)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
	events  chan straumur.Event
}

func NewConsumer(amqpURI, exchange, exchangeType, queueName, key, ctag string, durable, auto_delete bool) (*Consumer, error) {

	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
		events:  make(chan straumur.Event),
	}

	var err error

	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, err
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, err
	}

	if err = c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		durable,      // durable
		auto_delete,  // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, err
	}

	queue, err := c.channel.QueueDeclare(
		queueName,   // name of the queue
		durable,     // durable
		auto_delete, // delete when usused
		false,       // exclusive
		false,       // noWait
		nil,         // arguments
	)
	if err != nil {
		return nil, err
	}

	if err = c.channel.QueueBind(
		queue.Name, // name of the queue
		key,        // bindingKey
		exchange,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return nil, err
	}

	deliveries, err := c.channel.Consume(
		queue.Name, // name
		c.tag,      // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)

	if err != nil {
		return nil, err
	}

	go c.handle(deliveries, c.done)

	return c, nil
}

func (c *Consumer) Close() error {

	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return err
	}

	if err := c.conn.Close(); err != nil {
		return err
	}

	return <-c.done
}

func (c *Consumer) Updates() <-chan straumur.Event {
	return c.events
}

func (c *Consumer) handle(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		decoder := json.NewDecoder(bytes.NewReader(d.Body))
		var e straumur.Event
		err := decoder.Decode(&e)
		if err != nil {
			done <- err
		}
		c.events <- e
	}
	done <- nil
}
