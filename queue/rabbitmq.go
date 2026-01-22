package queue

import (
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

type RabbitMQClient struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	queueName    string
	exchangeName string
	logger       *logrus.Logger
}

type UploadEvent struct {
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
	Path     string `json:"path,omitempty"`
}

func NewRabbitMQClient(url, queueName, exchangeName string, ttl int, logger *logrus.Logger) (*RabbitMQClient, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	if queueName != "" {
		args := amqp.Table{
			"x-message-ttl": int32(ttl),
		}
		_, err = ch.QueueDeclare(
			queueName, // name
			true,      // durable
			false,     // delete when unused
			false,     // exclusive
			false,     // no-wait
			args,      // arguments
		)
		if err != nil {
			ch.Close()
			conn.Close()
			return nil, fmt.Errorf("failed to declare a queue: %w", err)
		}
	}

	return &RabbitMQClient{
		conn:         conn,
		channel:      ch,
		queueName:    queueName,
		exchangeName: exchangeName,
		logger:       logger,
	}, nil
}

func (c *RabbitMQClient) Close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *RabbitMQClient) PublishEvent(event UploadEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = c.channel.Publish(
		c.exchangeName, // exchange
		c.queueName,    // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
		})

	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	c.logger.WithFields(logrus.Fields{
		"exchange": c.exchangeName,
		"queue":    c.queueName,
		"filename": event.Filename,
	}).Info("Event published to RabbitMQ")

	return nil
}
