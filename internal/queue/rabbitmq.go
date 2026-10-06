package queue

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	PaymentExchange   = "gopay.events"
	PaymentQueue      = "gopay.payments.queue"
	WebhookQueue      = "gopay.webhooks.queue"
	DeadLetterExchange = "gopay.dlx"
	DeadLetterQueue   = "gopay.dead_letter.queue"
)

type EventMessage struct {
	Type      string      `json:"type"`
	PaymentID string      `json:"payment_id"`
	Amount    float64     `json:"amount"`
	Currency  string      `json:"currency,omitempty"`
	Status    string      `json:"status,omitempty"`
	Payload   interface{} `json:"payload,omitempty"`
}

type RabbitMQClient struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	url     string
}

func NewRabbitMQClient(url string) (*RabbitMQClient, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}

	client := &RabbitMQClient{
		conn:    conn,
		channel: ch,
		url:     url,
	}

	if err := client.setupTopology(); err != nil {
		client.Close()
		return nil, fmt.Errorf("setup rabbitmq topology: %w", err)
	}

	return client, nil
}

func (r *RabbitMQClient) setupTopology() error {
	// 1. Declare DLX Exchange & DLQ Queue
	if err := r.channel.ExchangeDeclare(
		DeadLetterExchange,
		"direct",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare dlx: %w", err)
	}

	if _, err := r.channel.QueueDeclare(
		DeadLetterQueue,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare dlq: %w", err)
	}

	if err := r.channel.QueueBind(
		DeadLetterQueue,
		"dead.letter",
		DeadLetterExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind dlq: %w", err)
	}

	// 2. Declare Main Topic Exchange
	if err := r.channel.ExchangeDeclare(
		PaymentExchange,
		"topic",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare payment exchange: %w", err)
	}

	// 3. Declare Webhook Queue with DLX
	args := amqp.Table{
		"x-dead-letter-exchange":    DeadLetterExchange,
		"x-dead-letter-routing-key": "dead.letter",
	}

	if _, err := r.channel.QueueDeclare(
		WebhookQueue,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		args,
	); err != nil {
		return fmt.Errorf("declare webhook queue: %w", err)
	}

	// Bind payment events to webhook queue
	routingKeys := []string{"payment.created", "payment.confirmed", "payment.refunded", "webhook.delivery"}
	for _, rk := range routingKeys {
		if err := r.channel.QueueBind(
			WebhookQueue,
			rk,
			PaymentExchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("bind webhook queue key %s: %w", rk, err)
		}
	}

	return nil
}

func (r *RabbitMQClient) Publish(ctx context.Context, routingKey string, body []byte) error {
	return r.channel.PublishWithContext(
		ctx,
		PaymentExchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

func (r *RabbitMQClient) ConsumeWebhooks() (<-chan amqp.Delivery, error) {
	return r.channel.Consume(
		WebhookQueue,
		"gopay-webhook-consumer",
		false, // autoAck: false, manual ack for reliability
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
}

func (r *RabbitMQClient) Close() {
	if r.channel != nil {
		if err := r.channel.Close(); err != nil {
			log.Printf("error closing channel: %v", err)
		}
	}
	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			log.Printf("error closing amqp conn: %v", err)
		}
	}
}

func (r *RabbitMQClient) Ping() bool {
	return !r.conn.IsClosed()
}
