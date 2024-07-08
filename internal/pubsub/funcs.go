package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonSlice, _ := json.Marshal(val)

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{ContentType: "application/json", Body: jsonSlice},
	)
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	durable bool,
) (*amqp.Channel, amqp.Queue, error) {
	subChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("Channel failed to make")
	}

	options := amqp.Table{"x-dead-letter-exchange": "peril_dlx"}
	queue, err := subChan.QueueDeclare(queueName, durable, !durable, !durable, false, options)
	if err != nil {
		fmt.Println(err)
		log.Fatalf("Failed to declare queue")
	}

	err = subChan.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		log.Fatalf("Failed to bind queue")
	}

	return subChan, queue, nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(val); err != nil {
		return err
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{ContentType: "application/gob", Body: buffer.Bytes()},
	)
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	durable bool,
	handler func(T) Acktype,
) error {
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		durable,
		handler,
		func(data []byte) (T, error) {
			var target T
			err := json.Unmarshal(data, &target)
			return target, err
		},
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	durable bool,
	handler func(T) Acktype,
) error {
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		durable,
		handler,
		func(data []byte) (T, error) {
			buffer := bytes.NewBuffer(data)
			decoder := gob.NewDecoder(buffer)
			var target T
			err := decoder.Decode(&target)
			return target, err
		},
	)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	durable bool,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, durable)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %v", err)
	}

	ch.Qos(10, 0, false)
	msgs, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	go func() {
		defer ch.Close()
		for msg := range msgs {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}
			switch handler(target) {
			case Ack:
				msg.Ack(false)
			case NackDiscard:
				msg.Nack(false, false)
			case NackRequeue:
				msg.Nack(false, true)
			}
		}
	}()
	return nil
}
