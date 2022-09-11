package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	Writer *kafka.Writer
	Dialer *kafka.Dialer
}

func NewProducer() *Producer {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Dialer:  dialer,
	})

	return &Producer{
		Writer: writer,
	}
}

func Produce(key []byte, value []byte, topic string, producer *Producer) {
	err := producer.Writer.WriteMessages(context.TODO(), kafka.Message{
		Topic:  topic,
		Offset: 0,
		Key:    key,
		Value:  value,
	})

	if err != nil {
		fmt.Printf("delivery failed %s \n", err.Error())
	} else {
		fmt.Printf("message delivered topic: %s | key: %s\n", topic, string(key))
	}
}
