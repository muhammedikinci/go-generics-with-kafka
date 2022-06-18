package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func GetNewProducer() (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "socket.gethostname()",
		"acks":              "all",
	})

	return producer, err
}

func Produce(key []byte, value []byte, topic string, producer *kafka.Producer) {
	deliveryChan := make(chan kafka.Event, 1)

	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   key,
		Value: value,
	}, deliveryChan)

	if err != nil {
		panic(err)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("delivery failed %v \n", m.TopicPartition.Error)
	} else {
		fmt.Printf("message delivered topic: %s | key: %s\n", topic, string(key))
	}

	close(deliveryChan)
}
