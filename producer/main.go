package main

import (
	"encoding/json"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

func main() {
	products := []Product{
		{
			ID:            0,
			Name:          "product1",
			Price:         23,
			OriginalPrice: 34,
			CategoryID:    1,
		},
		{
			ID:            2,
			Name:          "product2",
			Price:         23,
			OriginalPrice: 34,
			CategoryID:    0,
		},
		{
			ID:            3,
			Name:          "product3",
			Price:         23,
			OriginalPrice: 34,
			CategoryID:    3,
		},
		{
			ID:            4,
			Name:          "product4",
			Price:         23,
			OriginalPrice: 34,
			CategoryID:    0,
		},
		{
			ID:            5,
			Name:          "product5",
			Price:         23,
			OriginalPrice: 34,
			CategoryID:    2,
		},
	}
	categories := []Category{
		{
			ID:   0,
			Name: "Category1",
		},
		{
			ID:   2,
			Name: "Category2",
		},
		{
			ID:   3,
			Name: "Category3",
		},
	}
	images := []Image{
		{
			ID:        0,
			URL:       "http://google.com/image1.jpg",
			ProductID: 0,
		},
		{
			ID:        1,
			URL:       "http://google.com/image2.jpg",
			ProductID: 0,
		},
		{
			ID:        2,
			URL:       "http://google.com/image3.jpg",
			ProductID: 1,
		},
		{
			ID:        3,
			URL:       "http://google.com/image4.jpg",
			ProductID: 2,
		},
		{
			ID:        4,
			URL:       "http://google.com/image5.jpg",
			ProductID: 3,
		},
		{
			ID:        5,
			URL:       "http://google.com/image6.jpg",
			ProductID: 3,
		},
	}

	productTopic := "producer-product-table-testing"
	categoryTopic := "producer-category-table-testing"
	imageTopic := "producer-image-table-testing"

	connection, err := kafka.Dial("tcp", net.JoinHostPort("localhost", "9092"))

	if err != nil {
		panic(err.Error())
	}

	productTopicConfig := kafka.TopicConfig{Topic: productTopic, NumPartitions: 1, ReplicationFactor: 1}
	categoryTopicConfig := kafka.TopicConfig{Topic: categoryTopic, NumPartitions: 1, ReplicationFactor: 1}
	imageTopicConfig := kafka.TopicConfig{Topic: imageTopic, NumPartitions: 1, ReplicationFactor: 1}

	err = connection.CreateTopics(productTopicConfig, categoryTopicConfig, imageTopicConfig)

	if err != nil {
		panic(err.Error())
	}

	producer := NewProducer()

	for _, product := range products {
		marsheledProduct, _ := json.Marshal(product)
		Produce([]byte(strconv.Itoa(product.ID)), marsheledProduct, productTopic, producer)
	}

	for _, category := range categories {
		marsheledProduct, _ := json.Marshal(category)
		Produce([]byte(strconv.Itoa(category.ID)), marsheledProduct, categoryTopic, producer)
	}

	for _, image := range images {
		marsheledProduct, _ := json.Marshal(image)
		Produce([]byte(strconv.Itoa(image.ID)), marsheledProduct, imageTopic, producer)
	}
}
