package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
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

	producer, err := GetNewProducer()

	if err != nil {
		fmt.Println("Failed to create producer: %s \n", err)
		os.Exit(1)
	}

	productTopic := "testing.producer-product-table-testing"
	categoryTopic := "testing.producer-category-table-testing"
	imageTopic := "testing.producer-image-table-testing"

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
