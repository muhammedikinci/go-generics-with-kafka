package main

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func main() {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	productConsumer := Consumer[Product]{
		dialer: dialer,
		topic:  "producer-product-table-testing",
	}
	productConsumer.CreateConnection()

	imageConsumer := Consumer[Image]{
		dialer: dialer,
		topic:  "producer-image-table-testing",
	}
	imageConsumer.CreateConnection()

	categoryConsumer := Consumer[Category]{
		dialer: dialer,
		topic:  "producer-category-table-testing",
	}
	categoryConsumer.CreateConnection()

	productConsumer.Read(Product{}, func(product Product, err error) {
		collectedProduct := CollectedProduct{
			ID:            product.ID,
			Name:          product.Name,
			Price:         product.Price,
			OriginalPrice: product.OriginalPrice,
			Images:        []string{},
		}

		imageConsumer.Read(Image{}, func(image Image, err error) {
			if product.ID == image.ProductID {
				collectedProduct.Images = append(collectedProduct.Images, image.URL)
			}
		})

		categoryConsumer.Read(Category{}, func(category Category, err error) {
			if product.CategoryID == category.ID {
				collectedProduct.Category = category.Name
			}
		})

		imageConsumer.reader.SetOffset(0)
		categoryConsumer.reader.SetOffset(0)

		fmt.Println(collectedProduct)
	})

	if err := productConsumer.reader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

	if err := imageConsumer.reader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
