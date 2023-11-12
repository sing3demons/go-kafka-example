package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/sing3demons/service-producer/models"
	logger "github.com/sirupsen/logrus"
)

type eventProducer struct {
	kafkaTopic string
	producer   sarama.SyncProducer
	logger     *logger.Logger
}

func NewEventProducer(kafkaTopic string, producer sarama.SyncProducer, logger *logger.Logger) eventProducer {
	return eventProducer{kafkaTopic, producer, logger}
}

type Model[T any] struct {
	Data []T
}

func (e *eventProducer) Produce(topic string, event any) (err error) {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	partition, offset, err := e.producer.SendMessage(&msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func (ev *eventProducer) EventCreateSalesRecords(sales_records []models.SalesRecord) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for _, sales_record := range sales_records {
			select {
			case <-ctx.Done():
				return
			default:
				ev.Produce(ev.kafkaTopic, sales_record)
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm
	log.Println("terminating: via signal")

	cancel()
	wg.Wait()
}
