package services

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
)

type EventProducer struct {
	producer sarama.SyncProducer
}

func NewEventProducer(producer sarama.SyncProducer) EventProducer {
	return EventProducer{producer}
}

type Model[T any] struct {
	Data []T
}

func (e EventProducer) Produce(topic string, event any) (err error) {
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
