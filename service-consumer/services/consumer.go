package services

import (
	"github.com/IBM/sarama"
	"go.mongodb.org/mongo-driver/mongo"
)

type consumerHandler struct {
	client *mongo.Client
}

func NewConsumerHandler(client *mongo.Client) sarama.ConsumerGroupHandler {
	return consumerHandler{client}
}

func (obj consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (obj consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (obj consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		obj.EventHandler(msg.Topic, msg.Value)
		session.MarkMessage(msg, "")
	}

	return nil
}
