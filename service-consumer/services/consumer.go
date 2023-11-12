package services

import (
	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

type consumerHandler struct {
	client *mongo.Client
	logger *log.Logger
}

func NewConsumerHandler(client *mongo.Client, logger *log.Logger) sarama.ConsumerGroupHandler {
	return consumerHandler{client, logger}
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
