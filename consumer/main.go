package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/sing3demons/service-consumer/services"
)

type Topic struct {
	Online  string
	Offline string
}

func main() {
	kafkaBrokers := []string{"localhost:9092"}
	// kafkaTopic := "create.sales_records"
	kafkaTopic := Topic{
		Online:  "sales_records.Online",
		Offline: "sales_records.Offline",
	}

	consumerGroupID := "sales_records_consumer_group"

	db, err := ConnectMonoDB()
	if err != nil {
		panic(err)
	}

	log.Println("Starting a new Sarama consumer")

	version, err := sarama.ParseKafkaVersion("1.0.0")
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = version
	consumer, err := sarama.NewConsumerGroup(kafkaBrokers, consumerGroupID, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	handler := services.NewConsumerHandler(db)
	fmt.Println("Consumer up and running!...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err := consumer.Consume(ctx, []string{kafkaTopic.Offline, kafkaTopic.Online}, handler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}

		}
	}()

	// Handle graceful shutdown
	consumptionIsPaused := false
	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigterm:
		log.Println("Received termination signal. Initiating shutdown...")
		cancel()
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigusr1:
		toggleConsumptionFlow(consumer, &consumptionIsPaused)

	}

	// Wait for the consumer to finish processing
	wg.Wait()
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}
