package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/rcrowley/go-metrics"
	"github.com/sing3demons/service-producer/models"
)

// Sarama configuration options
var (
	producers           = 10
	verbose             = false
	recordsNumber int64 = 1
	recordsRate         = metrics.GetOrRegisterMeter("records.rate", nil)
)

func NewAsyncProducer(kafkaBrokers []string) *producerProvider {
	log.Println("Starting a new Sarama producer")
	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion("1.0.0")
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	producerProvider := newProducerProvider(kafkaBrokers, func() *sarama.Config {
		config := sarama.NewConfig()
		config.Version = version
		config.Producer.Idempotent = true
		config.Producer.Return.Errors = false
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
		config.Producer.Transaction.Retry.Backoff = 10
		config.Producer.Transaction.ID = "txn_producer"
		config.Net.MaxOpenRequests = 1
		return config
	})

	return producerProvider
}

// producerProvider is a thread-safe producer provider.
func (producerProvider *producerProvider) SendMessage(c *gin.Context, topic string, data []models.SalesRecord) {
	// go metrics.Log(metrics.DefaultRegistry, 5*time.Second, log.New(os.Stderr, "metrics: ", log.LstdFlags))
	ctx, cancel := context.WithCancel(context.Background())
	statusCode := strconv.Itoa(c.Writer.Status())
	header := []sarama.RecordHeader{
		{Key: []byte("METHOD"), Value: []byte(c.Request.Method)},
		{Key: []byte("STATUS"), Value: []byte(statusCode)},
		{Key: []byte("CLIENT_IP"), Value: []byte(c.ClientIP())},
		{Key: []byte("REQUEST_ID"), Value: []byte(c.Writer.Header().Get("X-Request-Id"))},
		{Key: []byte("REMOTE_IP"), Value: []byte(c.Request.RemoteAddr)},
		{Key: []byte("USER_ID"), Value: []byte(c.Request.URL.User.Username())},
		{Key: []byte("USER_AGENT"), Value: []byte(c.Request.UserAgent())},
		{Key: []byte("ERROR"), Value: []byte(c.Errors.ByType(gin.ErrorTypePrivate).String())},
		{Key: []byte("REQUEST"), Value: []byte(c.Request.PostForm.Encode())},
		{Key: []byte("BODY_SIZE"), Value: []byte(strconv.Itoa(c.Writer.Size()))},
		{Key: []byte("HOST"), Value: []byte(c.Request.Host)},
		{Key: []byte("PROTOCOL"), Value: []byte(c.Request.Proto)},
		{Key: []byte("PATH"), Value: []byte(c.Request.RequestURI)},
		{Key: []byte("TIME"), Value: []byte(time.Now().Location().String())},
		{Key: []byte("ResponseSize"), Value: []byte(strconv.Itoa(c.Writer.Size()))},
	}

	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for _, msg := range data {
				jsonByte, _ := json.Marshal(msg)
				select {
				case <-ctx.Done():
					return
				default:
					producerProvider.produceRecord(topic, header, jsonByte)
				}
			}
		}()
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm
	log.Println("terminating: via signal")

	cancel()
	wg.Wait()
	producerProvider.clear()
}

// pool of producers that ensure transactional-id is unique.
type producerProvider struct {
	transactionIdGenerator int32

	producersLock sync.Mutex
	producers     []sarama.AsyncProducer

	producerProvider func() sarama.AsyncProducer
}

func newProducerProvider(brokers []string, producerConfigurationProvider func() *sarama.Config) *producerProvider {
	provider := &producerProvider{}
	provider.producerProvider = func() sarama.AsyncProducer {
		config := producerConfigurationProvider()
		suffix := provider.transactionIdGenerator
		// Append transactionIdGenerator to current config.Producer.Transaction.ID to ensure transaction-id uniqueness.
		if config.Producer.Transaction.ID != "" {
			provider.transactionIdGenerator++
			config.Producer.Transaction.ID = config.Producer.Transaction.ID + "-" + fmt.Sprint(suffix)
		}
		producer, err := sarama.NewAsyncProducer(brokers, config)
		if err != nil {
			return nil
		}
		return producer
	}
	return provider
}

func (p *producerProvider) borrow() (producer sarama.AsyncProducer) {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	if len(p.producers) == 0 {
		for {
			producer = p.producerProvider()
			if producer != nil {
				return
			}
		}
	}

	index := len(p.producers) - 1
	producer = p.producers[index]
	p.producers = p.producers[:index]
	return
}

func (p *producerProvider) release(producer sarama.AsyncProducer) {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	// If released producer is erroneous close it and don't return it to the producer pool.
	if producer.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
		// Try to close it
		_ = producer.Close()
		return
	}
	p.producers = append(p.producers, producer)
}

func (p *producerProvider) clear() {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	for _, producer := range p.producers {
		producer.Close()
	}
	p.producers = p.producers[:0]
}

func (producerProvider *producerProvider) produceRecord(topic string, header []sarama.RecordHeader, msg []byte) {
	producer := producerProvider.borrow()
	defer producerProvider.release(producer)

	// Start kafka transaction
	err := producer.BeginTxn()
	if err != nil {
		log.Printf("unable to start txn %s\n", err)
		return
	}

	// Produce some records in transaction
	var i int64
	for i = 0; i < recordsNumber; i++ {
		producer.Input() <- &sarama.ProducerMessage{
			Topic:   topic,
			Key:     nil,
			Value:   sarama.ByteEncoder(msg),
			Headers: header,
		}
	}

	// commit transaction
	err = producer.CommitTxn()
	if err != nil {
		log.Printf("Producer: unable to commit txn %s\n", err)
		for {
			if producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
				// fatal error. need to recreate producer.
				log.Printf("Producer: producer is in a fatal state, need to recreate it")
				break
			}
			// If producer is in abortable state, try to abort current transaction.
			if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
				err = producer.AbortTxn()
				if err != nil {
					// If an error occured just retry it.
					log.Printf("Producer: unable to abort transaction: %+v", err)
					continue
				}
				break
			}
			// if not you can retry
			err = producer.CommitTxn()
			if err != nil {
				log.Printf("Producer: unable to commit txn %s\n", err)
				continue
			}
		}
		return
	}
	recordsRate.Mark(recordsNumber)
}
