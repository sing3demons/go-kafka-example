package services

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
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

func (e *eventProducer) Produce(c *gin.Context, topic string, event any) (err error) {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	statusCode := strconv.Itoa(c.Writer.Status())
	headers := []sarama.RecordHeader{
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
	msg := sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(value),
		Headers: headers,
	}
	partition, offset, err := e.producer.SendMessage(&msg)
	if err != nil {
		return err
	}

	e.logger.WithFields(logger.Fields{
		"topic":     topic,
		"partition": partition,
		"offset":    offset,
	}).Info("send message to kafka")

	return nil
}

func (ev *eventProducer) EventCreateSalesRecords(c *gin.Context, sales_records []models.SalesRecord) {
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
				ev.Produce(c, ev.kafkaTopic, sales_record)
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
