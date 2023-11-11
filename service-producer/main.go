package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/sing3demons/service-producer/dto"
	"github.com/sing3demons/service-producer/handler"
	"github.com/sing3demons/service-producer/models"
	"github.com/sing3demons/service-producer/services"
	log "github.com/sirupsen/logrus"
)

func NewSyncProducer(kafkaBrokers []string) (sarama.SyncProducer, error) {
	producer, err := sarama.NewSyncProducer(kafkaBrokers, nil)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

type Topic struct {
	Online  string
	Offline string
}

func main() {
	logger := log.New()
	logger.SetFormatter(&log.JSONFormatter{})
	logger.SetOutput(os.Stdout)
	logger.SetLevel(log.InfoLevel)

	db, err := ConnectMonoDB()
	if err != nil {
		panic(err)
	}
	defer DisconnectMongo(db)
	broker := os.Getenv("KAFKA_BROKERS")
	if broker == "" {
		broker = "localhost:9092"
	}

	kafkaBrokers := []string{broker}
	kafkaTopic := Topic{
		Online:  "sales_records.Online",
		Offline: "sales_records.Offline",
	}

	producer, err := NewSyncProducer(kafkaBrokers)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	// go metrics.Log(metrics.DefaultRegistry, 5*time.Second, log.New(os.Stderr, "metrics: ", log.LstdFlags))
	eventProducer := services.NewEventProducer(producer, logger)

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	salesHandler := handler.NewHandler(db)

	r.GET("/online", salesHandler.GetSaleRecordsOnline)
	r.GET("/online/:id", salesHandler.GetSaleRecordOnline)
	r.GET("/offline", salesHandler.GetSaleRecordsOffline)
	r.GET("/offline/:id", salesHandler.GetSaleRecordOffline)

	r.POST("/", func(c *gin.Context) {
		var sales_records []dto.ReqSalesRecord

		if err := c.BindJSON(&sales_records); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		fmt.Println("sales_records: ", sales_records)

		offline := "Offline"
		offlineChannel := []models.SalesRecord{}
		online := "Online"
		onlineChannel := []models.SalesRecord{}

		for _, v := range sales_records {
			sales_record := models.SalesRecord{
				Region:        v.Region,
				Country:       v.Country,
				ItemType:      v.ItemType,
				SalesChannel:  v.SalesChannel,
				OrderPriority: v.OrderPriority,
				OrderDate:     v.OrderDate,
				OrderId:       v.OrderId,
				ShipDate:      v.ShipDate,
				UnitsSold:     v.UnitsSold,
				UnitPrice:     v.UnitPrice,
				UnitCost:      v.UnitCost,
				TotalRevenue:  v.TotalRevenue,
				TotalCost:     v.TotalCost,
				TotalProfit:   v.TotalProfit,
			}
			if sales_record.SalesChannel == offline {
				offlineChannel = append(offlineChannel, sales_record)
			} else if sales_record.SalesChannel == online {
				onlineChannel = append(onlineChannel, sales_record)
			}
		}

		if len(offlineChannel) > 0 {
			event := NewEventProducer(kafkaTopic.Offline, eventProducer)
			event.EventCreateSalesRecords(offlineChannel)
		}

		if len(onlineChannel) > 0 {
			event := NewEventProducer(kafkaTopic.Online, eventProducer)
			event.EventCreateSalesRecords(onlineChannel)
		}

		c.JSON(200, gin.H{
			"message": "success",
		})
	})

	r.POST("/sales_records", func(c *gin.Context) {
		event := services.NewAsyncProducer(kafkaBrokers)
		offline := "Offline"
		offlineChannel := []models.SalesRecord{}
		online := "Online"
		onlineChannel := []models.SalesRecord{}

		var sales_records []dto.ReqSalesRecord

		if err := c.Bind(&sales_records); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		for _, v := range sales_records {
			sales_record := models.SalesRecord{
				Region:        v.Region,
				Country:       v.Country,
				ItemType:      v.ItemType,
				SalesChannel:  v.SalesChannel,
				OrderPriority: v.OrderPriority,
				OrderDate:     v.OrderDate,
				OrderId:       v.OrderId,
				ShipDate:      v.ShipDate,
				UnitsSold:     v.UnitsSold,
				UnitPrice:     v.UnitPrice,
				UnitCost:      v.UnitCost,
				TotalRevenue:  v.TotalRevenue,
				TotalCost:     v.TotalCost,
				TotalProfit:   v.TotalProfit,
			}
			if sales_record.SalesChannel == offline {
				offlineChannel = append(offlineChannel, sales_record)
			} else if sales_record.SalesChannel == online {
				onlineChannel = append(onlineChannel, sales_record)
			}
		}

		if len(offlineChannel) > 0 {
			fmt.Println("offlineChannel: ", len(offlineChannel))
			go services.SendMessage(event, kafkaTopic.Offline, offlineChannel)
		}

		if len(onlineChannel) > 0 {
			fmt.Println("onlineChannel: ", len(onlineChannel))
			go services.SendMessage(event, kafkaTopic.Online, onlineChannel)
		}

		c.JSON(200, gin.H{
			"message": "success",
		})
	})

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	RunServer(":2566", "service-producer", r)
}

type eventProducer struct {
	kafkaTopic string
	producer   services.EventProducer
}

func NewEventProducer(kafkaTopic string, producer services.EventProducer) *eventProducer {
	return &eventProducer{
		kafkaTopic: kafkaTopic,
		producer:   producer,
	}
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
				ev.producer.Produce(ev.kafkaTopic, sales_record)
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

func RunServer(addr, serviceName string, router http.Handler) {
	srv := &http.Server{
		Addr:    addr,
		Handler: router,
	}

	go func() {
		host, err := os.Hostname()
		if err != nil {
			host = "unknown"
		}

		fmt.Printf("[%s] http listen: %s%s\n", serviceName, host, srv.Addr)

		if err := srv.ListenAndServe(); err != nil && errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("server listen err: %v\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("server forced to shutdown: ", err)
	}

	fmt.Println("server exited")
}
