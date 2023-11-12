package services

import (
	"context"
	"encoding/json"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

func (obj *consumerHandler) EventHandler(topic string, msg *sarama.ConsumerMessage) {
	eventBytes := msg.Value
	var headers []map[string]any
	for _, h := range msg.Headers {
		headers = append(headers, map[string]any{string(h.Key): string(h.Value)})
	}
	switch topic {
	case "sales_records.Online":
		collection := obj.client.Database("sales_records_Online").Collection("sales_records")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var salesRecord SalesRecord
		json.Unmarshal(eventBytes, &salesRecord)
		result, err := collection.InsertOne(ctx, salesRecord)
		if err != nil {
			obj.logger.WithFields(logrus.Fields{
				"topic":        topic,
				"offset":       msg.Offset,
				"key":          string(msg.Key),
				"header":       headers,
				"ts":           msg.Timestamp,
				"part":         msg.Partition,
				"action":       "insert one document",
				"result":       "failed",
				"inserted_id":  result.InsertedID,
				"db_name":      collection.Database().Name(),
				"collection":   collection.Name(),
				"SalesChannel": salesRecord.SalesChannel,
				"error":        err.Error(),
			}).Error("failed")
			return
		}
		obj.logger.WithFields(logrus.Fields{
			"topic":        topic,
			"offset":       msg.Offset,
			"key":          string(msg.Key),
			"header":       headers,
			"ts":           msg.Timestamp,
			"part":         msg.Partition,
			"action":       "insert one document",
			"result":       "success",
			"inserted_id":  result.InsertedID,
			"db_name":      collection.Database().Name(),
			"collection":   collection.Name(),
			"SalesChannel": salesRecord.SalesChannel,
		}).Info("success")

	case "sales_records.Offline":
		collection := obj.client.Database("sales_records_Offline").Collection("sales_records")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var salesRecord SalesRecord
		json.Unmarshal(eventBytes, &salesRecord)
		result, err := collection.InsertOne(ctx, salesRecord)
		if err != nil {
			obj.logger.WithFields(logrus.Fields{
				"topic":        topic,
				"offset":       msg.Offset,
				"key":          string(msg.Key),
				"header":       headers,
				"ts":           msg.Timestamp,
				"part":         msg.Partition,
				"action":       "insert one document",
				"inserted_id":  result.InsertedID,
				"db_name":      collection.Database().Name(),
				"collection":   collection.Name(),
				"SalesChannel": salesRecord.SalesChannel,
				"error":        err.Error(),
			}).Error("failed")
			return
		}
		obj.logger.WithFields(logrus.Fields{
			"topic":        topic,
			"offset":       msg.Offset,
			"key":          string(msg.Key),
			"header":       headers,
			"ts":           msg.Timestamp,
			"part":         msg.Partition,
			"action":       "insert one document",
			"inserted_id":  result.InsertedID,
			"db_name":      collection.Database().Name(),
			"collection":   collection.Name(),
			"SalesChannel": salesRecord.SalesChannel,
		}).Info("success")
	}
}

type SalesRecord struct {
	Region        string `json:"region"`
	Country       string `json:"country"`
	ItemType      string `json:"item_type"`
	SalesChannel  string `json:"sales_channel"`
	OrderPriority string `json:"order_priority"`
	OrderDate     string `json:"order_date"`
	OrderId       string `json:"order_id"`
	ShipDate      string `json:"ship_date"`
	UnitsSold     string `json:"units_sold"`
	UnitPrice     string `json:"unit_price"`
	UnitCost      string `json:"unit_cost"`
	TotalRevenue  string `json:"total_revenue"`
	TotalCost     string `json:"total_cost"`
	TotalProfit   string `json:"total_profit"`
}
