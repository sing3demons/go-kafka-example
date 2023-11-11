package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

func (obj *consumerHandler) EventHandler(topic string, eventBytes []byte) {
	switch topic {
	case "sales_records.Online":
		fmt.Println("sales_records.Online")

		collection := obj.client.Database("sales_records_Online").Collection("sales_records")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var salesRecord SalesRecord
		json.Unmarshal(eventBytes, &salesRecord)
		result, err := collection.InsertOne(ctx, salesRecord)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Inserted a single document: %s\n", result.InsertedID)
	case "sales_records.Offline":
		collection := obj.client.Database("sales_records_Offline").Collection("sales_records")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var salesRecord SalesRecord
		json.Unmarshal(eventBytes, &salesRecord)
		result, err := collection.InsertOne(ctx, salesRecord)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Inserted a single document: %s\n", result.InsertedID)
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
