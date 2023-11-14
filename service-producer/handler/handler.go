package handler

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sing3demons/service-producer/dto"
	"github.com/sing3demons/service-producer/models"
	"github.com/sing3demons/service-producer/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type handler struct {
	db *mongo.Client
}

func NewHandler(db *mongo.Client) *handler {
	return &handler{db: db}
}

func (h *handler) GetSaleRecordsOnline(c *gin.Context) {
	collection := h.db.Database("sales_records_Online").Collection("sales_records")
	s := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(s)

	filter := bson.D{}
	opts := &options.FindOptions{}
	opts.SetLimit(int64(limit))
	// cur, err := collection.Find(ctx, filter, opts)
	results, count, err := getMultiple[models.SalesRecord](collection, filter, opts)
	if err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}

	response := []dto.SalesRecord{}
	for _, result := range results {
		response = append(response, dto.SalesRecord{
			Href:          utils.GetHost() + "/" + strings.ToLower(result.SalesChannel) + "/" + result.ID,
			Region:        result.Region,
			Country:       result.Country,
			ItemType:      result.ItemType,
			SalesChannel:  result.SalesChannel,
			OrderPriority: result.OrderPriority,
			OrderDate:     result.OrderDate,
			OrderId:       result.OrderId,
			ShipDate:      result.ShipDate,
			UnitsSold:     result.UnitsSold,
			UnitPrice:     result.UnitPrice,
			UnitCost:      result.UnitCost,
			TotalRevenue:  result.TotalRevenue,
			TotalCost:     result.TotalCost,
			TotalProfit:   result.TotalProfit,
		})
	}

	c.JSON(200, gin.H{
		"total": count,
		"data":  response,
	})
}

func (h *handler) GetSaleRecordOnline(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(400, gin.H{
			"message": "id is empty",
		})
		return
	}

	collection := h.db.Database("sales_records_Online").Collection("sales_records")
	result, err := getOne[models.SalesRecord](collection, id)
	if err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}

	response := dto.SalesRecord{
		ID:            result.ID,
		Href:          utils.GetHost() + "/" + strings.ToLower(result.SalesChannel) + "/" + result.ID,
		Region:        result.Region,
		Country:       result.Country,
		ItemType:      result.ItemType,
		SalesChannel:  result.SalesChannel,
		OrderPriority: result.OrderPriority,
		OrderDate:     result.OrderDate,
		OrderId:       result.OrderId,
		ShipDate:      result.ShipDate,
		UnitsSold:     result.UnitsSold,
		UnitPrice:     result.UnitPrice,
		UnitCost:      result.UnitCost,
		TotalRevenue:  result.TotalRevenue,
		TotalCost:     result.TotalCost,
		TotalProfit:   result.TotalProfit,
	}

	c.JSON(200, response)
}

func (h *handler) GetSaleRecordsOffline(c *gin.Context) {
	collection := h.db.Database("sales_records_Offline").Collection("sales_records")
	s := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(s)

	filter := bson.D{}
	opts := &options.FindOptions{}
	opts.SetLimit(int64(limit))
	results, count, err := getMultiple[models.SalesRecord](collection, filter, opts)
	if err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}

	response := []dto.SalesRecord{}
	for _, result := range results {
		response = append(response, dto.SalesRecord{
			Href:          utils.GetHost() + "/" + strings.ToLower(result.SalesChannel) + "/" + result.ID,
			Region:        result.Region,
			Country:       result.Country,
			ItemType:      result.ItemType,
			SalesChannel:  result.SalesChannel,
			OrderPriority: result.OrderPriority,
			OrderDate:     result.OrderDate,
			OrderId:       result.OrderId,
			ShipDate:      result.ShipDate,
			UnitsSold:     result.UnitsSold,
			UnitPrice:     result.UnitPrice,
			UnitCost:      result.UnitCost,
			TotalRevenue:  result.TotalRevenue,
			TotalCost:     result.TotalCost,
			TotalProfit:   result.TotalProfit,
		})
	}

	c.JSON(200, gin.H{
		"total": count,
		"data":  response,
	})
}

func (h *handler) GetSaleRecordOffline(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(400, gin.H{
			"message": "id is empty",
		})
		return
	}

	collection := h.db.Database("sales_records_Offline").Collection("sales_records")

	result, err := getOne[models.SalesRecord](collection, id)
	if err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}

	response := dto.SalesRecord{
		ID:            result.ID,
		Href:          utils.GetHost() + "/" + strings.ToLower(result.SalesChannel) + "/" + result.ID,
		Region:        result.Region,
		Country:       result.Country,
		ItemType:      result.ItemType,
		SalesChannel:  result.SalesChannel,
		OrderPriority: result.OrderPriority,
		OrderDate:     result.OrderDate,
		OrderId:       result.OrderId,
		ShipDate:      result.ShipDate,
		UnitsSold:     result.UnitsSold,
		UnitPrice:     result.UnitPrice,
		UnitCost:      result.UnitCost,
		TotalRevenue:  result.TotalRevenue,
		TotalCost:     result.TotalCost,
		TotalProfit:   result.TotalProfit,
	}

	c.JSON(200, response)
}

func getOne[T any](collection *mongo.Collection, id string) (*T, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_id, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, fmt.Errorf("error getting data: %w", err)
	}

	var result T

	if err := collection.FindOne(ctx, bson.M{"_id": _id}).Decode(&result); err != nil {
		return nil, fmt.Errorf("error getting data: %w", err)
	}

	return &result, nil
}

func getMultiple[T any](collection *mongo.Collection, filter primitive.D, opts *options.FindOptions) ([]T, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cur, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("error getting data: %w", err)
	}
	var result []T
	for cur.Next(ctx) {
		var r T
		err := cur.Decode(&r)
		if err != nil {
			return nil, 0, fmt.Errorf("error getting data: %w", err)
		}
		result = append(result, r)
	}

	total := make(chan int64)
	go func() {
		count, err := collection.CountDocuments(ctx, filter)
		if err != nil {
			total <- 0
		}
		total <- count
	}()
	count := <-total
	return result, count, nil
}
