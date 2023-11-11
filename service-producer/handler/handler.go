package handler

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sing3demons/service-producer/dto"
	"github.com/sing3demons/service-producer/models"
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(s)

	filter := bson.D{}
	opts := &options.FindOptions{}
	opts.SetLimit(int64(limit))
	cur, err := collection.Find(ctx, filter, opts)
	if err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}

	response := []dto.SalesRecord{}
	for cur.Next(ctx) {
		var result models.SalesRecord
		err := cur.Decode(&result)
		if err != nil {
			c.JSON(500, gin.H{
				"message": err.Error(),
			})
			return
		}

		response = append(response, dto.SalesRecord{
			Href:          "http://localhost:2566/" + strings.ToLower(result.SalesChannel) + "/" + result.ID,
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

	count, err := collection.CountDocuments(ctx, filter)
	if err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_id, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}

	result := models.SalesRecord{}
	if err := collection.FindOne(ctx, bson.M{"_id": _id}).Decode(&result); err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}
	response := dto.SalesRecord{
		ID:            result.ID,
		Href:          "http://localhost:2566/" + strings.ToLower(result.SalesChannel) + "/" + result.ID,
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(s)

	filter := bson.D{}
	opts := &options.FindOptions{}
	opts.SetLimit(int64(limit))
	cur, err := collection.Find(ctx, filter, opts)
	if err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}

	response := []dto.SalesRecord{}
	for cur.Next(ctx) {
		var result models.SalesRecord
		err := cur.Decode(&result)
		if err != nil {
			c.JSON(500, gin.H{
				"message": err.Error(),
			})
			return
		}

		response = append(response, dto.SalesRecord{
			Href:          "http://localhost:2566/" + strings.ToLower(result.SalesChannel) + "/" + result.ID,
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

	count, err := collection.CountDocuments(ctx, filter)
	if err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_id, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}

	result := models.SalesRecord{}
	if err := collection.FindOne(ctx, bson.M{"_id": _id}).Decode(&result); err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}
	response := dto.SalesRecord{
		ID:            result.ID,
		Href:          "http://localhost:2566/" + strings.ToLower(result.SalesChannel) + "/" + result.ID,
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
