package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

type Request struct {
	Type string                `form:"type" binding:"required"`
	File *multipart.FileHeader `form:"file" binding:"required"`
}

func main() {
	path := "uploads"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0755)
	}
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.POST("/upload-csv", func(c *gin.Context) {
		var req Request
		if err := c.ShouldBind(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if req.Type == "csv" {
			file, err := c.FormFile("file")
			if err != nil || file == nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid file"})
				return
			}

			filename := path + "/" + filepath.Base(file.Filename)
			if err := c.SaveUploadedFile(file, filename); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}

			sales_records, err := ReadCsvSalesRecord(filename)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}

			body, err := json.Marshal(sales_records)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			if err := HttpPost("http://localhost:2566/sales_records", body); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}

			if err := os.Remove(filename); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}

			c.JSON(http.StatusOK, gin.H{
				"message": "success",
				"body": map[string]any{
					"type": req.Type,
					"file": file.Filename,
				},
				"sales_records": len(sales_records),
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "ok"})
	})

	runServer(":8080", "upload-csv", r)
}

func HttpPost(url string, payload []byte) error {
	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpClient := &http.Client{
		Timeout: time.Second * 90,
	}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	fmt.Println(resp.Status)
	return nil
}

func ReadCsvSalesRecord(filename string) ([]SalesRecord, error) {
	// https://excelbianalytics.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}

	if len(records) == 0 {
		return nil, errors.New("no records found in the CSV file")
	}

	sales_records := make([]SalesRecord, 0)

	for index, record := range records {
		if index == 0 {
			continue
		}

		sales_record := SalesRecord{
			Region:        record[0],
			Country:       record[1],
			ItemType:      record[2],
			SalesChannel:  record[3],
			OrderPriority: record[4],
			OrderDate:     record[5],
			OrderId:       record[6],
			ShipDate:      record[7],
			UnitsSold:     record[8],
			UnitPrice:     record[9],
			UnitCost:      record[10],
			TotalRevenue:  record[11],
			TotalCost:     record[12],
			TotalProfit:   record[13],
		}

		sales_records = append(sales_records, sales_record)

	}

	return sales_records, nil
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

func runServer(addr, serviceName string, router http.Handler) {
	srv := &http.Server{
		Addr:    addr,
		Handler: router,
	}

	go func() {
		fmt.Printf("[%s] http listen: %s\n", serviceName, srv.Addr)

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
