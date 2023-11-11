package dto

type SalesRecord struct {
	ID            string `json:"id,omitempty" bson:"_id,omitempty"`
	Href          string `json:"href"`
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

type ReqSalesRecord struct {
	Region        string `json:"region" form:"region"`
	Country       string `json:"country" form:"country"`
	ItemType      string `json:"item_type" form:"item_type"`
	SalesChannel  string `json:"sales_channel" form:"sales_channel"`
	OrderPriority string `json:"order_priority" form:"order_priority"`
	OrderDate     string `json:"order_date" form:"order_date"`
	OrderId       string `json:"order_id" form:"order_id"`
	ShipDate      string `json:"ship_date" form:"ship_date"`
	UnitsSold     string `json:"units_sold" form:"units_sold"`
	UnitPrice     string `json:"unit_price" form:"unit_price"`
	UnitCost      string `json:"unit_cost" form:"unit_cost"`
	TotalRevenue  string `json:"total_revenue" form:"total_revenue"`
	TotalCost     string `json:"total_cost" form:"total_cost"`
	TotalProfit   string `json:"total_profit" form:"total_profit"`
}
