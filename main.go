package main

import (
	"database/sql"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql" // import MySQL driver
	"github.com/sirupsen/logrus"
)

// Cratejoy Structs
type CratejoyResponse struct {
	Count   int            `json:"count"`
	Next    string         `json:"next"`
	Prev    interface{}    `json:"prev"`
	Results []Subscription `json:"results"`
}

type Subscription struct {
	Address          Address         `json:"address"`
	Autorenew        bool            `json:"autorenew"`
	Billing          Billing         `json:"billing"`
	BillingName      string          `json:"billing_name"`
	Credit           interface{}     `json:"credit"`
	Customer         Customer        `json:"customer"`
	EndDate          string          `json:"end_date"`
	ID               int             `json:"id"`
	IsTest           bool            `json:"is_test"`
	Note             string          `json:"note"`
	Product          Product         `json:"product"`
	ProductBillingID int             `json:"product_billing_id"`
	ProductInstance  ProductInstance `json:"product_instance"`
	SkippedDate      interface{}     `json:"skipped_date"`
	Source           int             `json:"source"`
	StartDate        string          `json:"start_date"`
	Status           string          `json:"status"`
	StoreID          int             `json:"store_id"`
	Term             Term            `json:"term"`
	Type             string          `json:"type"`
	URL              string          `json:"url"`
}

type Address struct {
	City          string      `json:"city"`
	Company       string      `json:"company"`
	Country       string      `json:"country"`
	Icon          string      `json:"icon"`
	ID            int         `json:"id"`
	PhoneNumber   string      `json:"phone_number"`
	State         string      `json:"state"`
	Status        int         `json:"status"`
	StatusMessage interface{} `json:"status_message"`
	Street        string      `json:"street"`
	To            string      `json:"to"`
	Type          string      `json:"type"`
	Unit          string      `json:"unit"`
	ZipCode       string      `json:"zip_code"`
}

type Billing struct {
	ID           int         `json:"id"`
	RebillDay    int         `json:"rebill_day"`
	RebillMonths int         `json:"rebill_months"`
	RebillWeeks  interface{} `json:"rebill_weeks"`
	RebillWindow int         `json:"rebill_window"`
	StoreID      int         `json:"store_id"`
	Type         string      `json:"type"`
}

type Customer struct {
	Country   string      `json:"country"`
	Email     string      `json:"email"`
	FirstName string      `json:"first_name"`
	ID        int         `json:"id"`
	LastName  interface{} `json:"last_name"`
	Location  string      `json:"location"`
	Name      string      `json:"name"`
	Status    interface{} `json:"status"`
	Type      string      `json:"type"`
}

type Product struct {
	Deleted           bool        `json:"deleted"`
	Description       string      `json:"description"`
	DisplayOrder      int         `json:"display_order"`
	FlatShipPrice     float64     `json:"flat_ship_price"`
	GiftShipping      int         `json:"gift_shipping"`
	Giftable          bool        `json:"giftable"`
	ID                int         `json:"id"`
	Listed            bool        `json:"listed"`
	MaxSubs           interface{} `json:"max_subs"`
	Meta              interface{} `json:"meta"`
	MpVisible         bool        `json:"mp_visible"`
	Name              string      `json:"name"`
	ProductBillingID  int         `json:"product_billing_id"`
	ProductType       int         `json:"product_type"`
	Reviewable        bool        `json:"reviewable"`
	ShipOption        int         `json:"ship_option"`
	ShipWeight        float64     `json:"ship_weight"`
	SinglePurchasable bool        `json:"single_purchasable"`
	Sku               string      `json:"sku"`
	Slug              string      `json:"slug"`
	StoreID           int         `json:"store_id"`
	SubscribeFlow     bool        `json:"subscribe_flow"`
	SubscribeFlowData interface{} `json:"subscribe_flow_data"`
	Visible           bool        `json:"visible"`
}

type ProductInstance struct {
	ID        int     `json:"id"`
	Name      string  `json:"name"`
	Price     float64 `json:"price"`
	ProductID int     `json:"product_id"`
	Sku       string  `json:"sku"`
}

type Term struct {
	Description string      `json:"description"`
	Enabled     bool        `json:"enabled"`
	ID          int         `json:"id"`
	Images      []TermImage `json:"images"` // Assuming images is an array of image objects
	Name        string      `json:"name"`
	NumCycles   int         `json:"num_cycles"`
	Type        string      `json:"type"`
}

type TermImage struct {
	ID                     int    `json:"id"`
	SubscriptionTypeTermID int    `json:"subscription_type_term_id"`
	Type                   string `json:"type"`
	URL                    string `json:"url"`
}

// Structs for Orders
type OrderGiftInfo struct {
	GiftMessage        string `json:"gift_message"`
	GiftRecipientEmail string `json:"gift_recipient_email"`
	GiftRecipientName  string `json:"gift_recipient_name"`
}

type Order struct {
	ID                   int64         `json:"id"`
	CardRefundedAmount   int           `json:"card_refunded_amount"`
	CreditApplied        int           `json:"credit_applied"`
	CustomerID           int64         `json:"customer_id"`
	FinancialStatus      string        `json:"financial_status"`
	FulfillmentStatus    string        `json:"fulfillment_status"`
	GiftCardDiscount     int           `json:"gift_card_discount"`
	GiftMessage          string        `json:"gift_message"`
	GiftRenewalNotif     bool          `json:"gift_renewal_notif"`
	GrossShipping        int           `json:"gross_shipping"`
	IsGift               bool          `json:"is_gift"`
	OrderGiftInfo        OrderGiftInfo `json:"order_gift_info"`
	IsRenewal            bool          `json:"is_renewal"`
	IsTest               bool          `json:"is_test"`
	Note                 string        `json:"note"`
	PlacedAt             string        `json:"placed_at"`
	ProratedCharge       int           `json:"prorated_charge"`
	RefundApplied        int           `json:"refund_applied"`
	RefundedAmount       int           `json:"refunded_amount"`
	Status               string        `json:"status"`
	StoreID              int64         `json:"store_id"`
	SubTotal             int           `json:"sub_total"`
	Total                int           `json:"total"`
	TotalAppFees         int           `json:"total_app_fees"`
	TotalLabelCost       int           `json:"total_label_cost"`
	TotalPendingFees     int           `json:"total_pending_fees"`
	TotalPrice           int           `json:"total_price"`
	TotalShipping        int           `json:"total_shipping"`
	TotalTax             int           `json:"total_tax"`
	TransactionFees      int           `json:"transaction_fees"`
	TransactionFeeStatus int           `json:"transaction_fee_status"`
	Type                 string        `json:"type"`
	URL                  string        `json:"url"`
}

type CratejoyOrderResponse struct {
	Count   int         `json:"count"`
	Next    string      `json:"next"`
	Prev    interface{} `json:"prev"`
	Results []Order     `json:"results"`
}

// setup logging
var log = logrus.New()

// main function
func main() {
	log.SetLevel(logrus.DebugLevel)

	//Open DB Connection
	log.Info("Connecting to database")
	db := opendb()
	defer db.Close()

	MailChimp(db)
	// Cratejoy(db)
}

// return an open database
func opendb() (db *sql.DB) {
	var err error
	user := os.Getenv("USER")
	pass := os.Getenv("PASS")
	server := os.Getenv("SERVER")
	port := os.Getenv("PORT")
	// Get a database handle.
	log.Info("Connecting to DB...")
	log.Debug("user:", user)
	log.Debug("pass:", pass)
	log.Debug("server:", server)
	log.Debug("port:", port)
	log.Debug("Opening Database...")
	connectstring := os.Getenv("USER") + ":" + os.Getenv("PASS") + "@tcp(" + os.Getenv("SERVER") + ":" + os.Getenv("PORT") + ")/customers?parseTime=true"
	log.Debug("Connection: ", connectstring)
	db, err = sql.Open("mysql",
		connectstring)
	if err != nil {
		log.Error(err)
	}

	//Test Connection
	pingErr := db.Ping()
	if pingErr != nil {
		log.Error(err)
	}

	//Success!
	log.Info("Returning Open DB...")
	return db
}

func parseDate(dateStr string) (string, error) {
	// Parse the input date string
	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return "", err
	}

	// Return in the format MySQL expects
	return t.Format("2006-01-02 15:04:05"), nil
}
