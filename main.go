package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql" // import MySQL driver
	"github.com/sirupsen/logrus"
)

// Mailchimp structs
type Member struct {
	Email              string `json:"email_address"`
	Status             string `json:"status"`
	SubscriptionStatus string `json:"Subscription Status"`
}
type Response struct {
	Members    []Member `json:"members"`
	TotalItems int      `json:"total_items"`
}

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
	SKU               string      `json:"sku"`
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
	SKU       string  `json:"sku"`
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

var log = logrus.New()

func main() {
	// Set up logrus. For example, to log as JSON instead of the default ASCII formatter:
	// log.SetFormatter(&logrus.JSONFormatter{})
	log.SetLevel(logrus.DebugLevel)

	//Open DB Connection
	log.Info("Connecting to database")
	db := opendb()
	defer db.Close()

	// MailChimp(db)
	Cratejoy(db)
}

func MailChimp(db *sql.DB) {

	//Retrieve API credentials
	apiKey := os.Getenv("apiKey")
	listID := os.Getenv("listID")

	// Set up the API request to retrieve the first batch of members
	url := "https://us6.api.mailchimp.com/3.0/lists/" + listID + "/members?fields=members.email_address,members.status,total_items,merge_fields.Subscription+Status&count=100"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.WithError(err).Error("Failed to create new HTTP request")
		return
	}
	req.SetBasicAuth("username", apiKey)

	// Send the API request
	log.Info("Sending API request")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.WithError(err).Error("Failed to send API request")
		return
	}
	defer resp.Body.Close()

	// Read the API response
	log.Info("Reading response")
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WithError(err).Error("Failed to read response body")
		return
	}
	log.Debug("Raw JSON response: ", string(body))

	// Parse the response JSON to CSV
	// log.Info("Creating CSV file")
	// file, err := os.Create("mailchimp-audience.csv")
	// if err != nil {
	// 	log.WithError(err).Error("Failed to create CSV file")
	// 	return
	// }
	// defer file.Close()

	log.Info("Parsing JSON response")

	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		log.WithError(err).Error("Failed to unmarshal JSON response")
		return
	}

	log.Infof("Retrieved %d members out of %d", len(response.Members), response.TotalItems)

	// Insert members into the database
	err = insertMembers(db, response)
	if err != nil {
		log.WithError(err).Error("Failed to insert members into the database")
		return
	}

	// Retrieve additional members with count and offset
	totalCount := response.TotalItems
	offset := 100

	for offset < totalCount {
		// Set up the API request to retrieve the next batch of members
		url = "https://us6.api.mailchimp.com/3.0/lists/" + listID + "/members?fields=members.email_address,members.status,merge_fields.Subscription+Status&count=100&offset=" + strconv.Itoa(offset)
		req, err = http.NewRequest("GET", url, nil)
		if err != nil {
			log.WithError(err).Error("Failed to create new HTTP request for next batch of members")
			return
		}
		req.SetBasicAuth("username", apiKey)

		// Send the API request
		resp, err = client.Do(req)
		if err != nil {
			log.WithError(err).Error("Failed to send API request for next batch of members")
			return
		}
		defer resp.Body.Close()

		// Read the API response
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log.WithError(err).Error("Failed to read response body for next batch of members")
			return
		}

		err = json.Unmarshal(body, &response)
		if err != nil {
			log.WithError(err).Error("Failed to unmarshal JSON response for next batch of members")
			return
		}

		log.Infof("Retrieved %d members out of %d", offset, response.TotalItems)

		// Insert members into the database
		err = insertMembers(db, response)
		if err != nil {
			log.WithError(err).Error("Failed to insert members into the database")
			return
		}

		offset += len(response.Members)
	}
}

// insertMembers inserts members into the database
func insertMembers(db *sql.DB, response Response) error {
	var valuePlaceholders []string
	var memberData []interface{}

	for _, member := range response.Members {
		valuePlaceholders = append(valuePlaceholders, "(?, ?)")
		memberData = append(memberData, member.Email, member.Status) // Add more fields if needed
	}

	if len(valuePlaceholders) == 0 {
		// No members to insert
		return nil
	}

	stmtText := "REPLACE INTO mailchimp(email, status) VALUES " + strings.Join(valuePlaceholders, ",")
	stmt, err := db.Prepare(stmtText)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(memberData...)
	if err != nil {
		return err
	}

	return nil
}

func Cratejoy(db *sql.DB) {
	// Fetch data from Cratejoy
	username := os.Getenv("CRATEJOY_CLIENT")
	password := os.Getenv("CRATEJOY_API_KEY")
	err := fetchCratejoyData(username, password, db)
	if err != nil {
		log.WithError(err).Error("Failed to fetch data from Cratejoy")
		return
	}
}

// insertSubscriptions inserts subscriptions into the database
func insertSubscriptions(db *sql.DB, response CratejoyResponse) error {
	var valuePlaceholders []string
	var subscriptionData []interface{}

	for _, subscription := range response.Results {
		// Prepare the SQL statement for each subscription
		valuePlaceholders = append(valuePlaceholders, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

		// Append the data for each subscription
		subscriptionData = append(subscriptionData,
			subscription.Customer.Email,       // Customer Email
			subscription.Customer.FirstName,   // First Name
			subscription.Customer.LastName,    // Last Name
			subscription.Customer.Country,     // Country
			subscription.Billing.RebillDay,    // Billing RebillDay
			subscription.Billing.RebillMonths, // Billing RebillMonths
			subscription.Autorenew,            // Subscription Autorenew
			subscription.Status,               // Subscription Status
			subscription.StartDate,            // Subscription StartDate
			subscription.EndDate)              // Subscription EndDate
	}

	if len(valuePlaceholders) == 0 {
		// No subscriptions to insert
		return nil
	}

	stmtText := "REPLACE INTO cratejoy_subscriptions(customer_email, first_name, last_name, country, rebill_day, rebill_months, autorenew, status, start_date, end_date) VALUES " + strings.Join(valuePlaceholders, ",")
	stmt, err := db.Prepare(stmtText)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(subscriptionData...)
	if err != nil {
		return err
	}

	return nil
}

func fetchCratejoyData(username, password string, db *sql.DB) error {
	// Define the Cratejoy endpoint for fetching subscriptions
	baseURL := "https://api.cratejoy.com/v1/subscriptions/"
	url := baseURL + "?limit=100"

	log.Info("Fetching data from Cratejoy API")

	for {
		log.Debug("Cratejoy API URL: ", url)
		// Set up the HTTP request
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.WithError(err).Error("Failed to create new HTTP request")
			return err
		}

		// Encode username and password for basic authentication
		authStr := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		req.Header.Add("Authorization", "Basic "+authStr)
		log.Debug("Authorization header set for basic authentication")
		log.Debug("Authorization Header: ", req.Header.Get("Authorization"))

		// Send the API request
		client := &http.Client{}
		log.Info("Sending request to Cratejoy API")
		resp, err := client.Do(req)
		if err != nil {
			log.WithError(err).Error("Failed to send API request")
			return err
		}
		defer resp.Body.Close()

		// Check for non-200 status code
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body) // Ignore error here; we're already handling an error case
			log.WithFields(logrus.Fields{
				"status_code": resp.StatusCode,
				"response":    string(body),
			}).Error("Cratejoy API responded with an error")
			return fmt.Errorf("Cratejoy API error: %d - %s", resp.StatusCode, string(body))
		}

		log.Info("Received response from Cratejoy API")
		log.Debugf("Status Code: %d", resp.StatusCode)

		// Read the API response
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.WithError(err).Error("Failed to read response body")
			return err
		}

		log.Debug("Raw JSON response: ", string(body))

		// Parse the JSON response
		var response CratejoyResponse
		err = json.Unmarshal(body, &response)
		if err != nil {
			log.WithError(err).Error("Failed to unmarshal JSON response")
			return err
		}

		log.Info("Successfully fetched and parsed Cratejoy API data")
		log.Debugf("CratejoyResponse: %+v", response)

		// Insert the subscription data into the database
		err = insertSubscriptions(db, response)
		if err != nil {
			log.WithError(err).Error("Failed to insert subscriptions into the database")
			return nil
		}

		// Check if there is a next page. If not, break the loop
		if response.Next == "" {
			break
		}

		// Update the URL to the next page URL
		url = baseURL + response.Next
	}
	return nil
}

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
