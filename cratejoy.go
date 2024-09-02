package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

// run Cratejoy API
func Cratejoy(db *sql.DB) {
	// Fetch data from Cratejoy
	username := os.Getenv("CRATEJOY_CLIENT")
	password := os.Getenv("CRATEJOY_API_KEY")
	//Fetch Orders
	err := fetchCratejoyOrders(username, password, db)
	if err != nil {
		log.WithError(err).Error("Failed to fetch data from Cratejoy")
		return
	}
	//Fetch Subscriptions
	err = fetchCratejoyData(username, password, db)
	if err != nil {
		log.WithError(err).Error("Failed to fetch data from Cratejoy")
		return
	}
}

// Insert orders into the Database
func insertOrders(db *sql.DB, response CratejoyOrderResponse) error {
	if len(response.Results) == 0 {
		// No orders to insert
		return nil
	}

	// Start time for the function
	startTime := time.Now()
	log.WithFields(logrus.Fields{
		"start_time": startTime,
		"operation":  "insertOrders",
	}).Info("Inserting orders into cj_orders table")

	query := `
		INSERT INTO orders.cj_orders (
			id, card_refunded_amount, credit_applied, customer_id, financial_status, fulfillment_status, gift_card_discount,
			gift_message, gift_renewal_notif, gross_shipping, is_gift, order_gift_info, is_renewal, is_test, note, 
			placed_at, prorated_charge, refund_applied, refunded_amount, status, store_id, sub_total, total, total_app_fees, 
			total_label_cost, total_pending_fees, total_price, total_shipping, total_tax, transaction_fees, 
			transaction_fee_status, type, url) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		card_refunded_amount = VALUES(card_refunded_amount),
		credit_applied = VALUES(credit_applied),
		customer_id = VALUES(customer_id),
		financial_status = VALUES(financial_status),
		fulfillment_status = VALUES(fulfillment_status),
		gift_card_discount = VALUES(gift_card_discount),
		gift_message = VALUES(gift_message),
		gift_renewal_notif = VALUES(gift_renewal_notif),
		gross_shipping = VALUES(gross_shipping),
		is_gift = VALUES(is_gift),
		order_gift_info = VALUES(order_gift_info),
		is_renewal = VALUES(is_renewal),
		is_test = VALUES(is_test),
		note = VALUES(note),
		placed_at = VALUES(placed_at),
		prorated_charge = VALUES(prorated_charge),
		refund_applied = VALUES(refund_applied),
		refunded_amount = VALUES(refunded_amount),
		status = VALUES(status),
		store_id = VALUES(store_id),
		sub_total = VALUES(sub_total),
		total = VALUES(total),
		total_app_fees = VALUES(total_app_fees),
		total_label_cost = VALUES(total_label_cost),
		total_pending_fees = VALUES(total_pending_fees),
		total_price = VALUES(total_price),
		total_shipping = VALUES(total_shipping),
		total_tax = VALUES(total_tax),
		transaction_fees = VALUES(transaction_fees),
		transaction_fee_status = VALUES(transaction_fee_status),
		type = VALUES(type),
		url = VALUES(url)`

	recordCount := 0

	for _, order := range response.Results {
		orderGiftInfo, _ := json.Marshal(order.OrderGiftInfo)

		// Convert the placed_at datetime to MySQL compatible format
		placedAt, err := parseDate(order.PlacedAt)
		if err != nil {
			log.WithFields(logrus.Fields{
				"order_id": order.ID,
				"error":    err,
			}).Error("Failed to format placed_at date")
			return err
		}

		_, err = db.Exec(query,
			order.ID,
			order.CardRefundedAmount,
			order.CreditApplied,
			order.CustomerID,
			order.FinancialStatus,
			order.FulfillmentStatus,
			order.GiftCardDiscount,
			order.GiftMessage,
			order.GiftRenewalNotif,
			order.GrossShipping,
			order.IsGift,
			string(orderGiftInfo),
			order.IsRenewal,
			order.IsTest,
			order.Note,
			placedAt,
			order.ProratedCharge,
			order.RefundApplied,
			order.RefundedAmount,
			order.Status,
			order.StoreID,
			order.SubTotal,
			order.Total,
			order.TotalAppFees,
			order.TotalLabelCost,
			order.TotalPendingFees,
			order.TotalPrice,
			order.TotalShipping,
			order.TotalTax,
			order.TransactionFees,
			order.TransactionFeeStatus,
			order.Type,
			order.URL,
		)
		if err != nil {
			log.WithFields(logrus.Fields{
				"order_id": order.ID,
				"error":    err,
			}).Error("Failed to insert or update order in cj_orders table")
			return err
		}
		recordCount++
	}

	// End time and duration
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	log.WithFields(logrus.Fields{
		"end_time":     endTime,
		"duration":     duration,
		"record_count": recordCount,
	}).Info("Finished inserting or updating orders in cj_orders table")

	return nil
}

// Helper Functions for Cratejoy
// Function to insert into cj_addresses
func insertAddresses(db *sql.DB, subscriptions []Subscription) (map[int]int, error) {
	// Start time for the function
	startTime := time.Now()
	log.WithFields(logrus.Fields{
		"start_time": startTime,
		"operation":  "insertAddresses",
	}).Info("Inserting addresses into cj_addresses table")

	addressMap := make(map[int]int) // Original ID to new ID
	query := `
		INSERT INTO cj_addresses (id, city, company, country, icon, phone_number, state, status, status_message, street, to_name, type, unit, zip_code) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		city = VALUES(city),
		company = VALUES(company),
		country = VALUES(country),
		icon = VALUES(icon),
		phone_number = VALUES(phone_number),
		state = VALUES(state),
		status = VALUES(status),
		status_message = VALUES(status_message),
		street = VALUES(street),
		to_name = VALUES(to_name),
		type = VALUES(type),
		unit = VALUES(unit),
		zip_code = VALUES(zip_code)`

	recordCount := 0

	for _, subscription := range subscriptions {
		address := subscription.Address
		_, err := db.Exec(query,
			address.ID,
			address.City,
			address.Company,
			address.Country,
			address.Icon,
			address.PhoneNumber,
			address.State,
			address.Status,
			address.StatusMessage,
			address.Street,
			address.To,
			address.Type,
			address.Unit,
			address.ZipCode,
		)
		if err != nil {
			log.WithFields(logrus.Fields{
				"address_id": address.ID,
				"error":      err,
			}).Error("Failed to insert or update address in cj_addresses table")
			return nil, err
		}
		addressMap[address.ID] = address.ID
		recordCount++
	}

	// End time and duration
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	log.WithFields(logrus.Fields{
		"end_time":     endTime,
		"duration":     duration,
		"record_count": recordCount,
	}).Info("Finished inserting or updating addresses in cj_addresses table")

	return addressMap, nil
}

// Function to insert into cj_billings
func insertBillings(db *sql.DB, subscriptions []Subscription) (map[int]int, error) {
	// Start time for the function
	startTime := time.Now()
	log.WithFields(logrus.Fields{
		"start_time": startTime,
		"operation":  "insertBillings",
	}).Info("Inserting billings into cj_billings table")

	billingMap := make(map[int]int)
	query := `
		INSERT INTO cj_billings (id, rebill_day, rebill_months, rebill_weeks, rebill_window, store_id, type) 
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		rebill_day = VALUES(rebill_day),
		rebill_months = VALUES(rebill_months),
		rebill_weeks = VALUES(rebill_weeks),
		rebill_window = VALUES(rebill_window),
		store_id = VALUES(store_id),
		type = VALUES(type)`

	recordCount := 0

	for _, subscription := range subscriptions {
		billing := subscription.Billing
		rebillWeeks, _ := json.Marshal(billing.RebillWeeks)

		_, err := db.Exec(query,
			billing.ID,
			billing.RebillDay,
			billing.RebillMonths,
			string(rebillWeeks),
			billing.RebillWindow,
			billing.StoreID,
			billing.Type,
		)
		if err != nil {
			log.WithFields(logrus.Fields{
				"billing_id": billing.ID,
				"error":      err,
			}).Error("Failed to insert or update billing in cj_billings table")
			return nil, err
		}
		billingMap[billing.ID] = billing.ID
		recordCount++
	}

	// End time and duration
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	log.WithFields(logrus.Fields{
		"end_time":     endTime,
		"duration":     duration,
		"record_count": recordCount,
	}).Info("Finished inserting or updating billings in cj_billings table")

	return billingMap, nil
}

// Function to insert into cj_customers
func insertCustomers(db *sql.DB, subscriptions []Subscription) (map[int]int, error) {
	// Start time for the function
	startTime := time.Now()
	log.WithFields(logrus.Fields{
		"start_time": startTime,
		"operation":  "insertCustomers",
	}).Info("Inserting customers into cj_customers table")

	customerMap := make(map[int]int)
	query := `
		INSERT INTO cj_customers (id, country, email, first_name, last_name, location, name, status, type) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		country = VALUES(country),
		email = VALUES(email),
		first_name = VALUES(first_name),
		last_name = VALUES(last_name),
		location = VALUES(location),
		name = VALUES(name),
		status = VALUES(status),
		type = VALUES(type)`

	recordCount := 0

	for _, subscription := range subscriptions {
		customer := subscription.Customer
		status, _ := json.Marshal(customer.Status)

		_, err := db.Exec(query,
			customer.ID,
			customer.Country,
			customer.Email,
			customer.FirstName,
			customer.LastName,
			customer.Location,
			customer.Name,
			string(status),
			customer.Type,
		)
		if err != nil {
			log.WithFields(logrus.Fields{
				"customer_id": customer.ID,
				"error":       err,
			}).Error("Failed to insert or update customer in cj_customers table")
			return nil, err
		}
		customerMap[customer.ID] = customer.ID
		recordCount++
	}

	// End time and duration
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	log.WithFields(logrus.Fields{
		"end_time":     endTime,
		"duration":     duration,
		"record_count": recordCount,
	}).Info("Finished inserting or updating customers in cj_customers table")

	return customerMap, nil
}

// Function to insert into cj_products
func insertProducts(db *sql.DB, subscriptions []Subscription) (map[int]int, error) {
	// Start time for the function
	startTime := time.Now()
	log.WithFields(logrus.Fields{
		"start_time": startTime,
		"operation":  "insertProducts",
	}).Info("Inserting products into cj_products table")

	productMap := make(map[int]int)
	query := `
		INSERT INTO cj_products (id, deleted, description, display_order, flat_ship_price, gift_shipping, giftable, listed, max_subs, meta, mp_visible, name, product_billing_id, product_type, reviewable, ship_option, ship_weight, single_purchasable, sku, slug, store_id, subscribe_flow, subscribe_flow_data, visible) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		deleted = VALUES(deleted),
		description = VALUES(description),
		display_order = VALUES(display_order),
		flat_ship_price = VALUES(flat_ship_price),
		gift_shipping = VALUES(gift_shipping),
		giftable = VALUES(giftable),
		listed = VALUES(listed),
		max_subs = VALUES(max_subs),
		meta = VALUES(meta),
		mp_visible = VALUES(mp_visible),
		name = VALUES(name),
		product_billing_id = VALUES(product_billing_id),
		product_type = VALUES(product_type),
		reviewable = VALUES(reviewable),
		ship_option = VALUES(ship_option),
		ship_weight = VALUES(ship_weight),
		single_purchasable = VALUES(single_purchasable),
		sku = VALUES(sku),
		slug = VALUES(slug),
		store_id = VALUES(store_id),
		subscribe_flow = VALUES(subscribe_flow),
		subscribe_flow_data = VALUES(subscribe_flow_data),
		visible = VALUES(visible)`

	recordCount := 0

	for _, subscription := range subscriptions {
		product := subscription.Product
		maxSubs, _ := json.Marshal(product.MaxSubs)
		meta, _ := json.Marshal(product.Meta)
		subscribeFlowData, _ := json.Marshal(product.SubscribeFlowData)

		_, err := db.Exec(query,
			product.ID,
			product.Deleted,
			product.Description,
			product.DisplayOrder,
			product.FlatShipPrice,
			product.GiftShipping,
			product.Giftable,
			product.Listed,
			string(maxSubs),
			string(meta),
			product.MpVisible,
			product.Name,
			product.ProductBillingID,
			product.ProductType,
			product.Reviewable,
			product.ShipOption,
			product.ShipWeight,
			product.SinglePurchasable,
			product.Sku,
			product.Slug,
			product.StoreID,
			product.SubscribeFlow,
			string(subscribeFlowData),
			product.Visible,
		)
		if err != nil {
			log.WithFields(logrus.Fields{
				"product_id": product.ID,
				"error":      err,
			}).Error("Failed to insert or update product in cj_products table")
			return nil, err
		}
		productMap[product.ID] = product.ID
		recordCount++
	}

	// End time and duration
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	log.WithFields(logrus.Fields{
		"end_time":     endTime,
		"duration":     duration,
		"record_count": recordCount,
	}).Info("Finished inserting or updating products in cj_products table")

	return productMap, nil
}

// Function to insert into cj_product_instances
func insertProductInstances(db *sql.DB, subscriptions []Subscription) (map[int]int, error) {
	// Start time for the function
	startTime := time.Now()
	log.WithFields(logrus.Fields{
		"start_time": startTime,
		"operation":  "insertProductInstances",
	}).Info("Inserting product instances into cj_product_instances table")

	productInstanceMap := make(map[int]int)
	query := `
		INSERT INTO cj_product_instances (id, name, price, product_id, sku) 
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		name = VALUES(name),
		price = VALUES(price),
		product_id = VALUES(product_id),
		sku = VALUES(sku)`

	recordCount := 0

	for _, subscription := range subscriptions {
		productInstance := subscription.ProductInstance

		_, err := db.Exec(query,
			productInstance.ID,
			productInstance.Name,
			productInstance.Price,
			productInstance.ProductID,
			productInstance.Sku,
		)
		if err != nil {
			log.WithFields(logrus.Fields{
				"product_instance_id": productInstance.ID,
				"error":               err,
			}).Error("Failed to insert or update product instance in cj_product_instances table")
			return nil, err
		}
		productInstanceMap[productInstance.ID] = productInstance.ID
		recordCount++
	}

	// End time and duration
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	log.WithFields(logrus.Fields{
		"end_time":     endTime,
		"duration":     duration,
		"record_count": recordCount,
	}).Info("Finished inserting or updating product instances in cj_product_instances table")

	return productInstanceMap, nil
}

func insertTerms(db *sql.DB, subscriptions []Subscription) (map[int]int, error) {
	termMap := make(map[int]int)
	query := `
		INSERT INTO cj_terms (id, description, enabled, name, num_cycles, type, images) 
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		description = VALUES(description),
		enabled = VALUES(enabled),
		name = VALUES(name),
		num_cycles = VALUES(num_cycles),
		type = VALUES(type),
		images = VALUES(images)`

	// Start time for the function
	startTime := time.Now()
	log.WithFields(logrus.Fields{
		"start_time": startTime,
		"operation":  "insertTerms",
	}).Info("Inserting terms into cj_terms table")

	recordCount := 0

	for _, subscription := range subscriptions {
		term := subscription.Term
		images, _ := json.Marshal(term.Images)

		_, err := db.Exec(query,
			term.ID,
			term.Description,
			term.Enabled,
			term.Name,
			term.NumCycles,
			term.Type,
			string(images),
		)
		if err != nil {
			log.WithFields(logrus.Fields{
				"term_id": term.ID,
				"error":   err,
			}).Error("Failed to insert or update term in cj_terms table")
			return nil, err
		}

		termMap[term.ID] = term.ID
		recordCount++
	}

	// End time and duration
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	log.WithFields(logrus.Fields{
		"end_time":     endTime,
		"duration":     duration,
		"record_count": recordCount,
	}).Info("Finished inserting or updating terms in cj_terms table")

	return termMap, nil
}

func insertSubscriptions(db *sql.DB, response CratejoyResponse) error {
	if len(response.Results) == 0 {
		// No subscriptions to insert
		return nil
	}

	log.WithFields(logrus.Fields{
		"records": len(response.Results),
	}).Debug("Beginning Database Insert")
	startTime := time.Now() // Start timing the operation

	// Insert into the dependent tables first
	addressMap, err := insertAddresses(db, response.Results)
	if err != nil {
		return err
	}

	billingMap, err := insertBillings(db, response.Results)
	if err != nil {
		return err
	}

	customerMap, err := insertCustomers(db, response.Results)
	if err != nil {
		return err
	}

	productMap, err := insertProducts(db, response.Results)
	if err != nil {
		return err
	}

	productInstanceMap, err := insertProductInstances(db, response.Results)
	if err != nil {
		return err
	}

	termMap, err := insertTerms(db, response.Results)
	if err != nil {
		return err
	}

	// Now insert into cj_subscriptions
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := `REPLACE INTO cj_subscriptions 
	          (id, address_id, billing_id, customer_id, product_id, product_instance_id, term_id, autorenew, billing_name, credit, end_date, is_test, note, skipped_date, source, start_date, status, store_id, type, url) 
	          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	for _, subscription := range response.Results {
		startDate, err := parseDate(subscription.StartDate)
		if err != nil {
			return err
		}
		endDate, err := parseDate(subscription.EndDate)
		if err != nil {
			return err
		}

		credit, _ := json.Marshal(subscription.Credit)
		skippedDate, _ := json.Marshal(subscription.SkippedDate)

		_, err = tx.Exec(query,
			subscription.ID,
			addressMap[subscription.Address.ID],
			billingMap[subscription.Billing.ID],
			customerMap[subscription.Customer.ID],
			productMap[subscription.Product.ID],
			productInstanceMap[subscription.ProductInstance.ID],
			termMap[subscription.Term.ID],
			subscription.Autorenew,
			subscription.BillingName,
			string(credit),
			endDate,
			subscription.IsTest,
			subscription.Note,
			string(skippedDate),
			subscription.Source,
			startDate,
			subscription.Status,
			subscription.StoreID,
			subscription.Type,
			subscription.URL,
		)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.WithFields(logrus.Fields{
		"duration": time.Since(startTime),
	}).Debug("Database Insert Successful")

	return nil
}

// fetch Cratejoy API data
func fetchCratejoyData(username, password string, db *sql.DB) error {
	// Define the Cratejoy endpoint for fetching subscriptions
	baseURL := "https://api.cratejoy.com/v1/subscriptions/"
	url := baseURL + "?limit=500"

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

		// log.Debug("Raw JSON response: ", string(body))

		// Parse the JSON response
		var response CratejoyResponse
		err = json.Unmarshal(body, &response)
		if err != nil {
			log.WithError(err).Error("Failed to unmarshal JSON response")
			return err
		}

		log.Info("Successfully fetched and parsed Cratejoy API data")
		// log.Debugf("CratejoyResponse: %+v", response)

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

// sendCratejoyRequest handles the HTTP request with retry logic
func sendCratejoyRequest(url, username, password string) (*http.Response, error) {
	maxRetries := 5
	var resp *http.Response
	// var err error

	for retry := 0; retry < maxRetries; retry++ {
		log.Debug("Cratejoy API URL: ", url)

		// Set up the HTTP request
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.WithError(err).Error("Failed to create new HTTP request")
			return nil, err
		}

		// Encode username and password for basic authentication
		authStr := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		req.Header.Add("Authorization", "Basic "+authStr)
		log.Debug("Authorization header set for basic authentication")

		// Send the API request
		client := &http.Client{
			Timeout: time.Second * 60, // 60-second timeout
		}
		log.Info("Sending request to Cratejoy API")
		resp, err = client.Do(req)
		if err != nil {
			log.WithError(err).Error("Failed to send API request")
			if retry < maxRetries-1 {
				log.Warnf("Retrying request, attempt %d/%d...", retry+1, maxRetries)
				continue
			}
			return nil, err
		}

		// Check for non-200 status code
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body) // Ignore error here; we're already handling an error case
			log.WithFields(logrus.Fields{
				"status_code": resp.StatusCode,
				"response":    string(body),
			}).Error("Cratejoy API responded with an error")
			if retry < maxRetries-1 {
				log.Warnf("Retrying request, attempt %d/%d...", retry+1, maxRetries)
				continue
			}
			return nil, fmt.Errorf("Cratejoy API error: %d - %s", resp.StatusCode, string(body))
		}

		// If everything is successful, return the response
		return resp, nil
	}

	return nil, fmt.Errorf("Failed to send request after %d retries", maxRetries)
}

// fetchCratejoyOrders fetches order data from the Cratejoy API and processes it
func fetchCratejoyOrders(username, password string, db *sql.DB) error {
	// Query the most recent placed_at date from the database
	var mostRecentDate time.Time
	query := "SELECT MAX(placed_at) FROM orders.cj_orders"
	err := db.QueryRow(query).Scan(&mostRecentDate)
	if err != nil {
		log.WithError(err).Error("Failed to query the most recent placed_at date")
		return err
	}

	// Subtract 5 days from the most recent date
	filterDate := mostRecentDate.AddDate(0, 0, -5)
	filterDateStr := filterDate.Format("2006-01-02T15:04:05Z") // Format to ISO 8601

	// Define the Cratejoy endpoint for fetching orders with the filter
	baseURL := "https://api.cratejoy.com/v1/orders/"
	url := fmt.Sprintf("%s?placed_at__gt=%s&limit=150", baseURL, filterDateStr)

	log.Info("Fetching order data from Cratejoy API")

	for {
		resp, err := sendCratejoyRequest(url, username, password)
		if err != nil {
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

		// Parse the JSON response
		var response CratejoyOrderResponse
		err = json.Unmarshal(body, &response)
		if err != nil {
			log.WithError(err).Error("Failed to unmarshal JSON response")
			return err
		}

		log.Info("Successfully fetched and parsed Cratejoy order data")

		// Insert the order data into the database
		err = insertOrders(db, response)
		if err != nil {
			log.WithError(err).Error("Failed to insert orders into the database")
			return err
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
