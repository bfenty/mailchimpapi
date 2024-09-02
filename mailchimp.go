package main

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// Mailchimp structs
type Member struct {
	Email              string `json:"email_address"`
	Status             string `json:"status"`
	SubscriptionStatus string `json:"Subscription Status"`
	FullName           string `json:"full_name"`
	ContactID          string `json:"contact_id"` // Assuming 'unique_id' is your contact_id in the JSON response
}
type Response struct {
	Members    []Member `json:"members"`
	TotalItems int      `json:"total_items"`
}

// run Mailchimp API
func MailChimp(db *sql.DB) {
	apiKey := os.Getenv("apiKey")
	listIDs := strings.Split(os.Getenv("listID"), ",")
	count := "1000"

	for _, listID := range listIDs {
		processList(db, apiKey, listID, count)
	}
}

func processList(db *sql.DB, apiKey, listID, count string) {
	client := &http.Client{}
	offset := 0
	totalCount := 1 // Initialize to force entry into the loop

	for offset < totalCount {
		url := "https://us6.api.mailchimp.com/3.0/lists/" + listID + "/members?fields=members.email_address,members.status,members.full_name,merge_fields.Subscription+Status,members.contact_id,total_items&count=" + count + "&offset=" + strconv.Itoa(offset)
		log.Printf("Making API request to URL: %s", url) // Log the URL of the API request

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Printf("Failed to create HTTP request: %v", err)
			continue
		}
		req.SetBasicAuth("username", apiKey) // Assuming 'username' is a placeholder
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Failed to send HTTP request: %v", err)
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Failed to read response body: %v", err)
			resp.Body.Close()
			continue
		}
		resp.Body.Close() // It's good practice to close the body right after reading

		// log.Printf("API Response received: %s", string(body)) // Log raw response body

		var response Response
		if err = json.Unmarshal(body, &response); err != nil {
			log.Printf("Failed to unmarshal JSON: %v", err)
			continue
		}

		log.Printf("Processing %d members from list ID: %s", len(response.Members), listID) // Log number of members being processed

		if err = insertMembers(db, listID, response); err != nil {
			log.Printf("Failed to insert members into database: %v", err)
			continue
		}

		log.Printf("Inserted members successfully, continuing to next batch") // Log successful insertion

		offset += len(response.Members)
		totalCount = response.TotalItems
		log.Printf("Updated offset: %d, Total members: %d", offset, totalCount) // Log progress of member retrieval
	}

	log.Printf("Completed processing all members for list ID: %s", listID) // Log completion of processing for a list
}

func insertMembers(db *sql.DB, listID string, response Response) error {
	valueStrings := []string{}
	valueArgs := []interface{}{}
	for _, member := range response.Members {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs, listID, member.ContactID, member.Email, member.Status, member.FullName)
	}

	if len(valueStrings) == 0 {
		return nil
	}

	stmt := "REPLACE INTO mailchimp (list_id, contact_id, email, status, full_name) VALUES " + strings.Join(valueStrings, ",")
	if _, err := db.Exec(stmt, valueArgs...); err != nil {
		return err
	}

	return nil
}
