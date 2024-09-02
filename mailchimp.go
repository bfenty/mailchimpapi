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

// run Mailchimp API
func MailChimp(db *sql.DB) {

	//Retrieve API credentials
	apiKey := os.Getenv("apiKey")
	listID := os.Getenv("listID")
	count := "1000"

	// Set up the API request to retrieve the first batch of members
	url := "https://us6.api.mailchimp.com/3.0/lists/" + listID + "/members?fields=members.email_address,members.status,total_items,merge_fields.Subscription+Status&count=" + count
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
	offset, _ := strconv.Atoi(count)

	for offset < totalCount {
		// Set up the API request to retrieve the next batch of members
		url = "https://us6.api.mailchimp.com/3.0/lists/" + listID + "/members?fields=members.email_address,members.status,merge_fields.Subscription+Status&count=" + count + "&offset=" + strconv.Itoa(offset)
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

// insert Mailchimp Members inserts members into the database
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
