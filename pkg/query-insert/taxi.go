// Function to query Chicago Open Data API for taxi trips for 2021 and 2020 and insert into PostgreSQL database

package queryInsert

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"

	linq "github.com/ahmetb/go-linq"
	pq "github.com/lib/pq"
)

// Create struct to insert API request results
type TaxiTemp []struct {
	TripStartTimestamp       string `json:"trip_start_timestamp"`
	PickupCommunityArea      string `json:"pickup_community_area"`
	PickupCentroidLatitude   string `json:"pickup_centroid_latitude"`
	PickupCentroidLongitude  string `json:"pickup_centroid_longitude"`
	DropoffCommunityArea     string `json:"dropoff_community_area"`
	DropoffCentroidLatitude  string `json:"dropoff_centroid_latitude"`
	DropoffCentroidLongitude string `json:"dropoff_centroid_longitude"`
}

// Create structs for aggregation
type TaxiTransactionRecord struct {
	TripStartTimestamp       string `json:"trip_start_timestamp"`
	PickupCommunityArea      string `json:"pickup_community_area"`
	PickupCentroidLatitude   string `json:"pickup_centroid_latitude"`
	PickupCentroidLongitude  string `json:"pickup_centroid_longitude"`
	DropoffCommunityArea     string `json:"dropoff_community_area"`
	DropoffCentroidLatitude  string `json:"dropoff_centroid_latitude"`
	DropoffCentroidLongitude string `json:"dropoff_centroid_longitude"`
}

type TaxiGroupKey struct {
	TripStartTimestamp       string `json:"trip_start_timestamp"`
	PickupCommunityArea      string `json:"pickup_community_area"`
	PickupCentroidLatitude   string `json:"pickup_centroid_latitude"`
	PickupCentroidLongitude  string `json:"pickup_centroid_longitude"`
	DropoffCommunityArea     string `json:"dropoff_community_area"`
	DropoffCentroidLatitude  string `json:"dropoff_centroid_latitude"`
	DropoffCentroidLongitude string `json:"dropoff_centroid_longitude"`
}

type TaxiGroupedRecord struct {
	TripStartTimestamp       string `json:"trip_start_timestamp"`
	PickupCommunityArea      string `json:"pickup_community_area"`
	PickupCentroidLatitude   string `json:"pickup_centroid_latitude"`
	PickupCentroidLongitude  string `json:"pickup_centroid_longitude"`
	DropoffCommunityArea     string `json:"dropoff_community_area"`
	DropoffCentroidLatitude  string `json:"dropoff_centroid_latitude"`
	DropoffCentroidLongitude string `json:"dropoff_centroid_longitude"`
	Count                    int    `json:"count"`
}

func deleteCurrentTaxiData() {
	// Set PostgreSQL arguments
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)

	// delete data so the table will only hold the most recent updated data
	_, err = db.Exec(`DELETE FROM "taxi2021"`)
	CheckError(err)

	// close database
	db.Close()
}

func QueryInsertTaxi2021() {

	deleteCurrentTaxiData()
	// Query from the API
	resp, err := http.Get("https://data.cityofchicago.org/resource/9kgb-ykyt.json?$SELECT=trip_start_timestamp,pickup_community_area,pickup_centroid_latitude,pickup_centroid_longitude,dropoff_community_area,dropoff_centroid_latitude,dropoff_centroid_longitude&$limit=5000000&$offset=0&$$app_token=Y78083yryERkSvlcZm69t1C5E")
	CheckError(err)
	defer resp.Body.Close()

	// Convert response into bytes
	body, err := ioutil.ReadAll(resp.Body)
	CheckError(err)

	// remove uncessary precision and time of day for aggregation purposes
	tempBody := string(body)
	reg1 := regexp.MustCompile(`T.*000`)
	stringBody1 := reg1.ReplaceAllString(tempBody, "${1}")

	reg2 := regexp.MustCompile(`\d{4}"`)
	stringBody2 := reg2.ReplaceAllString(stringBody1, `"`)

	stringBody3 := strings.TrimSuffix(stringBody2, ",")

	// Unmarshal response in bytes into struct
	var queryResult TaxiTemp
	if err := json.Unmarshal([]byte(stringBody3), &queryResult); err != nil {
		fmt.Print(err.Error())
	}

	// aggregate for memory purposes
	var results []TaxiGroupedRecord
	linq.From(queryResult).
		GroupByT(
			func(r TaxiTransactionRecord) TaxiGroupKey {
				return TaxiGroupKey{r.TripStartTimestamp, r.PickupCommunityArea, r.PickupCentroidLatitude, r.PickupCentroidLongitude,
					r.DropoffCommunityArea, r.DropoffCentroidLatitude, r.DropoffCentroidLongitude}
			},
			func(r TaxiTransactionRecord) TaxiTransactionRecord { return r },
		).
		SelectT(func(g linq.Group) TaxiGroupedRecord {
			return TaxiGroupedRecord{
				TripStartTimestamp:       g.Key.(TaxiGroupKey).TripStartTimestamp,
				PickupCommunityArea:      g.Key.(TaxiGroupKey).PickupCommunityArea,
				PickupCentroidLatitude:   g.Key.(TaxiGroupKey).PickupCentroidLatitude,
				PickupCentroidLongitude:  g.Key.(TaxiGroupKey).PickupCentroidLongitude,
				DropoffCommunityArea:     g.Key.(TaxiGroupKey).DropoffCommunityArea,
				DropoffCentroidLatitude:  g.Key.(TaxiGroupKey).DropoffCentroidLatitude,
				DropoffCentroidLongitude: g.Key.(TaxiGroupKey).DropoffCentroidLongitude,
				Count:                    linq.From(g.Group).SelectT(func(r TaxiTransactionRecord) string { return r.TripStartTimestamp }).Count(),
			}
		}).
		OrderByT(func(g TaxiGroupedRecord) string { return g.TripStartTimestamp }).
		ThenByT(func(g TaxiGroupedRecord) string { return g.PickupCommunityArea }).
		ThenByT(func(g TaxiGroupedRecord) string { return g.PickupCentroidLatitude }).
		ThenByT(func(g TaxiGroupedRecord) string { return g.PickupCentroidLongitude }).
		ThenByT(func(g TaxiGroupedRecord) string { return g.DropoffCommunityArea }).
		ThenByT(func(g TaxiGroupedRecord) string { return g.DropoffCentroidLatitude }).
		ThenByT(func(g TaxiGroupedRecord) string { return g.DropoffCentroidLongitude }).
		ThenByT(func(g TaxiGroupedRecord) int { return g.Count }).
		ToSlice(&results)

	// Set PostgreSQL arguments
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)

	// close database once the other statements below execute
	defer db.Close()

	// the following lines allow for a bulk insert
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("taxi2021", "trip_start_timestamp", "pickup_community_area", "pickup_centroid_latitude",
		"pickup_centroid_longitude", "dropoff_community_area", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "count"))
	if err != nil {
		log.Fatal(err)
	}

	for _, result := range results {
		_, err = stmt.Exec(result.TripStartTimestamp, result.PickupCommunityArea, result.PickupCentroidLatitude,
			result.PickupCentroidLongitude, result.DropoffCommunityArea, result.DropoffCentroidLatitude, result.DropoffCentroidLongitude,
			result.Count)
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

func QueryInsertTaxi2020() {

	// Query from the API
	resp, err := http.Get("https://data.cityofchicago.org/resource/r2u4-wwk3.json?$SELECT=trip_start_timestamp,pickup_community_area,pickup_centroid_latitude,pickup_centroid_longitude,dropoff_community_area,dropoff_centroid_latitude,dropoff_centroid_longitude&$limit=5000000&$offset=0&$$app_token=Y78083yryERkSvlcZm69t1C5E")
	CheckError(err)
	defer resp.Body.Close()

	// Convert response into bytes
	body, err := ioutil.ReadAll(resp.Body)
	CheckError(err)

	// remove uncessary precision and time of day for aggregation purposes
	tempBody := string(body)
	reg1 := regexp.MustCompile(`T.*000`)
	stringBody1 := reg1.ReplaceAllString(tempBody, "${1}")

	reg2 := regexp.MustCompile(`\d{4}"`)
	stringBody2 := reg2.ReplaceAllString(stringBody1, `"`)

	stringBody3 := strings.TrimSuffix(stringBody2, ",")

	// Unmarshal response in bytes into struct
	var queryResult TaxiTemp
	if err := json.Unmarshal([]byte(stringBody3), &queryResult); err != nil {
		fmt.Print(err.Error())
	}

	// aggregate for purposes of memory
	var results []TaxiGroupedRecord
	linq.From(queryResult).
		GroupByT(
			func(r TaxiTransactionRecord) TaxiGroupKey {
				return TaxiGroupKey{r.TripStartTimestamp, r.PickupCommunityArea, r.PickupCentroidLatitude, r.PickupCentroidLongitude,
					r.DropoffCommunityArea, r.DropoffCentroidLatitude, r.DropoffCentroidLongitude}
			},
			func(r TaxiTransactionRecord) TaxiTransactionRecord { return r },
		).
		SelectT(func(g linq.Group) TaxiGroupedRecord {
			return TaxiGroupedRecord{
				TripStartTimestamp:       g.Key.(TaxiGroupKey).TripStartTimestamp,
				PickupCommunityArea:      g.Key.(TaxiGroupKey).PickupCommunityArea,
				PickupCentroidLatitude:   g.Key.(TaxiGroupKey).PickupCentroidLatitude,
				PickupCentroidLongitude:  g.Key.(TaxiGroupKey).PickupCentroidLongitude,
				DropoffCommunityArea:     g.Key.(TaxiGroupKey).DropoffCommunityArea,
				DropoffCentroidLatitude:  g.Key.(TaxiGroupKey).DropoffCentroidLatitude,
				DropoffCentroidLongitude: g.Key.(TaxiGroupKey).DropoffCentroidLongitude,
				Count:                    linq.From(g.Group).SelectT(func(r TaxiTransactionRecord) string { return r.TripStartTimestamp }).Count(),
			}
		}).
		OrderByT(func(g TaxiGroupedRecord) string { return g.TripStartTimestamp }).
		ThenByT(func(g TaxiGroupedRecord) string { return g.PickupCommunityArea }).
		ThenByT(func(g TaxiGroupedRecord) string { return g.PickupCentroidLatitude }).
		ThenByT(func(g TaxiGroupedRecord) string { return g.PickupCentroidLongitude }).
		ThenByT(func(g TaxiGroupedRecord) string { return g.DropoffCommunityArea }).
		ThenByT(func(g TaxiGroupedRecord) string { return g.DropoffCentroidLatitude }).
		ThenByT(func(g TaxiGroupedRecord) string { return g.DropoffCentroidLongitude }).
		ThenByT(func(g TaxiGroupedRecord) int { return g.Count }).
		ToSlice(&results)

	// Set PostgreSQL arguments
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)

	// close database once the other statements below execute
	defer db.Close()

	// the following lines allow for a bulk insert
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("taxi2020", "trip_start_timestamp", "pickup_community_area", "pickup_centroid_latitude",
		"pickup_centroid_longitude", "dropoff_community_area", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "count"))
	if err != nil {
		log.Fatal(err)
	}

	for _, result := range results {
		_, err = stmt.Exec(result.TripStartTimestamp, result.PickupCommunityArea, result.PickupCentroidLatitude,
			result.PickupCentroidLongitude, result.DropoffCommunityArea, result.DropoffCentroidLatitude, result.DropoffCentroidLongitude,
			result.Count)
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}
