// Function to query Chicago Open Data API for TNC trips for 2021 and 2020 and insert into PostgreSQL database

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
	"time"

	pq "github.com/lib/pq"
)

// Create struct to insert API request results
type TncTemp []struct {
	TripStartTimestamp       string `json:"trip_start_timestamp"`
	PickupCommunityArea      string `json:"pickup_community_area"`
	DropoffCommunityArea     string `json:"dropoff_community_area"`
	PickupCentroidLatitude   string `json:"pickup_centroid_latitude"`
	PickupCentroidLongitude  string `json:"pickup_centroid_longitude"`
	DropoffCentroidLatitude  string `json:"dropoff_centroid_latitude"`
	DropoffCentroidLongitude string `json:"dropoff_centroid_longitude"`
}

func QueryInsertTnc21() {

	deleteCurrentDataTnc()

	subQueryInsertTnc("unf9-2zu4", "5000000", "0", "tnc21")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("unf9-2zu4", "5000000", "5000000", "tnc21")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("unf9-2zu4", "5000000", "10000000", "tnc21")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("unf9-2zu4", "5000000", "15000000", "tnc21")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("unf9-2zu4", "5000000", "20000000", "tnc21")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("unf9-2zu4", "5000000", "25000000", "tnc21")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("unf9-2zu4", "5000000", "30000000", "tnc21")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("unf9-2zu4", "5000000", "35000000", "tnc21")
}

func QueryInsertTnc20() {

	subQueryInsertTnc("rmc8-eqv4.", "5000000", "0", "tnc20")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("rmc8-eqv4.", "5000000", "5000000", "tnc20")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("rmc8-eqv4.", "5000000", "10000000", "tnc20")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("rmc8-eqv4.", "5000000", "15000000", "tnc20")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("rmc8-eqv4.", "5000000", "20000000", "tnc20")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("rmc8-eqv4.", "5000000", "25000000", "tnc20")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("rmc8-eqv4.", "5000000", "30000000", "tnc20")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("rmc8-eqv4.", "5000000", "35000000", "tnc20")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("rmc8-eqv4.", "5000000", "40000000", "tnc20")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("rmc8-eqv4.", "5000000", "45000000", "tnc20")
	time.Sleep(5 * time.Minute)
	subQueryInsertTnc("rmc8-eqv4.", "5000000", "55000000", "tnc20")
}

func deleteCurrentDataTnc() {
	// Set PostgreSQL arguments
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)

	// delete data so the table will only hold the most recent updated data
	_, err = db.Exec(`DELETE FROM "tnc21"`)
	CheckError(err)

	// close database
	db.Close()
}

func subQueryInsertTnc(url string, end string, begin string, table string) {

	// Query from the API
	resp, err := http.Get("https://data.cityofchicago.org/resource/" + url + ".json?$SELECT=trip_start_timestamp,pickup_community_area,dropoff_community_area,pickup_centroid_latitude,pickup_centroid_longitude,dropoff_centroid_latitude,dropoff_centroid_longitude&$limit=" + end + "&$offset=" + begin + "&$$app_token=Y78083yryERkSvlcZm69t1C5E")
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
	var queryResult TncTemp
	if err := json.Unmarshal([]byte(stringBody3), &queryResult); err != nil {
		fmt.Print(err.Error())
	}

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

	stmt, err := txn.Prepare(pq.CopyIn(table, "trip_start_timestamp", "pickup_community_area", "dropoff_community_area",
		"pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude"))

	if err != nil {
		log.Fatal(err)
	}

	for _, result := range queryResult {
		_, err = stmt.Exec(result.TripStartTimestamp, result.PickupCommunityArea, result.DropoffCommunityArea, result.PickupCentroidLatitude,
			result.PickupCentroidLongitude, result.DropoffCentroidLatitude, result.DropoffCentroidLongitude)
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
