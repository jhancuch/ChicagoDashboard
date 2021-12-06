// Function to query Chicago Open Data API for COVID-19 weekly cases and insert into PostgreSQL database

package queryInsert

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	pq "github.com/lib/pq"
)

// Create struct to insert API request results
type CovidDWeeklyTemp []struct {
	ZipCode                         string `json:"zip_code"`
	WeekNumber                      string `json:"week_number"`
	WeekStart                       string `json:"week_start"`
	WeekEnd                         string `json:"week_end"`
	CasesWeekly                     string `json:"cases_weekly"`
	CasesCumulative                 string `json:"cases_cumulative"`
	CaseRateWeekly                  string `json:"case_rate_weekly"`
	CaseRateCumulative              string `json:"case_rate_cumulative"`
	TestsWeekly                     string `json:"tests_weekly"`
	TestsCumulative                 string `json:"tests_cumulative"`
	TestRateWeekly                  string `json:"test_rate_weekly"`
	TestRateCumulative              string `json:"test_rate_cumulative"`
	PercentTestedPositiveWeekly     string `json:"percent_tested_positive_weekly"`
	PercentTestedPositiveCumulative string `json:"percent_tested_positive_cumulative"`
	DeathsWeekly                    string `json:"deaths_weekly"`
	DeathsCumulative                string `json:"deaths_cumulative"`
	DeathRateWeekly                 string `json:"death_rate_weekly"`
	DeathRateCumulative             string `json:"death_rate_cumulative"`
	Population                      string `json:"population"`
	RowID                           string `json:"row_id"`
}

func QueryInsertCovidWeekly() {

	// Query from the API
	resp, err := http.Get("https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=10000")
	CheckError(err)
	defer resp.Body.Close()

	// Convert response into bytes
	body, err := ioutil.ReadAll(resp.Body)
	CheckError(err)

	// Unmarshal response in bytes into struct
	var queryResult CovidDWeeklyTemp
	if err := json.Unmarshal(body, &queryResult); err != nil {
		fmt.Println("Can not unmarshal JSON")
	}

	// Set PostgreSQL arguments
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)

	// close database once the other statements below execute
	defer db.Close()

	// delete previous data so the table will only contain the most recent data
	_, err = db.Exec(`DELETE FROM "covid19weekly"`)
	CheckError(err)

	// the following lines allow for a bulk insert
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("covid19weekly", "zip_code", "week_number", "week_start", "week_end", "cases_weekly",
		"cases_cumulative", "case_rate_weekly", "case_rate_cumulative", "tests_weekly", "tests_cumulative", "test_rate_weekly",
		"test_rate_cumulative", "percent_tested_positive_weekly", "percent_tested_positive_cumulative",
		"deaths_weekly", "deaths_cumulative", "death_rate_weekly", "death_rate_cumulative", "population", "row_id"))
	if err != nil {
		log.Fatal(err)
	}

	for _, result := range queryResult {
		_, err = stmt.Exec(result.ZipCode, result.WeekNumber, result.WeekStart, result.WeekEnd, result.CasesWeekly, result.CasesCumulative,
			result.CaseRateWeekly, result.CaseRateCumulative, result.TestsWeekly, result.TestsCumulative, result.TestRateWeekly, result.TestRateCumulative,
			result.PercentTestedPositiveWeekly, result.PercentTestedPositiveCumulative, result.DeathsWeekly, result.DeathsCumulative, result.DeathRateWeekly,
			result.DeathRateCumulative, result.Population, result.RowID)
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
