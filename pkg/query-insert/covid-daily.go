// Function to query Chicago Open Data API for COVID-19 daily cases and insert into PostgreSQL database

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
type CovidDailyTemp []struct {
	LabReportDate                        string `json:"lab_report_date"`
	CasesTotal                           string `json:"cases_total"`
	DeathsTotal                          string `json:"deaths_total"`
	CasesAge017                          string `json:"cases_age_0_17"`
	CasesAge1829                         string `json:"cases_age_18_29"`
	CasesAge3039                         string `json:"cases_age_30_39"`
	CasesAge4049                         string `json:"cases_age_40_49"`
	CasesAge5059                         string `json:"cases_age_50_59"`
	CasesAge6069                         string `json:"cases_age_60_69"`
	CasesAge7079                         string `json:"cases_age_70_79"`
	CasesAge80                           string `json:"cases_age_80_"`
	CasesAgeUnknown                      string `json:"cases_age_unknown"`
	CasesFemale                          string `json:"cases_female"`
	CasesMale                            string `json:"cases_male"`
	CasesUnknownGender                   string `json:"cases_unknown_gender"`
	CasesLatinx                          string `json:"cases_latinx"`
	CasesAsianNonLatinx                  string `json:"cases_asian_non_latinx"`
	CasesBlackNonLatinx                  string `json:"cases_black_non_latinx"`
	CasesWhiteNonLatinx                  string `json:"cases_white_non_latinx"`
	CasesOtherNonLatinx                  string `json:"cases_other_non_latinx"`
	CasesUnknownRaceEth                  string `json:"cases_unknown_race_eth"`
	Deaths017Yrs                         string `json:"deaths_0_17_yrs"`
	Deaths1829Yrs                        string `json:"deaths_18_29_yrs"`
	Deaths3039Yrs                        string `json:"deaths_30_39_yrs"`
	Deaths4049Yrs                        string `json:"deaths_40_49_yrs"`
	Deaths5059Yrs                        string `json:"deaths_50_59_yrs"`
	Deaths6069Yrs                        string `json:"deaths_60_69_yrs"`
	Deaths7079Yrs                        string `json:"deaths_70_79_yrs"`
	Deaths80Yrs                          string `json:"deaths_80_yrs"`
	DeathsUnknownAge                     string `json:"deaths_unknown_age"`
	DeathsFemale                         string `json:"deaths_female"`
	DeathsMale                           string `json:"deaths_male"`
	DeathsUnknownGender                  string `json:"deaths_unknown_gender"`
	DeathsLatinx                         string `json:"deaths_latinx"`
	DeathsAsianNonLatinx                 string `json:"deaths_asian_non_latinx"`
	DeathsBlackNonLatinx                 string `json:"deaths_black_non_latinx"`
	DeathsWhiteNonLatinx                 string `json:"deaths_white_non_latinx"`
	DeathsOtherNonLatinx                 string `json:"deaths_other_non_latinx"`
	DeathsUnknownRaceEth                 string `json:"deaths_unknown_race_eth"`
	HospitalizationsTotal                string `json:"hospitalizations_total"`
	HospitalizationsAge017               string `json:"hospitalizations_age_0_17"`
	HospitalizationsAge1829              string `json:"hospitalizations_age_18_29"`
	HospitalizationsAge3039              string `json:"hospitalizations_age_30_39"`
	HospitalizationsAge4049              string `json:"hospitalizations_age_40_49"`
	HospitalizationsAge5059              string `json:"hospitalizations_age_50_59"`
	HospitalizationsAge6069              string `json:"hospitalizations_age_60_69"`
	HospitalizationsAge7079              string `json:"hospitalizations_age_70_79"`
	HospitalizationsAge80                string `json:"hospitalizations_age_80_"`
	HospitalizationsAgeUnknown           string `json:"hospitalizations_age_unknown"`
	HospitalizationsFemale               string `json:"hospitalizations_female"`
	HospitalizationsMale                 string `json:"hospitalizations_male"`
	HospitalizationsUnknownGender        string `json:"hospitalizations_unknown_gender"`
	HospitalizationsLatinx               string `json:"hospitalizations_latinx"`
	HospitalizationsAsianNonLatinx       string `json:"hospitalizations_asian_non_latinx"`
	HospitalizationsBlackNonLatinx       string `json:"hospitalizations_black_non_latinx"`
	HospitalizationsWhiteNonLatinx       string `json:"hospitalizations_white_non_latinx"`
	HospitalizationsOtherRaceNonLatinx   string `json:"hospitalizations_other_race_non_latinx"`
	HospitalizationsUnknownRaceEthnicity string `json:"hospitalizations_unknown_race_ethnicity"`
}

func QueryInsertCovidDaily() {

	// Query from the API
	resp, err := http.Get("https://data.cityofchicago.org/resource/naz8-j4nc.json")
	CheckError(err)
	defer resp.Body.Close()

	// Convert response into bytes
	body, err := ioutil.ReadAll(resp.Body)
	CheckError(err)

	// Unmarshal response in bytes into struct
	var queryResult CovidDailyTemp
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
	_, err = db.Exec(`DELETE FROM "covid19daily"`)
	CheckError(err)

	// the following lines allow for a bulk insert
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("covid19daily", "lab_report_date", "cases_total", "deaths_total", "cases_age_0_17", "cases_age_18_29", "cases_age_30_39", "cases_age_40_49",
		"cases_age_50_59", "cases_age_60_69", "cases_age_70_79", "cases_age_80_", "cases_age_unknown", "cases_female", "cases_male", "cases_unknown_gender", "cases_latinx",
		"cases_asian_non_latinx", "cases_black_non_latinx", "cases_white_non_latinx", "cases_other_non_latinx", "cases_unknown_race_eth", "deaths_0_17_yrs", "deaths_18_29_yrs",
		"deaths_30_39_yrs", "deaths_40_49_yrs", "deaths_50_59_yrs", "deaths_60_69_yrs", "deaths_70_79_yrs", "deaths_80_yrs", "deaths_unknown_age", "deaths_female", "deaths_male",
		"deaths_unknown_gender", "deaths_latinx", "deaths_asian_non_latinx", "deaths_black_non_latinx", "deaths_white_non_latinx", "deaths_other_non_latinx",
		"deaths_unknown_race_eth", "hospitalizations_total", "hospitalizations_age_0_17", "hospitalizations_age_18_29", "hospitalizations_age_30_39",
		"hospitalizations_age_40_49", "hospitalizations_age_50_59", "hospitalizations_age_60_69", "hospitalizations_age_70_79", "hospitalizations_age_80_",
		"hospitalizations_age_unknown", "hospitalizations_female", "hospitalizations_male", "hospitalizations_unknown_gender", "hospitalizations_latinx",
		"hospitalizations_asian_non_latinx", "hospitalizations_black_non_latinx", "hospitalizations_white_non_latinx", "hospitalizations_other_race_non_latinx",
		"hospitalizations_unknown_race_ethnicity"))
	if err != nil {
		log.Fatal(err)
	}

	for _, result := range queryResult {
		_, err = stmt.Exec(result.LabReportDate, result.CasesTotal, result.DeathsTotal, result.CasesAge017, result.CasesAge1829, result.CasesAge3039, result.CasesAge4049,
			result.CasesAge5059, result.CasesAge6069, result.CasesAge7079, result.CasesAge80, result.CasesAgeUnknown, result.CasesFemale, result.CasesMale,
			result.CasesUnknownGender, result.CasesLatinx, result.CasesAsianNonLatinx, result.CasesBlackNonLatinx, result.CasesWhiteNonLatinx, result.CasesOtherNonLatinx,
			result.CasesUnknownRaceEth, result.Deaths017Yrs, result.Deaths1829Yrs, result.Deaths3039Yrs, result.Deaths4049Yrs, result.Deaths5059Yrs, result.Deaths6069Yrs,
			result.Deaths7079Yrs, result.Deaths80Yrs, result.DeathsUnknownAge, result.DeathsFemale, result.DeathsMale, result.DeathsUnknownGender, result.DeathsLatinx,
			result.DeathsAsianNonLatinx, result.DeathsBlackNonLatinx, result.DeathsWhiteNonLatinx, result.DeathsOtherNonLatinx, result.DeathsUnknownRaceEth,
			result.HospitalizationsTotal, result.HospitalizationsAge017, result.HospitalizationsAge1829, result.HospitalizationsAge3039, result.HospitalizationsAge4049,
			result.HospitalizationsAge5059, result.HospitalizationsAge6069, result.HospitalizationsAge7079, result.HospitalizationsAge80, result.HospitalizationsAgeUnknown,
			result.HospitalizationsFemale, result.HospitalizationsMale, result.HospitalizationsUnknownGender, result.HospitalizationsLatinx, result.HospitalizationsAsianNonLatinx,
			result.HospitalizationsBlackNonLatinx, result.HospitalizationsWhiteNonLatinx, result.HospitalizationsOtherRaceNonLatinx, result.HospitalizationsUnknownRaceEthnicity)
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
