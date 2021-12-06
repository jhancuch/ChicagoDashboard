/*
This go module creates a dashboard on localhost:5050 and in the background collects the data from APIs,
stores the data, geocodes the data, and conducts analysis before pushing new charts to the dashboard.
*/

package main

import (
	"fmt"
	"time"

	analysis "github.com/jhancuch/chicago-dashboard/pkg/analysis"
	dashboard "github.com/jhancuch/chicago-dashboard/pkg/dashboard"
	queryInsert "github.com/jhancuch/chicago-dashboard/pkg/queryInsert"
)

// ensure the response by the user is either yes or no

func answerCheck(response string) {
	if response == "yes" || response == "no" {
		return
	} else {
		for {
			fmt.Println(`Invalid response. There are only two valid answers, "yes" or "no". Please try again.`)
			fmt.Scanln(&response)
			if response == "yes" || response == "no" {
				return
			}
		}
	}
}

func main() {

	dashboard.JustDashboard()

	// Depending on if the dashboard has been run before dictates if we need to grab Taxi and TNC data from 2020 or if it is already loaded in postgrsql
	fmt.Println(`Has this dashboard been run before, "yes" or "no"?`)
	var answer string
	fmt.Scanln(&answer)
	answerCheck(answer)

	if answer == "yes" {

		fmt.Println(`The dashboard has been served to a local port. Please navigate to "http://localhost:5000".`)
		fmt.Println("In the background, the program is updating the database with new data, running analysis, and will be pushed to the dashboard when the process is complete.")

		dashboard.DashboardAndQuery()

		// pop up fresh data on the dashboard and create infinite loop until the user kills the dashboard
		for {
			dashboard.DashboardAndQuery()
			fmt.Println(`The dashboard has been updated. Please fresh your browser.`)
			fmt.Println("Next refresh will occur in 24 hours.")
			time.Sleep(24 * time.Hour)
		}

	} else {

		fmt.Println("The program is collecting the data, running analysis, and will generate a dashboard when the process is complete.")
		fmt.Println("Due to the large number of observations being handled, it will take an hour or so. The console will notify you when the dashboard is live.")

		// Initial query of necessary datasets and insert into PostgreSQL database for the first time this dashboard is run
		queryInsert.QueryInsertVulnerability()
		queryInsert.QueryInsertCovidDaily()
		queryInsert.QueryInsertCovidWeekly()
		queryInsert.QueryInsertTaxi2021()
		queryInsert.QueryInsertTaxi2020()
		queryInsert.QueryInsertTnc21()
		queryInsert.QueryInsertTnc20()
		queryInsert.QueryInsertBoundaries()

		// Call .py scripts that geocode missing community areas and generate charts
		analysis.CallPy()

		fmt.Println(`The dashboard has been served to a local port. Please navigate to "http://localhost:5000".`)
		fmt.Println("Next refresh will occur in 24 hours.")
		dashboard.JustDashboard()
		// pop up fresh data on the dashboard and create infinite loop until the user kills the dashboard
		for {
			dashboard.JustDashboard()
			fmt.Println(`The dashboard has been updated. Please fresh your browser.`)
			fmt.Println("Next refresh will occur in 24 hours.")
			time.Sleep(24 * time.Hour)
		}
	}
}
