// Contains functions that creates a html server on localport:5050

package dashboard

import (
	"io"
	"log"
	"net/http"
	"time"

	analysis "github.com/jhancuch/chicago-dashboard/pkg/analysis"
	queryInsert "github.com/jhancuch/chicago-dashboard/pkg/queryInsert"
)

func HomeHandler(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, htmlContents)
}

// Function that creates the html server and also updates the data and runs analysis in the background. The dashboard must have been
// run before
func DashboardAndQuery() {
	quit := make(chan bool)

	http.DefaultServeMux = new(http.ServeMux)
	myRouter := http.DefaultServeMux
	myRouter.HandleFunc("/", HomeHandler)

	// insert charts
	http.Handle("/figs/", http.StripPrefix("/figs/", http.FileServer(http.Dir("figs"))))

	// set a goroutine so we can query the Chicago Open Data API's while the dashboard is running in the background
	go func() {
		if err := http.ListenAndServe(":5000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	// Pull in new data minus 2020 data, boundry data, and vulnerability data since this dashboard has been run before
	queryInsert.QueryInsertCovidDaily()
	queryInsert.QueryInsertCovidWeekly()
	queryInsert.QueryInsertTaxi2021()
	queryInsert.QueryInsertTnc21()

	// Call .py scripts that geocode missing community areas and generate charts
	analysis.CallPy()

	// close goroutine that is running the server
	close(quit)
}

// This function is utilized for when the dashboard hasn't been run before. It just displays the dashboard.
func JustDashboard() {
	quit := make(chan bool)

	http.DefaultServeMux = new(http.ServeMux)
	myRouter := http.DefaultServeMux
	myRouter.HandleFunc("/", HomeHandler)

	// insert charts
	http.Handle("/figs/", http.StripPrefix("/figs/", http.FileServer(http.Dir("figs"))))

	// set a goroutine so we can query the Chicago Open Data API's while the dashboard is running in the background
	go func() {
		if err := http.ListenAndServe(":5000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(24 * time.Hour)

	// close server
	close(quit)
}
