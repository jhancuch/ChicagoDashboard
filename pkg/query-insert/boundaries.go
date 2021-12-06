// Obtain geographical boundaries for community areas

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
type BoundariesTemp []struct {
	TheGeom struct {
		Type        string          `json:"type"`
		Coordinates [][][][]float64 `json:"coordinates"`
	} `json:"the_geom"`
	Perimeter string `json:"perimeter"`
	Area      string `json:"area"`
	Comarea   string `json:"comarea"`
	ComareaID string `json:"comarea_id"`
	AreaNumbe string `json:"area_numbe"`
	Community string `json:"community"`
	AreaNum1  string `json:"area_num_1"`
	ShapeArea string `json:"shape_area"`
	ShapeLen  string `json:"shape_len"`
}

func QueryInsertBoundaries() {

	// Query from the API
	resp, err := http.Get("https://data.cityofchicago.org/resource/igwz-8jzy.json")
	CheckError(err)
	defer resp.Body.Close()

	// Convert response into bytes
	body, err := ioutil.ReadAll(resp.Body)
	CheckError(err)

	// Unmarshal response in bytes into struct
	var queryResult BoundariesTemp
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
	_, err = db.Exec(`DELETE FROM "boundaries"`)
	CheckError(err)

	// the following lines allow for a bulk insert
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("boundaries", "the_geom", "perimeter", "area", "comarea", "comarea_id", "area_numbe", "community", "area_num_1", "shape_area", "shape_len"))
	if err != nil {
		log.Fatal(err)
	}

	for _, result := range queryResult {
		_, err = stmt.Exec(result.TheGeom, result.Perimeter, result.Area, result.Comarea, result.ComareaID, result.AreaNumbe, result.Community,
			result.AreaNum1, result.ShapeArea, result.ShapeLen)
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
