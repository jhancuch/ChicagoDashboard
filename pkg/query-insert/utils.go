// Shared constants and functions across the queryInsert package

package queryInsert

import (
	"fmt"
)

const (
	host     = ""
	port     = 1
	user     = ""
	password = ""
	dbname   = ""
)

func CheckError(err error) {
	if err != nil {
		fmt.Print(err.Error())
	}
}
