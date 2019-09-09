package caterpillarmssql

import (
	"testing"
	"time"

	"github.com/byuoitav/common/log"
)

func TestLoadMSSQL(t *testing.T) {
	log.SetLevel("debug")
	log.L.Debugf("Connecting to DB")
	db, err := GetDB()
	if err != nil {
		log.L.Fatalf("Error %v", err.Error())
		return
	}

	q := `SELECT ExceptionDate, ExceptionType
	FROM ExceptionDates
	WHERE ExceptionDate >= @startdate`

	ti, _ := time.Parse("2006-01-02", "2006-01-02")

	_, err = db.NamedQuery(q, map[string]interface{}{
		"startdate": ti,
	})

	if err != nil {
		log.L.Fatalf("Error %v", err.Error())
		return
	}
}
