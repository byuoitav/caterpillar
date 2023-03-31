package caterpillarmssql

import (
	"database/sql"
	"os"

	"github.com/byuoitav/common/log"
	_ "github.com/denisenkom/go-mssqldb" //load driver
	"github.com/jmoiron/sqlx"
)

var connString string

func init() {
	connString = os.Getenv("METRICS_SQL_CONNECTION_STRING")
	if len(connString) == 0 {
		log.L.Fatalf("Need SQL Connection string")
	}
}

// GetDB get a db object so we can do
func GetDB() (*sqlx.DB, error) {

	db, err := sqlx.Connect("sqlserver", connString)

	if err != nil {
		log.L.Debugf("Error connecting to SQL %v", err.Error())
		return nil, err
	}
	log.L.Debugf("Connected to DB")
	return db, nil
}

// GetRawDB ..
func GetRawDB() (*sql.DB, error) {
	db, err := sql.Open("mssql", connString)

	if err != nil {
		log.L.Debugf("Error connecting to SQL %v", err.Error())
		return nil, err
	}

	return db, nil
}
