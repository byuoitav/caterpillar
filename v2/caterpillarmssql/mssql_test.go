package caterpillarmssql

import (
	
)

// func TestLoadMSSQL(t *testing.T) {
// 	log.SetLevel("debug")
// 	log.L.Debugf("Connecting to DB")
// 	db, err := getDB()
// 	if err != nil {
// 		log.L.Fatalf("Error %v", err.Error())
// 		return
// 	}

// 	q := `
// 	SELECT ExceptionDate, ExceptionType
// 	FROM ExceptionDates
// 	`

// 	exceptionDates := []ExceptionDateRecord{}
// 	err = db.Select(&exceptionDates, q)
// 	if err != nil {
// 		log.L.Fatalf("Error %v", err.Error())
// 		return
// 	}

// 	for _, excepDate := range exceptionDates {
// 		log.L.Debugf("%v-%v", excepDate.ExceptionDate, excepDate.ExceptionType)
// 	}
// }
