package displayinputcaterpillar

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/byuoitav/caterpillar/v2/caterpillarmssql"
	"github.com/byuoitav/caterpillar/v2/elkquery"
	"github.com/byuoitav/common/log"
)

type deviceAggregations struct {
	Devices struct {
		Buckets []struct {
			DocCount int    `json:"doc_count"`
			Key      string `json:"key"`
		} `json:"buckets"`
	} `json:"devices"`
}

//MetricsRecord ...
type MetricsRecord struct {
	DeviceID   string `json:"DeviceID" db:"DeviceID"`
	RoomID     string `json:"RoomID" db:"RoomID"`
	BuildingID string `json:"BuildingID" db:"BuildingID"`
}

func startDisplayInputCaterpillar() {

	log.L.Debugf("Starting Display Input Caterpillar")

	//get the value from SQL the last day we ran - we'll do the aggregation query back a month to make sure we got new ones
	db, err := caterpillarmssql.GetDB()

	if err != nil {
		log.L.Fatalf("Unable to get DB connection %v", err)
	}

	var lastKnownStateTime struct {
		LastKnownStateTime time.Time `db:"LastKnownStateTime"`
	}
	err = db.Get(&lastKnownStateTime, "SELECT isnull(min(LastKnownStateTime), '1/1/1980') as LastKnownStateTime from LastKnownStates")
	if err != nil {
		log.L.Fatalf("Unable to get min last known state time %v", err)
	}

	//log.L.Debugf("%v", lastKnownStateTime.LastKnownStateTime)

	//run the aggregation query to get the list of devices
	q := `
	{
		"query": {
		  "bool": {
			"filter": [
			  {
				"range": {
				  "timestamp": {
					"gt": "$STARTDATE"
				  }
				}
			  },
			  {
				"terms": {
				  "key": [ "input", "power", "active-signal", "blanked"  ]
				}
			  },
			  {
				  "term": {
					  "target-device.roomID": "ITB-1006"
				  }
			  }
			]
		  }
		},
		"aggs" : {
			  "devices" : {
				  "terms" : 
				  { 
					"field" : "target-device.deviceID",
					"size": 10000
				  }
			  }
		  },
		"size": 0
	  }
	  
	`

	q = strings.ReplaceAll(q, "$STARTDATE", lastKnownStateTime.LastKnownStateTime.Format("2006-01-01T15:04-07:00"))
	log.L.Debugf("%v", q)
	query, nerr := elkquery.GetQueryTemplateFromString([]byte(q))
	log.L.Debugf("%v", err != nil)
	if nerr != nil {
		log.L.Fatalf("Unable to translate query string %v", nerr)
	}

	response, nerr := elkquery.ExecuteElkQuery("av-delta-events*", query)
	if nerr != nil {
		log.L.Fatalf("Error executing query %v", nerr)
	}

	var responseAggs deviceAggregations
	x, _ := json.Marshal(response.Aggregations)
	err = json.Unmarshal(x, &responseAggs)
	if err != nil {
		log.L.Fatalf("Unable to convert device aggs")
	}

	var wg sync.WaitGroup
	//launch a go routine for each of the devices
	for _, bucket := range responseAggs.Devices.Buckets {
		wg.Add(1)
		go caterpillarDevice(bucket.Key, &wg)
	}

	//wait for them all to finish
	wg.Wait()

	//wait for turn off message, run now message, or the timeout and then do it again
}

func caterpillarDevice(deviceName string, wg *sync.WaitGroup) {
	defer wg.Done()

	//Get from SQL the last state known
	db, err := caterpillarmssql.GetDB()
	if err != nil {
		log.L.Fatalf("Unable to get db working on device %v: %v", deviceName, err.Error())
	}

	//Delete anything in SQL / Kibana that is older than the date we're starting at (so if we're redoing we don't have to worry about duplicates)

	//Get all events from delta since that date

	//Go through and create records for each change (should be each event)
	//send to the slicer

	//Update the current state record to be up to the latest whole hour that is more than an hour old
	//send to the slicer

	//update last state known in sql
}
