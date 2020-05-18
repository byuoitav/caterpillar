package displayinputcaterpillar

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	mssql "github.com/denisenkom/go-mssqldb"

	"github.com/byuoitav/caterpillar/v2/caterpillarmssql"
	"github.com/byuoitav/caterpillar/v2/elkquery"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/wso2services/classschedules/uapiclassschedule"
)

var byuLocation *time.Location
var numOfRoomsToDoAtOnce = 10

func init() {
	byuLocation, _ = time.LoadLocation("America/Denver")
}

type deviceAggregations struct {
	Devices struct {
		Buckets []struct {
			DocCount int    `json:"doc_count"`
			Key      string `json:"key"`
		} `json:"buckets"`
	} `json:"devices"`
}

type lastKnownState struct {
	LastKnownStateID   int       `db:"LastKnownStateID"`
	DeviceID           string    `db:"DeviceID"`
	LastKnownStateTime time.Time `db:"LastKnownStateTime"`
	LastKnownStateJSON string    `db:"LastKnownStateJSON"`
}

//MetricsRecord ...
type MetricsRecord struct {
	DeviceID       string `json:"DeviceID" db:"DeviceID"`
	RoomID         string `json:"RoomID" db:"RoomID"`
	BuildingID     string `json:"BuildingID" db:"BuildingID"`
	DeviceIDPrefix string `json:"DeviceIDPrefix" db:"DeviceIDPrefix"`

	StartTime         time.Time `json:"StartTime" db:"StartTime"`
	EndTime           time.Time `json:"EndTime" db:"EndTime"`
	ExceptionDateType string    `json:"ExceptionDateType" db:"ExceptionDateType"`
	StartHour         int       `json:"StartHour" db:"StartHour"`
	StartDayOfWeek    int       `json:"StartDayOfWeek" db:"StartDayOfWeek"`
	StartDay          int       `json:"StartDay" db:"StartDay"`
	StartMonth        int       `json:"StartMonth" db:"StartMonth"`
	StartYear         int       `json:"StartYear" db:"StartYear"`
	ElapsedSeconds    int       `json:"ElapsedSeconds" db:"ElapsedSeconds"`

	IsClass        bool   `json:"IsClass" db:"IsClass"`
	TeachingArea   string `json:"TeachingArea" db:"TeachingArea"`
	CourseNumber   string `json:"CourseNumber" db:"CourseNumber"`
	SectionNumber  string `json:"SectionNumber" db:"SectionNumber"`
	ClassName      string `json:"ClassName" db:"ClassName"`
	ScheduleType   string `json:"ScheduleType" db:"ScheduleType"`
	InstructorName string `json:"InstructorName" db:"InstructorName"`

	Power             string `json:"Power" db:"Power"`
	Blanked           string `json:"Blanked" db:"Blanked"`
	InputType         string `json:"InputType" db:"InputType"`
	Input             string `json:"Input" db:"Input"`
	InputActiveSignal string `json:"InputActiveSignal" db:"InputActiveSignal"`
	StatusDesc        string `json:"StatusDesc" db:"StatusDesc"`
}

func StartDisplayInputCaterpillar(building string) {

	log.L.Debugf("Starting Display Input Caterpillar")

	//get the value from SQL the last day we ran - we'll do the aggregation query back a month to make sure we got new ones
	db, err := caterpillarmssql.GetDB()
	defer db.Close()
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
				  "target-device.roomID": "$BUILDING"
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
					"size": 10000,
					"order": { "_term" : "asc" }
				  }
			  }
		  },
		"size": 0
	  }
	  
	`

	q = strings.ReplaceAll(q, "$STARTDATE", "2017-01-01")
	q = strings.ReplaceAll(q, "$BUILDING", building)

	query, nerr := elkquery.GetQueryTemplateFromString([]byte(q))
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

	//process each device
	for _, bucket := range responseAggs.Devices.Buckets {
		caterpillarDevice(bucket.Key)
	}
}

func caterpillarDevice(deviceName string) {
	//Get from SQL the last state known
	db, err := caterpillarmssql.GetDB()
	defer db.Close()
	if err != nil {
		log.L.Errorf("Unable to get db working on device %v: %v", deviceName, err.Error())
		return
	}

	lastKnownStateQuery :=
		`SELECT *
		from LastKnownStates
		where DeviceID = @p1`

	var myLastKnownState lastKnownState
	var myLastKnownStateSlice []lastKnownState

	err = db.Select(&myLastKnownStateSlice, lastKnownStateQuery, deviceName)
	if err != nil {
		log.L.Errorf("Unable to get last known state of device %v: %v", deviceName, err.Error())
		return
	}

	if len(myLastKnownStateSlice) == 0 {
		//create new
		myLastKnownState = lastKnownState{
			DeviceID:           deviceName,
			LastKnownStateJSON: "",
		}
	} else {
		myLastKnownState = myLastKnownStateSlice[0]
	}

	//unmarshal from the DB into an object
	var currentState MetricsRecord

	if len(myLastKnownState.LastKnownStateJSON) > 0 {
		json.Unmarshal([]byte(myLastKnownState.LastKnownStateJSON), &currentState)
	} else {
		roomParts := strings.Split(deviceName, "-")
		currentState = MetricsRecord{
			DeviceID:          deviceName,
			RoomID:            roomParts[0] + "-" + roomParts[1],
			BuildingID:        roomParts[0],
			DeviceIDPrefix:    strings.TrimRight(roomParts[2], "0123456789"),
			Power:             "unknown",
			Blanked:           "unknown",
			InputType:         "unknown",
			Input:             "unknown",
			InputActiveSignal: "unknown",
		}
	}

	//Get all events from delta since that date
	getEventsQuery :=
		`{
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
				"key": [ "input", "power", "active-signal", "blanked" ]
			  }
			},
			{
			  "term": {
				"target-device.deviceID": "$DEVICEID"
			  }
			}
		  ]
		}
	  },
	  "size": 10000,
	  "sort": [ { "timestamp": "asc" } ]  
	}`

	log.L.Debugf("Querying events for %v after %v", deviceName, currentState.StartTime.Format("2006-01-02T15:04-07:00"))
	getEventsQuery = strings.ReplaceAll(getEventsQuery, "$STARTDATE", currentState.StartTime.Format("2006-01-02T15:04-07:00"))
	getEventsQuery = strings.ReplaceAll(getEventsQuery, "$DEVICEID", deviceName)

	query, nerr := elkquery.GetQueryTemplateFromString([]byte(getEventsQuery))
	if nerr != nil {
		log.L.Fatalf("Unable to translate get events query %v", nerr)
	}

	log.L.Debugf("Executing elk query for %v", deviceName)
	response, nerr := elkquery.ExecuteElkQuery("av-delta-events*", query)
	if nerr != nil {
		log.L.Fatalf("Error executing events query for %v: %v", deviceName, nerr)
	}

	//Delete anything in SQL / Kibana that is older than the date we're starting at (so if we're redoing we don't have to worry about duplicates)
	if !myLastKnownState.LastKnownStateTime.IsZero() {
		log.L.Debugf("Removing future records for %v after %v", deviceName, myLastKnownState.LastKnownStateTime)
		deleteQuery :=
			`DELETE
		FROM DisplayInputMetrics
		WHERE DeviceID = @p1 and StartTime >= @p2`

		sqlResult, err := db.Exec(deleteQuery, deviceName, myLastKnownState.LastKnownStateTime)

		if err != nil {
			log.L.Errorf("Unable to remove future Metrics records for %v: %v", deviceName, err.Error())
			return
		}

		rowsAffected, err := sqlResult.RowsAffected()

		if err != nil {
			log.L.Errorf("Unable to get rows affected for %v: %v", deviceName, err.Error())
			return
		}

		log.L.Debugf("Removed %v rows for %v", rowsAffected, deviceName)
	}

	log.L.Debugf("Found %v events for %v", len(response.Hits.Hits), deviceName)

	//start up a storage channel
	storeChannel := make(chan MetricsRecord, 100)
	var storageWaitGroup sync.WaitGroup
	storageWaitGroup.Add(1)

	go storeRecord(storeChannel, &storageWaitGroup)

	realEventCount := 0
	for _, oneEvent := range response.Hits.Hits {
		src := oneEvent.Source
		if len(src.Value) == 0 {
			continue
		}

		realEventCount++
		src.Timestamp = src.Timestamp.Truncate(time.Second)

		//Go through and create records for each change (should be each event)
		if !currentState.StartTime.IsZero() {
			//finish off the previous record and then send it to the slicer

			//deep copy
			copyOfCurrent := currentState

			//set the end time and duration
			copyOfCurrent.EndTime = src.Timestamp
			copyOfCurrent.ElapsedSeconds = int(copyOfCurrent.EndTime.Sub(copyOfCurrent.StartTime).Seconds())

			//send to slicer
			sliceRecord(copyOfCurrent, storeChannel)
		}

		//now update the current state
		currentState.StartTime = src.Timestamp

		if src.Key == "power" {
			currentState.Power = src.Value
		} else if src.Key == "input" {
			currentState.Input = src.Value
			currentState.InputType = strings.TrimRight(src.Value, "0123456789")
		} else if src.Key == "blanked" {
			currentState.Blanked = src.Value
		} else if src.Key == "active-signal" {
			currentState.InputActiveSignal = src.Value
		}

		if strings.ToLower(currentState.Power) == "standby" {
			currentState.StatusDesc = "Off"
		} else if strings.ToLower(currentState.Power) == "on" && strings.ToLower(currentState.Blanked) == "unknown" {
			currentState.StatusDesc = "On"
		} else if strings.ToLower(currentState.Power) == "on" && strings.ToLower(currentState.Blanked) == "true" {
			currentState.StatusDesc = "Blanked"
		} else if strings.ToLower(currentState.Power) == "on" && strings.ToLower(currentState.Blanked) == "false" {
			currentState.StatusDesc = currentState.Input + "-" + currentState.InputActiveSignal
		}
	}

	if realEventCount == 0 {
		return
	}

	if response.Hits.Total == len(response.Hits.Hits) {
		//Update the current state record to be up to the latest whole hour that is more than an hour old
		lastHourEnd := time.Now()
		lastHourEnd = lastHourEnd.Truncate(time.Hour)
		lastHourEnd = lastHourEnd.Add(-1 * time.Hour)

		if currentState.StartTime.Before(lastHourEnd) {
			//update and then send to the slicer
			copyOfCurrent := currentState

			//set the end time and duration
			copyOfCurrent.EndTime = lastHourEnd
			copyOfCurrent.ElapsedSeconds = int(copyOfCurrent.EndTime.Sub(copyOfCurrent.StartTime).Seconds())

			//send to slicer
			sliceRecord(copyOfCurrent, storeChannel)

			currentState.StartTime = lastHourEnd
		}
	}

	//wait for storage to finish
	//signal that we're done with the store channel
	close(storeChannel)

	//wait for all storage routines to finish
	storageWaitGroup.Wait()

	//update last state known in sql
	lastKnownStateUpdateQuery :=
		`EXEC UpdateLastKnownState @p1, @p2, @p3`

	lastKnownStateJSON, err := json.Marshal(currentState)

	if err == nil {
		_, err = db.Exec(lastKnownStateUpdateQuery, deviceName, currentState.StartTime, string(lastKnownStateJSON))

		if err != nil {
			log.L.Errorf("Unable to update last known state for %v: %v", deviceName, err.Error())
		}
	}
}

func timeBetween(t, from, to time.Time) bool {
	return t.After(from) && t.Before(to)
}

func truncateDateOnlyWithTimezone(toRound time.Time) time.Time {
	rounded := time.Date(toRound.Year(), toRound.Month(), toRound.Day(), 0, 0, 0, 0, toRound.Location())
	return rounded
}

func sliceRecord(recordToSlice MetricsRecord, storeChannel chan MetricsRecord) {
	//for this record, get the class schedules from the memory cache (or WSO2 if not pulled yet)
	//get all the classes for the whole day so we don't have to hit it a bunch
	//debugTime, _ := time.Parse(time.RFC822, "05 Feb 20 06:00 UTC")

	classScheduleDate := truncateDateOnlyWithTimezone(recordToSlice.StartTime.In(byuLocation))
	classSchedules, _ := uapiclassschedule.GetSimpleClassSchedulesForRoomAndDate(recordToSlice.RoomID, classScheduleDate)

	// if recordToSlice.EndTime.After(debugTime) {
	// 	log.L.Debugf("Start time = " + recordToSlice.StartTime.String())
	// 	log.L.Debugf("Start time (BYU) = " + recordToSlice.StartTime.In(byuLocation).String())
	// 	log.L.Debugf("%v classes found for date %s", len(classSchedules), classScheduleDate.String())
	// }

	//slice up the record on each hour, as well as class schedule boundaries
	for {
		//see if we have crossed to a different day
		checkDate := truncateDateOnlyWithTimezone(recordToSlice.StartTime.In(byuLocation))
		if checkDate != classScheduleDate {
			classScheduleDate := truncateDateOnlyWithTimezone(recordToSlice.StartTime.In(byuLocation))
			classSchedules, _ = uapiclassschedule.GetSimpleClassSchedulesForRoomAndDate(recordToSlice.RoomID, classScheduleDate)
			//log.L.Debugf("%v classes found for %s", len(classSchedules), classScheduleDate.String())
		}

		//determine the hour end time
		hourEndTime := recordToSlice.StartTime.Truncate(time.Hour).Add(time.Hour)
		//log.L.Debugf("Initial end time %v", hourEndTime)
		if hourEndTime.After(recordToSlice.EndTime) {
			hourEndTime = recordToSlice.EndTime
		}

		//assume no class unless we find one
		recordToSlice.IsClass = false
		recordToSlice.TeachingArea = ""
		recordToSlice.CourseNumber = ""
		recordToSlice.SectionNumber = ""
		recordToSlice.ClassName = ""
		recordToSlice.ScheduleType = ""
		recordToSlice.InstructorName = ""

		copy := recordToSlice

		//see if we have any classes that have a start date or end date between our start and end time - if we do, that becomes the new end time
		for _, classSchedule := range classSchedules {

			if classSchedule.StartDateTime.After(hourEndTime) {
				continue
			}

			//see if we need to adjust the time of this slice
			if classSchedule.StartDateTime.Before(hourEndTime) &&
				classSchedule.StartDateTime.After(recordToSlice.StartTime) &&
				classSchedule.StartDateTime.Before(recordToSlice.EndTime) {
				//there is a class that starts in the middle of our block
				//so that becomes our new end time for the block
				hourEndTime = classSchedule.StartDateTime
				//log.L.Debugf("New end time - start of a class %v", hourEndTime)

			} else if classSchedule.EndDateTime.Before(hourEndTime) &&
				classSchedule.EndDateTime.After(recordToSlice.StartTime) &&
				classSchedule.EndDateTime.Before(recordToSlice.EndTime) {
				//there is class that ends in the middle of our block, but started before
				hourEndTime = classSchedule.EndDateTime
				//log.L.Debugf("New end time - end of a class %v", hourEndTime)
			}

			//now see if this slice is part of the class or not
			if (classSchedule.StartDateTime.Equal(recordToSlice.StartTime) ||
				classSchedule.StartDateTime.Before(recordToSlice.StartTime)) &&
				(classSchedule.EndDateTime.Equal(hourEndTime) ||
					classSchedule.EndDateTime.After(hourEndTime)) {
				//the whole block is in our class
				recordToSlice.IsClass = true
				recordToSlice.TeachingArea = classSchedule.TeachingArea
				recordToSlice.CourseNumber = classSchedule.CourseNumber
				recordToSlice.SectionNumber = classSchedule.SectionNumber
				recordToSlice.ClassName = classSchedule.TeachingArea + " " + classSchedule.CourseNumber
				recordToSlice.ScheduleType = classSchedule.ScheduleType
				recordToSlice.InstructorName = strings.Join(classSchedule.InstructorNames, "|")

				copy = recordToSlice
			}
		}

		//log.L.Debugf("Final end time for copy %v", hourEndTime)
		copy.EndTime = hourEndTime
		copy.StartTime = copy.StartTime.In(byuLocation)
		copy.EndTime = copy.EndTime.In(byuLocation)
		copy.ElapsedSeconds = int(copy.EndTime.Sub(copy.StartTime).Seconds())
		copy.StartHour = copy.StartTime.Hour()
		copy.StartDayOfWeek = int(copy.StartTime.Weekday())
		copy.StartDay = int(copy.StartTime.Day())
		copy.StartMonth = int(copy.StartTime.Month())
		copy.StartYear = copy.StartTime.Year()

		//send the sliced record to the storage go routine
		storeChannel <- copy

		//stop if we've done all of it
		if hourEndTime.Equal(recordToSlice.EndTime) {
			break
		}

		//update and loop
		recordToSlice.StartTime = hourEndTime
	}
}

func storeRecord(storeChannel chan MetricsRecord, wg *sync.WaitGroup) {
	defer wg.Done()

	var RecordsToStore []MetricsRecord

	//wait for something to come down the channel
	for {
		recordToStore, more := <-storeChannel

		if more {
			RecordsToStore = append(RecordsToStore, recordToStore)
			//do them 5000 at a time
			if len(RecordsToStore) > 5000 {
				bulkInsertToSQL(RecordsToStore)
				//clear it out
				RecordsToStore = []MetricsRecord{}
			}
		} else {
			break
		}
	}

	//do the bulk insert
	if len(RecordsToStore) > 0 {
		bulkInsertToSQL(RecordsToStore)
	}
}

func bulkInsertToSQL(recordsToStore []MetricsRecord) {

	log.L.Debugf("storing  %v records to SQL for device %v",
		len(recordsToStore), recordsToStore[0].DeviceID)

	db, err := caterpillarmssql.GetRawDB()
	defer db.Close()
	if err != nil {
		log.L.Errorf("Unable to get raw db to store records on device %v: %v", recordsToStore[0].DeviceID, err.Error())
		return
	}

	txn, err := db.Begin()

	if err != nil {
		log.L.Errorf("Unable to start txn to store records on device %v: %v", recordsToStore[0].DeviceID, err.Error())
		return
	}

	stmt, err := txn.Prepare(mssql.CopyIn("DisplayInputMetrics", mssql.BulkOptions{},
		"DeviceID",
		"RoomID",
		"BuildingID",
		"DeviceIDPrefix",
		"StartTime",
		"EndTime",
		"ExceptionDateType",
		"StartHour",
		"StartDayOfWeek",
		"StartDay",
		"StartMonth",
		"StartYear",
		"ElapsedSeconds",
		"IsClass",
		"TeachingArea",
		"CourseNumber",
		"SectionNumber",
		"ClassName",
		"ScheduleType",
		"InstructorName",
		"Power",
		"Blanked",
		"InputType",
		"Input",
		"InputActiveSignal",
		"StatusDesc",
	))

	if err != nil {
		log.L.Errorf("Unable to start copy in to store records on device %v: %v", recordsToStore[0].DeviceID, err.Error())
		return
	}

	for i, recordToStore := range recordsToStore {
		_, err = stmt.Exec(
			recordToStore.DeviceID,
			recordToStore.RoomID,
			recordToStore.BuildingID,
			recordToStore.DeviceIDPrefix,
			recordToStore.StartTime,
			recordToStore.EndTime,
			recordToStore.ExceptionDateType,
			recordToStore.StartHour,
			recordToStore.StartDayOfWeek,
			recordToStore.StartDay,
			recordToStore.StartMonth,
			recordToStore.StartYear,
			recordToStore.ElapsedSeconds,
			recordToStore.IsClass,
			recordToStore.TeachingArea,
			recordToStore.CourseNumber,
			recordToStore.SectionNumber,
			recordToStore.ClassName,
			recordToStore.ScheduleType,
			recordToStore.InstructorName,
			recordToStore.Power,
			recordToStore.Blanked,
			recordToStore.InputType,
			recordToStore.Input,
			recordToStore.InputActiveSignal,
			recordToStore.StatusDesc,
		)

		if err != nil {
			log.L.Errorf("db error when storing record %v on device %v: %v", i, recordToStore.DeviceID, err.Error())
		}
	}

	result, err := stmt.Exec()

	if err != nil {
		log.L.Errorf("Error executing statement on device %v: %v", recordsToStore[0].DeviceID, err.Error())
	}

	err = stmt.Close()

	if err != nil {
		log.L.Errorf("Error closing statement on device %v: %v", recordsToStore[0].DeviceID, err.Error())
	}

	err = txn.Commit()

	if err != nil {
		log.L.Errorf("Error committing txn on device %v: %v", recordsToStore[0].DeviceID, err.Error())
	}

	x, err := result.RowsAffected()

	log.L.Debugf("%v record stored in %v", x, recordsToStore[0].DeviceID)

	if err != nil {
		log.L.Errorf("Error getting record count on device %v: %v", recordsToStore[0].DeviceID, err.Error())
	}
}
