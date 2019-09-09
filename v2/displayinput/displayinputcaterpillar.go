package displayinputcaterpillar

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/byuoitav/caterpillar/v2/caterpillarmssql"
	"github.com/byuoitav/caterpillar/v2/elkquery"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/wso2services/classschedules/uapiclassschedule"
)

var byuLocation *time.Location

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
	DeviceID   string `json:"DeviceID" db:"DeviceID"`
	RoomID     string `json:"RoomID" db:"RoomID"`
	BuildingID string `json:"BuildingID" db:"BuildingID"`

	StartTime         time.Time `json:"StartTime" db:"StartTime"`
	EndTime           time.Time `json:"EndTime" db:"EndTime"`
	ExceptionDateType string    `json:"ExceptionDateType" db:"ExceptionDateType"`
	StartHour         int       `json:"StartHour" db:"StartHour"`
	StartDayOfWeek    int       `json:"StartDayOfWeek" db:"StartDayOfWeek"`
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
					  "target-device.deviceID": "B66-120-D1"
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

	//Delete anything in SQL / Kibana that is older than the date we're starting at (so if we're redoing we don't have to worry about duplicates)
	log.L.Debugf("Removing future records for %v", deviceName)
	deleteQuery :=
		`DELETE
		FROM DisplayInputMetrics
		WHERE DeviceID = @p1 and StartTime >= @p2`

	_, err = db.Exec(deleteQuery, deviceName, myLastKnownState.LastKnownStateTime)

	if err != nil {
		log.L.Errorf("Unable to remove future Metrics records for %v: %v", deviceName, err.Error())
		return
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

	getEventsQuery = strings.ReplaceAll(getEventsQuery, "$STARTDATE", currentState.StartTime.Format("2006-01-01T15:04-07:00"))
	getEventsQuery = strings.ReplaceAll(getEventsQuery, "$DEVICEID", deviceName)

	query, nerr := elkquery.GetQueryTemplateFromString([]byte(getEventsQuery))
	if nerr != nil {
		log.L.Fatalf("Unable to translate get events query %v", nerr)
	}

	log.L.Debugf("Executing elk query for %v", deviceName)
	response, nerr := elkquery.ExecuteElkQuery("av-delta-events*", query)
	if nerr != nil {
		log.L.Fatalf("Error executing events query %v", nerr)
	}

	//create slicer channels and start slicer
	slicerChannel := make(chan MetricsRecord, 100)
	slicerReadyDoneChannel := make(chan bool)
	var slicerWG sync.WaitGroup
	slicerWG.Add(1)
	go sliceByHourAndClassSchedule(slicerChannel, slicerReadyDoneChannel, &slicerWG)

	log.L.Debugf("Found %v events for %v", len(response.Hits.Hits), deviceName)

	for _, oneEvent := range response.Hits.Hits {
		// if i > 10 {
		// 	log.L.Fatalf("stop")
		// }
		src := oneEvent.Source
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
			//DEBUG DEBUG
			//slicerChannel <- copyOfCurrent
			sliceRecord(copyOfCurrent, nil)
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
		//DEBUG DEBUG
		//slicerChannel <- copyOfCurrent
		sliceRecord(copyOfCurrent, nil)

		currentState.StartTime = lastHourEnd
	}

	//update last state known in sql
	lastKnownStateUpdateQuery :=
		`UPDATE LastKnownStates
		SET LastKnownStateTime = @LastKnownStateTime,
		LastKnownStateJSON = @LastKnownStateJSON
		where DeviceID = @DeviceID`

	lastKnownStateJSON, err := json.Marshal(currentState)

	if err == nil {
		_, err = db.Exec(lastKnownStateUpdateQuery, deviceName, currentState.StartTime, string(lastKnownStateJSON))

		if err != nil {
			log.L.Errorf("Unable to update last known state for %v: %v", deviceName, err.Error())
		}
	}

	//wait for slicers
	slicerReadyDoneChannel <- true
	slicerWG.Wait()
}

func sliceByHourAndClassSchedule(slicerChannel chan MetricsRecord, slicerReadyDoneChannel chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	//start a record store channels and go routine
	storeChannel := make(chan MetricsRecord, 100)
	recordReadyDoneChannel := make(chan bool)
	var recordWG sync.WaitGroup
	recordWG.Add(1)
	go storeRecord(storeChannel, recordReadyDoneChannel, &recordWG)

	//wait for something to come down the channel
ChannelLoop:
	for {
		select {
		case recordToSlice := <-slicerChannel:
			sliceRecord(recordToSlice, storeChannel)
		case <-slicerReadyDoneChannel:
			break ChannelLoop
		}
	}

	//wait for all storage routines to finish
	recordReadyDoneChannel <- true
	wg.Wait()
}

func timeBetween(t, from, to time.Time) bool {
	return t.After(from) && t.Before(to)
}

func sliceRecord(recordToSlice MetricsRecord, storeChannel chan MetricsRecord) {
	//for this record, get the class schedules from the memory cache (or WSO2 if not pulled yet)
	//get all the classes for the whole day so we don't have to hit it a bunch
	classScheduleDate := recordToSlice.StartTime.Truncate(24 * time.Hour)
	classSchedules, _ := uapiclassschedule.GetSimpleClassSchedulesForRoomAndDate(recordToSlice.RoomID, recordToSlice.StartTime)
	//slice up the record on each hour, as well as class schedule boundaries

	for {
		//log.L.Debugf("slicing from %v to %v", recordToSlice.StartTime, recordToSlice.EndTime)

		//see if we have crossed to a different day
		if recordToSlice.StartTime.Truncate(24*time.Hour) != classScheduleDate {
			classSchedules, _ = uapiclassschedule.GetSimpleClassSchedulesForRoomAndDate(recordToSlice.RoomID, recordToSlice.StartTime)
		}

		//determine the hour end time
		hourEndTime := recordToSlice.StartTime.Truncate(time.Hour).Add(time.Hour)
		//log.L.Debugf("Initial end time %v", hourEndTime)
		if hourEndTime.After(recordToSlice.EndTime) {
			hourEndTime = recordToSlice.EndTime
		}

		copy := recordToSlice

		minTime := hourEndTime
		//see if we have any classes that have a start date or end date between our start and end time - if we do, that becomes the new end time
		for _, classSchedule := range classSchedules {

			if classSchedule.StartDateTime.After(minTime) {
				continue
			}

			if classSchedule.StartDateTime.Before(minTime) &&
				classSchedule.StartDateTime.After(recordToSlice.StartTime) &&
				classSchedule.StartDateTime.Before(recordToSlice.EndTime) {
				//there is a class that starts in the middle of our block
				//so that becomes our new end time for the block
				hourEndTime = classSchedule.StartDateTime
				//log.L.Debugf("New end time - start of a class %v", hourEndTime)

			} else if classSchedule.EndDateTime.Before(minTime) &&
				classSchedule.EndDateTime.After(recordToSlice.StartTime) &&
				classSchedule.EndDateTime.Before(recordToSlice.EndTime) {
				//there is class that ends in the middle of our block, but started before
				hourEndTime = classSchedule.EndDateTime
				//log.L.Debugf("New end time - end of a class %v", hourEndTime)

			}

			if classSchedule.StartDateTime.Equal(recordToSlice.StartTime) {
				//our block is starting a new class
				//log.L.Debugf("Block starting a class")
				recordToSlice.IsClass = true
				recordToSlice.TeachingArea = classSchedule.TeachingArea
				recordToSlice.CourseNumber = classSchedule.CourseNumber
				recordToSlice.SectionNumber = classSchedule.SectionNumber
				recordToSlice.ClassName = classSchedule.TeachingArea + " " + classSchedule.CourseNumber
				recordToSlice.ScheduleType = classSchedule.ScheduleType
				recordToSlice.InstructorName = strings.Join(classSchedule.InstructorNames, "|")

				copy = recordToSlice

			} else if classSchedule.EndDateTime.Equal(hourEndTime) {
				//our block is ending at the same time as the class
				//log.L.Debugf("Block ending a class")
				recordToSlice.IsClass = false
				recordToSlice.TeachingArea = ""
				recordToSlice.CourseNumber = ""
				recordToSlice.SectionNumber = ""
				recordToSlice.ClassName = ""
				recordToSlice.ScheduleType = ""
				recordToSlice.InstructorName = ""

				//don't do copy - only future for next round
			}
		}

		//log.L.Debugf("Final end time for copy %v", hourEndTime)
		copy.EndTime = hourEndTime
		copy.StartTime = copy.StartTime.In(byuLocation)
		copy.EndTime = copy.EndTime.In(byuLocation)
		copy.ElapsedSeconds = int(copy.EndTime.Sub(copy.StartTime).Seconds())
		copy.StartHour = copy.StartTime.Hour()
		copy.StartDayOfWeek = int(copy.StartTime.Weekday())
		copy.StartMonth = int(copy.StartTime.Month())
		copy.StartYear = copy.StartTime.Year()

		//send the sliced record to the storage go routine
		//DEBUG DEBUG
		//storeChannel <- copy
		storeRecordInSQL(copy)

		//stop if we've done all of it
		if hourEndTime.Equal(recordToSlice.EndTime) {
			break
		}

		//update and loop
		recordToSlice.StartTime = hourEndTime
	}
}

func storeRecord(storeChannel chan MetricsRecord, recordReadyDoneChannel chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	//wait for something to come down the channel
	for {
		select {
		case recordToStore := <-storeChannel:
			storeRecordInKibana(recordToStore)
			storeRecordInSQL(recordToStore)
		case <-recordReadyDoneChannel:
			return
		}
	}
}

func storeRecordInKibana(recordToStore MetricsRecord) {

}

func storeRecordInSQL(recordToStore MetricsRecord) {

	log.L.Debugf("storing record in %v - %v to %v, class %v, StatusDesc %v",
		recordToStore.DeviceID, recordToStore.StartTime,
		recordToStore.EndTime, recordToStore.ClassName, recordToStore.StatusDesc)
	return
	db, err := caterpillarmssql.GetDB()
	if err != nil {
		log.L.Errorf("Unable to get db to store record on device %v: %v", recordToStore.DeviceID, err.Error())
		return
	}

	q :=
		`INSERT INTO DisplayInputMetrics
	(
		DeviceID,
		RoomID,
		BuildingID,
		StartTime,
		EndTime,
		ExceptionDateType,
		StartHour,
		StartDayOfWeek,
		StartMonth,
		StartYear,
		ElapsedSeconds,
		IsClass,
		TeachingArea,
		CourseNumber,
		SectionNumber,
		ClassName,
		ScheduleType,
		InstructorName,
		Power,
		Blanked,
		InputType,
		Input,
		InputActiveSignal,
		StatusDesc
	)
		VALUES 
	(
		@p1,
		@p2,
		@p3,
		@p4,
		@p5,
		@p6,
		@p7,
		@p8,
		@p9,
		@p10,
		@p11,
		@p12,
		@p13,
		@p14,
		@p15,
		@p16,
		@p17,
		@p18,
		@p19,
		@p20,
		@p21,
		@p22,
		@p23,
		@p24
	)`

	_, err = db.Exec(q,
		recordToStore.DeviceID,
		recordToStore.RoomID,
		recordToStore.BuildingID,
		recordToStore.StartTime,
		recordToStore.EndTime,
		recordToStore.ExceptionDateType,
		recordToStore.StartHour,
		recordToStore.StartDayOfWeek,
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
		recordToStore.StatusDesc)

	if err != nil {
		log.L.Errorf("db error when storing record on device %v: %v", recordToStore.DeviceID, err.Error())
	}

	log.L.Debugf("record stored in %v", recordToStore.DeviceID)
}
