package test

import (
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/byuoitav/caterpillar/caterpillar/catinter"
	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/nydus"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/common/v2/events"
)

var gobRegisterOnce sync.Once

func init() {
	gobRegisterOnce = sync.Once{}
}

//Caterpillar is a test caterpillar
type Caterpillar struct {
}

//DataStruct .
type DataStruct struct {
}

//OutputStruct .
type OutputStruct struct {
	RecordCount   int       `json:"record-count"`
	WindowStart   time.Time `json:"window-start"`
	WindowEnd     time.Time `json:"window-end"`
	RunTime       string    `json:"time.Duration"`
	CaterpillarID string    `json:"caterpillar-id"`
}

//GetCaterpillar .
func GetCaterpillar() (catinter.Caterpillar, *nerr.E) {
	return &Caterpillar{}, nil
}

//Run fulfils the Caterpillar interface.
func (c *Caterpillar) Run(id string, recordCount int, state config.State, outChan chan nydus.BulkRecordEntry, config config.Caterpillar, GetData func(int) (chan interface{}, *nerr.E)) (config.State, *nerr.E) {

	log.L.Debugf("Running %v on %v records", id, recordCount)
	log.L.Debugf("State Document %+v", state)

	//check the config
	index, ok := config.TypeConfig["output-index"]
	if !ok {
		return state, nerr.Create(fmt.Sprintf("Missing config item for Caterpillar type %v. Need output-index", config.Type), "invalid-config")
	}

	startTime := time.Now()
	inchan, err := GetData(100)
	if err != nil {
		return state, err.Addf("Couldn't run caterpillar. Issue with initial data retreival")
	}
	var curEventTime time.Time
	firstEventTime := time.Time{}

	for i := range inchan {
		v, ok := i.(events.Event)
		if !ok {
			log.L.Infof("Couldn't assert that event was expected type. Event body: %v", i)
		}

		if firstEventTime.Equal(time.Time{}) {
			firstEventTime = v.Timestamp
		}

		curEventTime = v.Timestamp
		log.L.Debugf("processing %v", v.Timestamp.Format(time.RFC3339))
	}

	state.LastEventTime = curEventTime

	var testout OutputStruct

	testout.RunTime = fmt.Sprintf("%v", time.Now().Sub(startTime))
	testout.WindowStart = firstEventTime
	testout.WindowEnd = curEventTime
	testout.CaterpillarID = id
	testout.RecordCount = recordCount

	outChan <- nydus.BulkRecordEntry{
		Header: nydus.BulkRecordHeader{
			Index: nydus.HeaderIndex{
				Index: index,
				Type:  "record",
			},
		},
		Body: testout,
	}

	return state, nil
}

//RegisterGobStructs .
func (c *Caterpillar) RegisterGobStructs() {
	gobRegisterOnce.Do(func() {
		gob.Register(DataStruct{})
	})
}
