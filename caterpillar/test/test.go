package test

import (
	"encoding/gob"
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

//GetCaterpillar .
func GetCaterpillar() (catinter.Caterpillar, *nerr.E) {
	return &Caterpillar{}, nil
}

//Run fulfils the Caterpillar interface.
func (c *Caterpillar) Run(id string, recordCount int, state config.State, outChan chan nydus.BulkRecordEntry, GetData func(int) (chan interface{}, *nerr.E)) *nerr.E {

	log.L.Debugf("Running %v on %v records", id, recordCount)
	log.L.Debugf("State Document %+v", state)

	inchan, err := GetData(100)
	if err != nil {
		return err.Addf("Couldn't run caterpillar. Issue with initial data retreival")
	}
	for i := range inchan {
		v, ok := i.(events.Event)
		if !ok {
			log.L.Infof("Couldn't assert that event was expected type. Event body: %v", i)
		}
		log.L.Debugf("processing %v", v.Timestamp.Format(time.RFC3339))
	}

	return nil
}

//RegisterGobStructs .
func (c *Caterpillar) RegisterGobStructs() {
	gobRegisterOnce.Do(func() {
		gob.Register(DataStruct{})
	})
}
