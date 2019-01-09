package hatchery

import (
	"sync"

	"github.com/byuoitav/caterpillar/caterpillar"
	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/hatchery/feeder"
	"github.com/byuoitav/caterpillar/hatchery/store"
	"github.com/byuoitav/caterpillar/nydus"
	"github.com/byuoitav/common/log"
)

const (
	running = "running"
)

//Queen .
//It's a starcraft joke...
type Queen struct {
	config       config.Caterpillar
	runMutex     *sync.Mutex
	nydusChannel chan nydus.BulkRecordEntry
}

//SpawnQueen .
func SpawnQueen(c config.Caterpillar, nn chan nydus.BulkRecordEntry) Queen {
	return Queen{
		config:       c,
		runMutex:     &sync.Mutex{},
		nydusChannel: nn,
	}
}

//Run fulfills the job interface for the cron package.
func (q Queen) Run() {

	log.L.Debugf("Obtaining a run lock for %v", q.config.ID)

	//wait for the lock
	q.runMutex.Lock()
	defer q.runMutex.Unlock()
	log.L.Infof("Starting run of %v.", q.config.ID)

	//before we get the info from the store, we need to have the caterpillar
	cat, err := caterpillar.GetCaterpillar(q.config.Type)
	if err != nil {
		log.L.Errorf(err.Addf("Couldn't get the caterpillar %v.", q.config.ID).Error())
		log.L.Debugf("%s", err.Stack)
		return
	}

	//Register the info struct so it'll come back with an assertable type in the interface that was written.
	cat.RegisterGobStructs()

	//get the information from the store
	info, err := store.GetInfo(q.config.ID)
	if err != nil {
		log.L.Errorf(err.Addf("Couldn't get information for caterpillar %v from info store. Returning.", q.config.ID).Error())
		log.L.Debugf("%s", err.Stack)
		return
	}
	log.L.Debugf("State before run: %v", info)

	//get the feeder, from that we can get the number of events.
	feed, err := feeder.GetFeeder(q.config, info.LastEventTime)
	if err != nil {
		log.L.Errorf(err.Addf("Couldn't get feeder for %v from info store. Returning.", q.config.ID).Error())
		log.L.Debugf("%s", err.Stack)
		return
	}

	count, err := feed.GetCount()
	if err != nil {
		log.L.Errorf(err.Addf("Couldn't get event count from feeder for %v from info store. Returning.", q.config.ID).Error())
		log.L.Debugf("%s", err.Stack)
		return
	}

	//Run the caterpillar - this should block until the cateprillar is done chewing through the data.
	state, err := cat.Run(q.config.ID, count, info, q.nydusChannel, q.config, feed.StartFeeding)
	if err != nil {
		log.L.Error(err.Addf("There was an error running caterpillar %v: %v", q.config.ID, err.Error()))
		log.L.Debugf("%s", err.Stack)
		return
	}

	log.L.Debugf("State after run; %v", state)

	err = store.PutInfo(q.config.ID, state)
	if err != nil {
		log.L.Errorf(err.Addf("Couldn't store information for caterpillar %v to info store. Returning.", q.config.ID).Error())
		log.L.Debugf("%s", err.Stack)
		return
	}
}
