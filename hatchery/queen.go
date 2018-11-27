package hatchery

import (
	"sync"

	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/common/log"
)

const (
	running = "running"
)

//Queen .
//It's a starcraft joke...
type Queen struct {
	config   config.Caterpillar
	storeLoc string
	runMutex *sync.Mutex
}

//SpawnQueen .
func SpawnQueen(c config.Caterpillar, storeLoc string) Queen {
	return Queen{
		config:   c,
		storeLoc: storeLoc,
		runMutex: &sync.Mutex{},
	}
}

//Run fulfills the job interface for the cron package.
func (q Queen) Run() {

	log.L.Debugf("Obtaining a run lock for %v", q.config.ID)

	//wait for the lock
	q.runMutex.Lock()
	defer q.runMutex.Unlock()
	log.L.Infof("Starting run of %v.", q.config.ID)

	//get the information from the store

	info, err := store.GetInfo(q.storeLoc, q.config.ID)

	//get the number of records since the last run... (check last-event-time from the stored data)

	q.runMutex.Unlock()
}
