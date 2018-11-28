package hatchery

import (
	"sync"

	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/hatchery/store"
	"github.com/byuoitav/common/log"
)

const (
	running = "running"
)

//Queen .
//It's a starcraft joke...
type Queen struct {
	config   config.Caterpillar
	runMutex *sync.Mutex
}

//SpawnQueen .
func SpawnQueen(c config.Caterpillar) Queen {
	return Queen{
		config:   c,
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
	store.GetInfo(q.config.ID)

	//get the feeder, from that we can get the number of events.

	q.runMutex.Unlock()

}
