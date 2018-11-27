package hatchery

import (
	"github.com/byuoitav/caterpillar/config"
)

//Queen .
//It's a starcraft joke...
type Queen struct {
	config   config.Caterpillar
	storeLoc string
	state    string
}

//SpawnQueen .
func SpawnQueen(c config.Caterpillar, storeLoc string) Queen {
	return Queen{
		config:   c,
		storeLoc: storeLoc,
	}
}

//Run fulfills the job interface for the cron package.
func (q Queen) Run() {

}
