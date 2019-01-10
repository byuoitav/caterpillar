package feeder

import (
	"sync"
	"time"

	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/common/nerr"
)

const (
	//EventBufferInterval  is just so we can deal with events that are lagging a bit behind...
	EventBufferInterval = -10 * time.Minute
)

//A Feeder handles the feeding of a caterpillar, providing it with data to work through.
type Feeder interface {
	GetCount() (int, *nerr.E)
	StartFeeding(capacity int) (chan interface{}, *nerr.E)
}

//GetFeeder .
func GetFeeder(c config.Caterpillar, lastEventTime time.Time) (Feeder, *nerr.E) {
	if lastEventTime.Equal(time.Time{}) {

		//If it equals the default value, check the config for max-interval
		if c.MaxInterval != "" {
			d, err := time.ParseDuration(c.MaxInterval)
			if err != nil {
				return nil, nerr.Translate(err).Addf("couldn't parse max-interval specified.")
			}
			lastEventTime = time.Now().Add(-1 * d) //go back
		}
	}

	e := &elkFeeder{
		startTime:  lastEventTime,
		endTime:    time.Now().Add(EventBufferInterval),
		config:     c,
		countOnce:  &sync.Once{},
		countMutex: &sync.Mutex{},
	}

	return e, nil
}
