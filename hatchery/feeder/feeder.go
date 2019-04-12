package feeder

import (
	"sync"
	"time"

	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/common/log"
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

var absDateFormat = "2006-01-02 15:04:05"

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

	//check for absolute start/end times if they're there, we overrule the start and end time for the feeder with those.
	if len(c.AbsStart) != 0 {
		if len(c.AbsEnd) == 0 {
			log.L.Fatalf("Bad config for caterpillar %v. If absolute-start is defined must have absolute end", c.ID)
		}

		//we parse the two
		loc, err := time.LoadLocation("America/Denver")
		if err != nil {
			log.L.Fatalf("couldn't load timezone information")
		}

		absoluteStart, err := time.ParseInLocation(absDateFormat, c.AbsStart, loc)
		if err != nil {
			log.L.Fatalf("Bad config for caterpillar %v. Absolute-start in unknown format: %v", c.ID, err.Error())
		}

		absoluteEnd, err := time.ParseInLocation(absDateFormat, c.AbsEnd, loc)
		if err != nil {
			log.L.Fatalf("Bad config for caterpillar %v. Absolute-end in unknown format: %v", c.ID, err.Error())
		}

		e.startTime = absoluteStart
		e.endTime = absoluteEnd
	}

	return e, nil
}
