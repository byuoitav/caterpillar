package feeder

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/queries"
	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/state-parser/elk"
)

type elkFeeder struct {
	startTime  time.Time
	endTime    time.Time
	eventcount int
	config     config.Caterpillar

	countMutex *sync.Mutex
	countErr   *nerr.E
	countOnce  *sync.Once
}

//GetCount .
func (e *elkFeeder) GetCount() (int, *nerr.E) {

	e.countMutex.Lock()
	e.countOnce.Do(func() { _, e.countErr = e.getElkCount() })
	e.countMutex.Unlock()

	return e.eventcount, e.countErr
}

func (e *elkFeeder) getElkCount() (int, *nerr.E) {
	var err *nerr.E
	var query queries.ELKQueryTemplate

	//get the query
	if e.config.QueryFile != "" {
		query, err = queries.GetQueryTemplateFromFile(e.config.QueryFile)
		if err != nil {
			return 0, err.Addf("Couldn't initialize feeder for caterpillar %v.", e.config.ID)
		}

	} else if e.config.Query != nil {
		b, er := json.Marshal(e.config.Query)
		if er != nil {
			return 0, nerr.Translate(err).Addf("Couldn't process query specified for caterpillar feeder %v", e.config.ID)
		}
		query, err = queries.GetQueryTemplateFromString(b)
		if err != nil {
			return 0, err.Addf("Couldn't initialize feeder for caterpillar %v.", e.config.ID)
		}
	} else {
		//error
		return 0, nerr.Create(fmt.Sprintf("Invalid caterpillar config for %v. Must specify eighte query or query-file.", e.config.ID), "invalid-config")
	}

	query.Query.Bool.Filter = append(query.Query.Bool.Filter, queries.TimeRangeFilter{
		Range: map[string]queries.DateRange{
			e.config.TimeField: queries.DateRange{
				StartTime: e.startTime,
				EndTime:   e.endTime,
			},
		},
	})

	//We marshal and send the count request?
	queryBytes, er := json.Marshal(query)
	if er != nil {
		return 0, nerr.Translate(er).Addf("Couldn't make count request.")
	}

	respBytes, err := elk.MakeELKRequest("POST", fmt.Sprintf("/%v/_count", e.config.Index), queryBytes)
	if err != nil {
		return 0, err.Addf("COuldn't get count of documents for caterpillar %v", e.config.ID)
	}

	var cr queries.CountResponse

	er = json.Unmarshal(respBytes, &cr)
	if er != nil {
		return 0, nerr.Translate(er).Addf("Couldn't get count. Unkown response %s", respBytes)
	}
	e.eventcount = cr.Count

	return cr.Count, nil

}

func (e *elkFeeder) StartFeeding(capacity int) (chan interface{}, *nerr.E) {
	return make(chan interface{}, capacity), nil
}
