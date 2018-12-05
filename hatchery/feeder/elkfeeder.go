package feeder

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/queries"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/state-parser/elk"
	reflections "gopkg.in/oleiade/reflections.v1"
)

//MaxSize is the maximum number of events to get at one time from ELK
var MaxSize = 10000

type elkFeeder struct {
	startTime    time.Time
	endTime      time.Time
	eventcount   int
	eventssent   int
	eventChannel chan interface{}

	config config.Caterpillar

	curWindowStart time.Time
	windowLength   time.Duration
	baseQuery      queries.ELKQueryTemplate

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

	e.baseQuery = query

	q, err := e.getCountQuery()
	if err != nil {
		return 0, err.Addf("Couln't get count for caterpillar %v", e.config.ID)
	}

	queryBytes, er := json.Marshal(q)
	if er != nil {
		return 0, nerr.Translate(er).Addf("Couldn't get count for caterpillar %v", e.config.ID)
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

	log.L.Debugf("Count %v returned %v records", e.config.Index, cr.Count)
	return cr.Count, nil

}

func (e *elkFeeder) StartFeeding(capacity int) (chan interface{}, *nerr.E) {
	//make our channel
	e.eventChannel = make(chan interface{}, capacity)

	var err *nerr.E

	//get our first batch
	e.windowLength = calculateBatches(e.eventcount, e.startTime, e.endTime)
	e.curWindowStart = e.startTime
	vals, err := e.getNextBatch()
	if err != nil {
		return e.eventChannel, err.Addf("Couldn't start feeing. Couldn't get initial batch.")
	}

	//otherwise we start our feeder.
	go e.run(vals)

	return e.eventChannel, nil
}

func (e *elkFeeder) run(events []interface{}) {

	defer func() {
		close(e.eventChannel)
	}()

	var err *nerr.E
	log.L.Infof("Starting feeing caterpillar %v. Initial round size %v", e.config.ID, len(events))

	for {
		for i := range events {
			e.eventChannel <- events[i]
			e.eventssent++
		}
		if e.eventssent >= e.eventcount {
			log.L.Infof("Feeding of caterpillar %v done. Closing the feeder.", e.config.ID)
			return
		}
		log.L.Debugf("Finished that round of feed. Getting more. Finished feeding %v/%v events", e.eventssent, e.eventcount)
		//get the next batch
		events, err = e.getNextBatch()
		if err != nil {
			log.L.Errorf("Couldn't continue feeding of caterpillar %v: %v", e.config.ID, err.Error())
			log.L.Debugf("detailed error info: %v, %s", err.Type, err.Stack)
			return
		}
		log.L.Debugf("Next feed batch size for %v is  %v events", e.config.ID, len(events))
	}
}

func (e *elkFeeder) executeQuery(q queries.ELKQueryTemplate) (queries.QueryResponse, *nerr.E) {

	b, er := json.Marshal(q)
	if er != nil {
		return queries.QueryResponse{}, nerr.Translate(er).Addf("Couldn't execute query.")
	}

	resp, err := elk.MakeELKRequest("POST", fmt.Sprintf("/%v/_search", e.config.Index), b)
	if err != nil {
		return queries.QueryResponse{}, err.Addf("COuldn't get count of documents for caterpillar %v", e.config.ID)
	}
	var toReturn queries.QueryResponse

	er = json.Unmarshal(resp, &toReturn)
	if er != nil {
		return queries.QueryResponse{}, nerr.Translate(er).Addf("Couldn't execute query.")
	}

	return toReturn, nil
}

func (e *elkFeeder) getNextBatch() ([]interface{}, *nerr.E) {

	query, err := e.getNextQuery()
	if err != nil {
		return nil, err.Addf("Couldn't get next batch of events.")
	}

	resp, err := e.executeQuery(query)
	if err != nil {
		return nil, err.Addf("Couldn't get next batch of events.")
	}

	events := []interface{}{}

	//check to make sure that the total == number of hits
	if resp.Hits.Total > len(resp.Hits.Hits) {
		lastEventTime, er := reflections.GetField(resp.Hits.Hits[len(resp.Hits.Hits)-1].Source, e.config.TimeField)
		if er != nil {
			//couldn't get last event time. Go into dumb splitting.
			return e.splitQuery(query)
		}

		//get the last event in the deal, set the curWindowStart to it.
		log.L.Infof("Query for caterpillar %v for period ending %v and with length %v returned more than %v events. Setting window start to %v", e.config.ID, e.curWindowStart, e.windowLength, MaxSize, lastEventTime)

		lastEventTimeString, ok := lastEventTime.(string)
		if !ok {
			//unkown last event time format. Go into dumb splitting.
			return e.splitQuery(query)

		}

		e.curWindowStart, er = time.Parse(time.RFC3339, lastEventTimeString)
		if er != nil {
			//Unknown last event time format. Go into dumb splitting.
			return e.splitQuery(query)

		}
	}
	for i := range resp.Hits.Hits {
		events = append(events, resp.Hits.Hits[i].Source)
	}

	return events, nil
}

func (e *elkFeeder) splitQuery(q queries.ELKQueryTemplate) ([]interface{}, *nerr.E) {
	//take our query, assert that the last filter there is TimeRangeFilter that was added by getNextQuery
	curTimeFilter, ok := q.Query.Bool.Filter[len(q.Query.Bool.Filter)-1].(queries.TimeRangeFilter)
	if !ok {
		return []interface{}{}, nerr.Create("Invalid filter on the query.", "invalid-query")
	}

	dateRange := curTimeFilter.Range[e.config.TimeField]
	newInterval := dateRange.EndTime.Sub(dateRange.StartTime) / 2
	newEnd := dateRange.StartTime.Add(newInterval)

	query, err := buildQuery(e.baseQuery, dateRange.StartTime, newEnd, e.config.TimeField)
	if err != nil {
		return []interface{}{}, err.Addf("Couldn't split query for caterpillar %v", e.config.ID)
	}

	resp, err := e.executeQuery(query)
	if err != nil {
		return []interface{}{}, err.Addf("Couldn't split query for caterpillar %v", e.config.ID)
	}

	if resp.Hits.Total > len(resp.Hits.Hits) {
		//we gotta split again
		return e.splitQuery(query)
	}

	//we can reset the curWindowStart as the newEnd, and return all the events
	e.curWindowStart = newEnd

	events := []interface{}{}
	for i := range resp.Hits.Hits {
		events = append(events, resp.Hits.Hits[i].Source)
	}

	return events, nil
}

func buildQuery(base queries.ELKQueryTemplate, StartTime, EndTime time.Time, timefield string) (queries.ELKQueryTemplate, *nerr.E) {

	//check to see if we're out of window.
	if StartTime.After(EndTime) || StartTime.Equal(EndTime) {
		return base, nerr.Create("out of time window.", "out-of-window")
	}

	base.Query.Bool.Filter = append(base.Query.Bool.Filter, queries.TimeRangeFilter{
		Range: map[string]queries.DateRange{
			timefield: queries.DateRange{
				StartTime: StartTime,
				EndTime:   EndTime,
			},
		},
	})

	base.From = 0
	base.Size = MaxSize

	base.Sort = []map[string]string{
		map[string]string{
			timefield: "asc",
		},
	}

	return base, nil
}

func (e *elkFeeder) getCountQuery() (queries.ELKQueryTemplate, *nerr.E) {

	base := e.baseQuery
	//check to see if we're out of window.
	if e.startTime.After(e.endTime) || e.startTime.Equal(e.endTime) {
		return base, nerr.Create("out of time window.", "out-of-window")
	}

	base.Query.Bool.Filter = append(base.Query.Bool.Filter, queries.TimeRangeFilter{
		Range: map[string]queries.DateRange{
			e.config.TimeField: queries.DateRange{
				StartTime: e.startTime,
				EndTime:   e.endTime,
			},
		},
	})

	return base, nil
}

//getNextQuery is not idempotent. It will increment current window start to be the end of the query just sent.
func (e *elkFeeder) getNextQuery() (queries.ELKQueryTemplate, *nerr.E) {
	curQuery := e.baseQuery

	//check to see if we're out of window.
	if e.curWindowStart.After(e.endTime) || e.curWindowStart.Equal(e.endTime) {
		return curQuery, nerr.Create("out of time window.", "out-of-window")
	}

	windowEnd := e.curWindowStart.Add(e.windowLength)
	if windowEnd.After(e.endTime) {
		windowEnd = e.endTime
	}

	curQuery, err := buildQuery(curQuery, e.curWindowStart, windowEnd, e.config.TimeField)
	if err != nil {
		return curQuery, err.Addf("Coulnd't get next query")
	}

	e.curWindowStart = windowEnd

	return curQuery, nil
}

func calculateBatches(count int, start, end time.Time) time.Duration {

	if count <= MaxSize {
		return end.Sub(start)
	}

	batches := ((count / MaxSize) + 1) * 2

	dur := end.Sub(start) / time.Duration(batches)
	log.L.Debugf("dur: %v", dur)

	return dur
}
