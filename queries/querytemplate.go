package queries

import (
	"encoding/json"
	"io/ioutil"
	"time"

	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/common/v2/events"
)

//ELKQueryTemplate shows the template that we use for elk queries. The queries specified for a specific caterpillar wil be unmarshalled into this structure.
//Becasue we may have to batch documents from ELK the query should NOT include a date/time range query. This will be added by the feeder as it retreives data.
type ELKQueryTemplate struct {
	Query  ElkQueryDSL         `json:"query,omitempty"`
	Aggs   interface{}         `json:"aggs,omitempty"`
	From   int                 `json:"from,omitempty"`
	Size   int                 `json:"size,omitempty"`
	Sort   []map[string]string `json:"sort,omitempty"`
	Source interface{}         `json:"_source,omitempty"`
}

//ElkQueryDSL .
type ElkQueryDSL struct {
	Bool ElkBoolQueryDSL `json:"bool,omitempty"`
}

//ElkBoolQueryDSL .
type ElkBoolQueryDSL struct {
	Must               interface{}   `json:"must,omitempty"`
	Should             interface{}   `json:"should,omitempty"`
	MustNot            interface{}   `json:"must_not,omitempty"`
	Filter             []interface{} `json:"filter,omitempty"`
	MinimumShouldMatch int           `json:"minimum_should_match,omitempty"`
}

//TimeRangeFilter .
type TimeRangeFilter struct {
	Range map[string]DateRange `json:"range,omitempty"`
}

//DateRange .
type DateRange struct {
	StartTime time.Time `json:"gte"` //no omitempty on purpose
	EndTime   time.Time `json:"lte,omimempty"`
}

//CountResponse .
type CountResponse struct {
	Count  int            `json:"count"`
	Shards map[string]int `json:"_shards"`
}

//QueryResponse is a response from ELK with hits included
type QueryResponse struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total int `json:"total"`
		Hits  []struct {
			Index  string       `json:"_index"`
			Type   string       `json:"_type"`
			ID     string       `json:"_id"`
			Source events.Event `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

//GetQueryTemplateFromFile .
func GetQueryTemplateFromFile(file string) (ELKQueryTemplate, *nerr.E) {

	b, err := ioutil.ReadFile(file)
	if err != nil {
		return ELKQueryTemplate{}, nerr.Translate(err).Addf("couldn't get query template from file. Couldn't read file %v.", file)
	}

	return GetQueryTemplateFromString(b)
}

// GetQueryTemplateFromString .
func GetQueryTemplateFromString(query []byte) (ELKQueryTemplate, *nerr.E) {
	toReturn := ELKQueryTemplate{}

	//we unmarshal
	err := json.Unmarshal(query, &toReturn)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("Couldn't get Query from string.")
	}

	return toReturn, nil
}
