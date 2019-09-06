package elkquery

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/common/v2/events"
	"github.com/byuoitav/shipwright/elk"
)

//QueryTemplate shows the template that we use for elk queries. The queries specified for a specific caterpillar wil be unmarshalled into this structure.
//Becasue we may have to batch documents from ELK the query should NOT include a date/time range query. This will be added by the feeder as it retreives data.
type QueryTemplate struct {
	Query  QueryDSL            `json:"query,omitempty"`
	Aggs   interface{}         `json:"aggs,omitempty"`
	From   int                 `json:"from,omitempty"`
	Size   int                 `json:"size,omitempty"`
	Sort   []map[string]string `json:"sort,omitempty"`
	Source interface{}         `json:"_source,omitempty"`
}

//QueryDSL .
type QueryDSL struct {
	Bool BoolQueryDSL `json:"bool,omitempty"`
}

//BoolQueryDSL .
type BoolQueryDSL struct {
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
	StartTime time.Time `json:"gt"` //no omitempty on purpose
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
	Aggregations interface{} `json:"aggregations"`
}

//GetQueryTemplateFromFile .
func GetQueryTemplateFromFile(file string) (QueryTemplate, *nerr.E) {

	b, err := ioutil.ReadFile(file)
	if err != nil {
		return QueryTemplate{}, nerr.Translate(err).Addf("couldn't get query template from file. Couldn't read file %v.", file)
	}

	return GetQueryTemplateFromString(b)
}

// GetQueryTemplateFromString .
func GetQueryTemplateFromString(query []byte) (QueryTemplate, *nerr.E) {
	toReturn := QueryTemplate{}

	//we unmarshal
	err := json.Unmarshal(query, &toReturn)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("Couldn't get Query from string.")
	}

	return toReturn, nil
}

//ExecuteElkQuery ...
func ExecuteElkQuery(indexName string, q QueryTemplate) (QueryResponse, *nerr.E) {

	b, er := json.Marshal(q)
	if er != nil {
		return QueryResponse{}, nerr.Translate(er).Addf("Couldn't execute query.")
	}

	resp, err := elk.MakeELKRequest("POST", fmt.Sprintf("/%v/_search", indexName), b)
	if err != nil {
		return QueryResponse{}, err.Addf("COuldn't get count of documents for index name %v", indexName)
	}
	var toReturn QueryResponse

	er = json.Unmarshal(resp, &toReturn)
	if er != nil {
		return QueryResponse{}, nerr.Translate(er).Addf("Couldn't execute query.")
	}

	return toReturn, nil
}
