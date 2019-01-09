package nydus

import (
	"encoding/json"

	"github.com/byuoitav/common/log"
	"github.com/byuoitav/state-parser/elk"
)

//BatchSize is the number of records to send per bulk update.
const BatchSize = 2500

//SpawnWorm is meant to be spanwed and forgotten, it handles the dispatching of the entries as a bulk update to ELK.
func SpawnWorm(entries []BulkRecordEntry) {
	log.L.Infof("Spawning worm. Sending %v records", len(entries))

	body := []byte{}
	newline := []byte("\n")

	for i := range entries {
		hb, err := json.Marshal(entries[i].Header)
		if err != nil {
			log.L.Errorf("Couldn't marshal header %v", entries[i].Header)
			continue
		}

		bb, err := json.Marshal(entries[i].Body)
		if err != nil {
			log.L.Errorf("Couldn't marshal body %v", entries[i].Body)
			continue
		}

		body = append(body, hb...)
		body = append(body, newline...)
		body = append(body, bb...)
		body = append(body, newline...)
	}
	//	log.L.Debugf("Sending body: %s", body)

	//we send body
	resp, er := elk.MakeELKRequest("POST", "/_bulk", body)
	if er != nil {
		log.L.Errorf("Worm failed to send update: %v", er.Error())
		return
	}

	var eresp BulkUpdateResponse
	err := json.Unmarshal(resp, &eresp)
	if err != nil {
		log.L.Errorf("Uknown body receieved: %s, %v", resp, err.Error())
		return
	}
	if eresp.Errors {
		log.L.Errorf("Errors Received from Worm Bulk Request... %s", resp)
	}

	return
}
