package nydus

import (
	"time"

	"github.com/byuoitav/common/nerr"
)

const bufferSize = 10000

//A Network (Nydus Network) handles the shipping of generated records back up to the elk cluster.
type Network struct {
	inChannel chan BulkRecordEntry
	curBuffer []BulkRecordEntry
	timer     *time.Timer
}

//BulkRecordEntry corresponds to a single record to be pushed back up to the ELK cluter.
type BulkRecordEntry struct {
	Header BulkRecordHeader
	Body   interface{}
}

//BulkRecordHeader .
type BulkRecordHeader struct {
	Index HeaderIndex `json:"index"`
}

//HeaderIndex .
type HeaderIndex struct {
	Index string `json:"_indedx"`
	Type  string `json:"_type"`
	ID    string `json:"_id,omitempty"`
}

//BulkUpdateResponse .
type BulkUpdateResponse struct {
	Errors bool `json:"errors"`
}

//GetNetwork .
func GetNetwork() (*Network, *nerr.E) {

	toReturn := &Network{
		inChannel: make(chan BulkRecordEntry, bufferSize),
	}

	//we'd start the network running.
	go toReturn.run()

	return toReturn, nil
}

//GetChannel .
func (n *Network) GetChannel() chan BulkRecordEntry {
	return n.inChannel
}

//run starts the nydus network
func (n *Network) run() {

	started := false

	n.curBuffer = []BulkRecordEntry{}
	n.timer = time.NewTimer(1 * time.Second)
	n.timer.Stop()

	for {
		select {
		case <-n.timer.C:
			go SpawnWorm(n.curBuffer)
			n.curBuffer = []BulkRecordEntry{}
			started = false
			continue

		case record := <-n.inChannel:
			n.curBuffer = append(n.curBuffer, record)

			if len(n.curBuffer) >= BatchSize {
				go SpawnWorm(n.curBuffer)
				n.curBuffer = []BulkRecordEntry{}
				started = false
				n.timer.Stop()
				continue
			}

			if !started {
				started = true
				n.timer.Reset(5 * time.Second)
				continue
			}
		}
	}
}
