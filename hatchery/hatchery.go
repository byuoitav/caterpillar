package hatchery

import (
	"time"

	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/nydus"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
	"github.com/robfig/cron"
)

//Hatchery .
type Hatchery struct {
	Cron         *cron.Cron
	Queens       []*Queen
	NydusNetwork *nydus.Network
}

//CronEntry .
type CronEntry struct {
	Next time.Time `json:"next"`
	Prev time.Time `json:"prev"`
}

//HatchStatus .
type HatchStatus struct {
	Queens  []QueenStatus       `json:"caterpillar-status"`
	Entries []CronEntry         `json:"cron-staus"`
	Nydus   nydus.NetworkStatus `json:"nydus-status"`
}

//InitializeHatchery .
func InitializeHatchery() (*Hatchery, *nerr.E) {
	toReturn := &Hatchery{}

	c, err := config.GetConfig()
	if err != nil {
		return toReturn, err.Addf("Couldn't initialize hatchery...")
	}
	toReturn.Cron = cron.New()

	toReturn.NydusNetwork, err = nydus.GetNetwork()
	if err != nil {
		return toReturn, err.Addf("Couldn't initialize hatchery...")
	}

	for _, i := range c.Caterpillars {
		q := SpawnQueen(i, toReturn.NydusNetwork.GetChannel())

		toReturn.Queens = append(toReturn.Queens, q)
		toReturn.Cron.AddFunc(i.Interval, q.Run)
	}

	toReturn.Cron.Start()

	return toReturn, nil
}

//GetStatus .
func (h *Hatchery) GetStatus() HatchStatus {
	log.L.Debugf("Getting hatch status")
	toReturn := HatchStatus{
		Nydus: h.NydusNetwork.GetStatus(),
	}
	log.L.Debugf("Getting queen status")

	for _, v := range h.Cron.Entries() {
		toReturn.Entries = append(toReturn.Entries, CronEntry{
			Next: v.Next,
			Prev: v.Prev,
		})
	}

	for i := range h.Queens {
		toReturn.Queens = append(toReturn.Queens, h.Queens[i].GetStatus())
	}
	log.L.Debugf("got queen status")

	return toReturn
}
