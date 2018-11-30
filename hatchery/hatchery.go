package hatchery

import (
	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/nydus"
	"github.com/byuoitav/common/nerr"
	"github.com/robfig/cron"
)

//Hatchery .
type Hatchery struct {
	Cron         *cron.Cron
	Queens       []Queen
	NydusNetwork *nydus.Network
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
		toReturn.Cron.AddJob(i.Interval, q)
	}

	toReturn.Cron.Start()

	return toReturn, nil
}
