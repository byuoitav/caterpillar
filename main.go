package main

import (
	"time"

	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/hatchery"
	"github.com/byuoitav/caterpillar/hatchery/feeder"
	"github.com/byuoitav/caterpillar/nydus"
	"github.com/byuoitav/common/log"
)

func main() {
	log.SetLevel("debug")
	feeder.MaxSize = 8000
	c, err := config.GetConfig()
	if err != nil {
		log.L.Fatal(err.Error())
	}

	n, err := nydus.GetNetwork()
	if err != nil {
		log.L.Fatal(err.Error())
	}

	q := hatchery.SpawnQueen(c.Caterpillars[0], n.GetChannel())
	q.Run()
	time.Sleep(7 * time.Second)
}
