package hatchery

import (
	"testing"
	"time"

	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/hatchery/feeder"
	"github.com/byuoitav/caterpillar/hatchery/store"
	"github.com/byuoitav/caterpillar/nydus"
)

func TestConfig(t *testing.T) {
	c, err := config.GetConfig()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	t.Log(c)
}

func TestStore(t *testing.T) {
	c, err := config.GetConfig()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	info, err := store.GetInfo(c.Caterpillars[0].ID)
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	t.Log(info)

	//put something in
	info.LastEventTime = time.Now().AddDate(0, 0, -1)
	err = store.PutInfo(c.Caterpillars[0].ID, info)
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}

	info, err = store.GetInfo(c.Caterpillars[0].ID)
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	t.Log(info)
}

func TestSpawnQueen(t *testing.T) {
	c, err := config.GetConfig()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	n, err := nydus.GetNetwork()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	q := SpawnQueen(c.Caterpillars[0], n.GetChannel())
	t.Log(q)
}

func TestFeederCount(t *testing.T) {
	c, err := config.GetConfig()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	info, err := store.GetInfo(c.Caterpillars[0].ID)
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	f, err := feeder.GetFeeder(c.Caterpillars[0], info.LastEventTime)
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	count, err := f.GetCount()
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	t.Logf("count: %v", count)

}

func TestFeederFeeding(t *testing.T) {
	c, err := config.GetConfig()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	info, err := store.GetInfo(c.Caterpillars[0].ID)
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	f, err := feeder.GetFeeder(c.Caterpillars[0], info.LastEventTime)
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	count, err := f.GetCount()
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	t.Logf("count: %v", count)

	t.Logf("Starting feeding..")
	feedchan, err := f.StartFeeding(100)
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	processed := 0

	for processed < count {
		<-feedchan
		processed++
	}
}

func TestFeederBatchFeeding(t *testing.T) {
	c, err := config.GetConfig()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	info, err := store.GetInfo(c.Caterpillars[0].ID)
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	f, err := feeder.GetFeeder(c.Caterpillars[0], info.LastEventTime)
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	count, err := f.GetCount()
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	t.Logf("count: %v", count)

	t.Logf("Starting feeding..")
	feedchan, err := f.StartFeeding(100)
	if err != nil {
		t.Log(err.Type)
		t.Error(err.Error())
		t.FailNow()
	}
	processed := 0

	for processed < count {
		<-feedchan
		processed++
	}
}

func TestCaterpillarRun(t *testing.T) {
	feeder.MaxSize = 8000
	c, err := config.GetConfig()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	n, err := nydus.GetNetwork()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	q := SpawnQueen(c.Caterpillars[0], n.GetChannel())
	q.Run()
}
