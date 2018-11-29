package hatchery

import (
	"testing"
	"time"

	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/hatchery/feeder"
	"github.com/byuoitav/caterpillar/hatchery/store"
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

	q := SpawnQueen(c.Caterpillars[0])
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

}
