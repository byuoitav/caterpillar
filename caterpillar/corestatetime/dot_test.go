package corestatetime

import (
	"testing"

	ci "github.com/byuoitav/caterpillar/caterpillar/catinter"
	"github.com/byuoitav/common/log"
)

func TestBuildGraph(t *testing.T) {

	mc := &MachineCaterpillar{
		rectype: "metrics",
		devices: map[string]ci.DeviceInfo{},
		rooms:   map[string]ci.RoomInfo{},
	}
	log.SetLevel("debug")

	m, err := mc.buildStateMachine()
	if err != nil {
		log.L.Fatalf("Error: %v", err.Error())
	}

	m.PrintSimpleDotFile()
}
