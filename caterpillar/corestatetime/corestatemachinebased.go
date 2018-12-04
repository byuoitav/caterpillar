package j

import (
	"encoding/gob"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/byuoitav/caterpillar/caterpillar/catinter"
	sm "github.com/byuoitav/caterpillar/caterpillar/statemachine"
	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/nydus"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/common/v2/events"
)

//MachineCaterpillar .
type MachineCaterpillar struct {
	Machine *statemachine.Machine
	outChan chan nydus.BullkRecordEntry

	GobRegisterOnce sync.Once
}

//Run .
func (c *MachineCaterpillar) Run(id string, recordCount int, state config.State, outChan chan nydus.BulkRecordEntry, cnfg config.Caterpillar, GetData func(int) (chan interface{}, *nerr.E)) (config.State, *nerr.E) {

	Nodes := map[string]sm.Node{}
	//definitions of our state machine.
	Nodes["start"] = sm.Node{
		ID: "start",
		sm.Transitions: []sm.Transition{
			sm.Transition{
				ID:           "initial-transition",
				TriggerKey:   "power",
				TriggerValue: "on",
				Store:        PowerOnStore,
				Destination:  "poweron",
			},
		},
	}

	Nodes["poweron"] = sm.Node{
		ID: "poweron",
		sm.Transitions: []sm.Transition{
			sm.Transition{
				TriggerKey:  "input",
				Store:       InputStore,
				Destination: "inputactive",
			},
			sm.Transition{
				TriggerKey:   "blanked",
				TriggerValue: "true",
				Store:        EnterBlank,
				Destination:  "blank",
			},
			sm.Transition{
				TriggerKey:   "power",
				TriggerValue: "standby",
				Destination:  "powerstandby",
			},
		},
	}

	Nodes["inputactive"] = sm.Node{
		ID: "inputactive",
		sm.Transitions: []sm.Transition{
			sm.Transition{
				TriggerKey:  "input",
				Destination: "inputactive",
			},
			sm.Transition{
				TriggerKey:  "blanked",
				TriggerKey:  "true",
				Store:       EnterBlank,
				Destination: "blank",
			},
			sm.Transition{
				TriggerKey:  "power",
				TriggerKey:  "standby",
				Before:      BuildUnblankedRecord,
				Destination: "powerstandby",
			},
		},
		Exit: BuildInputRecord,
	}

	Nodes["blank"] = sm.Node{
		ID: "blank",
		sm.Transitions: []sm.Transition{
			sm.Transition{
				TriggerKey:  "blank",
				Destination: "blank",
				Store:       InputStore,
			},
			sm.Transition{
				TriggerKey:  "unblank",
				Destination: "inputactive",
				Before:      BuildBlankedRecord,
			},
			sm.Transition{
				TriggerKey:  "power",
				TriggerKey:  "standby",
				Destination: "powerstandby",
				Before:      BuildBlankedRecord,
			},
		},
	}

	Nodes["powerstandby"] = sm.Node{
		ID: "powerstandby",
		sm.Transitions: []sm.Transition{
			sm.Transition{
				TriggerKey:  "power",
				Destination: "on",
			},
		},
		Enter: StandbyEnter,
		Exit:  StandbyExit,
	}

	return config.State{}, nil
}

//StandbyEnter .
func (c *MachineCaterpillar) StandbyEnter(state map[string]interface{}, e events.Event) (catinter.MetricsRecord, *nerr.E) {
	//generate a 'time power on' event

	toReturn := catinter.MetricsRecord{Power: "on"}

	startTime, ok := state["power-set"].(time.Time)
	if !ok {
		return toReturn, nerr.Create("power-set not set to time.Time", "invalid-state")
	}
	state["power"] = "standby"
	state["power-set"] = e.Timestamp

	return c.AddMetaInfo(startTime, e, toReturn)
}

//StandbyExit .
func (c *MachineCaterpillar) StandbyExit(state map[string]interface{}, e events.Event) (catinter.MetricsRecord, *nerr.E) {

	//generate a 'time power standby' event
	toReturn := catinter.MetricsRecord{Power: "standby"}

	startTime, ok := state["power-set"].(time.Time)
	if !ok {
		return toReturn, nerr.Create("power-set not set to time.Time", "invalid-state")
	}
	state["power"] = "on"
	state["power-set"] = e.Timestamp

	return c.AddMetaInfo(startTime, e, toReturn)
}

//BuildBlankedRecord .
func (c *MachineCaterpillar) BuildBlankedRecord(state map[string]interface{}, e events.Event) (catinter.MetricsRecord, *nerr.E) {

	//generate the event
	toReturn := catinter.MetricsRecord{Blanked: true}

	startTime, ok := state["blank-set"].(time.Time)
	if !ok {
		return toReturn, nerr.Create("blank-set not set to time.Time", "invalid-state")
	}

	state["blanked"] = false
	state["input-set"] = e.Timestamp
	state["blank-set"] = e.Timestamp

	return c.AddMetaInfo(startTime, e, toReturn)
}

//BuildUnblankedRecord .
func (c *MachineCaterpillar) BuildUnblankedRecord(state map[string]interface{}, e events.Event) (catinter.MetricsRecord, *nerr.E) {

	//generate the event
	toReturn := catinter.MetricsRecord{Blanked: false}

	startTime, ok := state["blank-set"].(time.Time)
	if !ok {
		return toReturn, nerr.Create("blank-set not set to time.Time", "invalid-state")
	}

	return c.AddMetaInfo(startTime, e, toReturn)
}

//BuildInputRecord .
func (c *MachineCaterpillar) BuildInputRecord(state map[string]interface{}, e events.Event) (catinter.MetricsRecord, *nerr.E) {

	//generate the event
	toReturn := catinter.MetricsRecord{}
	var err *nerr.E

	//check to see if it's an input change
	var curInputStr string
	curInput, ok := state["input"]
	if ok {
		curInputStr, ok = curInput.(string)
		if ok {
			if curInputStr == e.Value {
				return catinter.MetricsRecord{}, nil
			}

			toReturn.Input = curInputStr
			startTime, ok := state["input-store"].(time.Time)

			toReturn, err = c.AddMetaInfo(starTime, e, &toReturn)
			if err != nil {
				return toReturn, err
			}
		} else {
			//unkown value stored
			return toReturn, nerr.Create(fmt.Sprintf("Invlaid type stored in input: %v", curInput), "invalid-state")
		}
		if !ok {
			return toReturn, nerr.Create("Power-store not set to time.Time", "invalid-state")
		}
	}

	return toReturn, nil
}

//EnterStandby  .
func (c *MachineCaterpillar) EnterStandby(state map[string]interface{}, e events.Event) (catinter.MetricsRecord, *nerr.E) {

	//generate the event
	toReturn := catinter.MetricsRecord{Power: "on"}

	startTime, ok := state["power-set"].(time.Time)
	if !ok {
		return toReturn, nerr.Create("Power-set not set to time.Time", "invalid-state")
	}

	//Store the new power state
	state["power-set"] = e.Timstamp
	state["power"] = "standby"

	return c.AddMetaInfo(starTime, e, toReturn)
}

//EnterBlank .
func EnterBlank(state map[string]interface{}, e events.Event) (catinter.MettricsRecord, *nerr.E) {

	//generate the 'time unblanked' event
	toReturn := catinter.MetricsRecord{Blanked: false}

	startTime, ok := state["blank-set"].(time.Time)
	if !ok {
		return toReturn, nerr.Create("blank-set not set to time.Time", "invalid-state")
	}

	//Store the new power state
	state["blank-set"] = e.Timestamp
	state["blanked"] = true

	return c.AddMetaInfo(starTime, e, toReturn)
}

//UnBlankStore .
func UnBlankStore(state map[string]interface{}, e events.Event) {
	state["blank-set"] = e.Timestamp
	state["blanked"] = false
}

//InputStore .
func InputStore(state map[string]interface{}, e events.Event) {
	state["input-set"] = e.Timestamp
	state["input"] = e.Value
}

//PowerOnStore .
func PowerOnStore(state map[string]interface{}, e events.Event) {
	state["power-set"] = e.Timestamp
	state["blank-set"] = e.Timestamp
	state["input-set"] = e.Timestamp
	state["power"] = "on"
	state["blanked"] = false
}

//AddMetaInfo .
func (c *MachineCaterpillar) AddMetaInfo(startTime time.Time, e events.Event, r catinter.MetricsRecord) (catinter.MetricsRecord, *nerr.E) {

	r.Device = catinter.DeviceInfo{ID: e.TargetDevice.DeviceID}
	r.Room = catinter.RoomInfo{ID: e.TargetDevice.RoomID}

	if dev, ok := c.devices[r.Device.ID]; ok {
		r.Device = dev
		//check if it's an input deal
		if r.RecordType == catinter.Input {
			if indev, ok := c.devices[r.Input]; ok {
				r.InputType = indev.DeviceType
			} else {

				r.InputType = strings.TrimRight(r.Input, "1234567890")
			}
		}

	} else {
		err := nerr.Create(fmt.Sprintf("unkown device %v", r.Device.ID), "invalid-device")
		log.L.Errorf("%v", err.Error())
		return r, err
	}

	if room, ok := c.rooms[r.Room.ID]; ok {
		r.Room = room
	} else {
		err := nerr.Create(fmt.Sprintf("unkown room %v", r.Device.ID), "invalid-room")
		log.L.Errorf("%v", err.Error())
		return r, err
	}

	AddTimeFields(startTime, e.Timestamp, &r)

	return r, nil
}

//RegisterGobStructs .
func (c *MachineCaterpillar) RegisterGobStructs() {
	c.GobRegisterOnce.Do(func() {
		gob.Register(sm.MachineState{})
	})
}
