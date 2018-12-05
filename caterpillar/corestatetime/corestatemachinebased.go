package corestatetime

import (
	"encoding/gob"
	"fmt"
	"strings"
	"sync"
	"time"

	ci "github.com/byuoitav/caterpillar/caterpillar/catinter"
	sm "github.com/byuoitav/caterpillar/caterpillar/statemachine"
	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/nydus"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/common/v2/events"
)

//MachineCaterpillar .
type MachineCaterpillar struct {
	Machine *sm.Machine
	outChan chan nydus.BulkRecordEntry
	state   config.State

	rectype string
	devices map[string]ci.DeviceInfo
	rooms   map[string]ci.RoomInfo

	curState map[string]State
	index    string

	GobRegisterOnce sync.Once
}

//True for pointer puposes
var True = true

//False for pointer puposes
var False = false

var location *time.Location

func init() {
	var er error
	location, er = time.LoadLocation("America/Denver")
	if er != nil {
		log.L.Fatalf("Couldn't load timezone: %v", er.Error())
	}
}

//GetMachineCaterpillar .
func GetMachineCaterpillar() (ci.Caterpillar, *nerr.E) {

	toReturn := &MachineCaterpillar{
		rectype:  "metrics",
		devices:  map[string]ci.DeviceInfo{},
		rooms:    map[string]ci.RoomInfo{},
		curState: map[string]State{},
	}

	var err *nerr.E
	toReturn.devices, toReturn.rooms, err = GetDeviceAndRoomInfo()
	if err != nil {
		return toReturn, err.Addf("Couldn't initialize corestatetime caterpillar.")
	}

	return toReturn, nil
}

//Run .
func (c *MachineCaterpillar) Run(id string, recordCount int, state config.State, outChan chan nydus.BulkRecordEntry, cnfg config.Caterpillar, GetData func(int) (chan interface{}, *nerr.E)) (config.State, *nerr.E) {

	index, ok := cnfg.TypeConfig["output-index"]
	if !ok {
		return state, nerr.Create(fmt.Sprintf("Missing config item for Caterpillar type %v. Need output-index", cnfg.Type), "invalid-config")
	}

	c.index = index
	c.state = state
	c.outChan = outChan
	var err *nerr.E

	c.Machine, err = c.buildStateMachine()

	if err != nil {
		return state, err.Addf("Couldn't run machinecaterepillar")
	}

	inchan, err := GetData(1000)
	if err != nil {
		return state, err.Addf("Couldn't run machinecaterepillar")
	}
	for i := range inchan {
		if e, ok := i.(events.Event); ok {
			err = c.Machine.ProcessEvent(e)
			if err != nil {
				log.L.Errorf("Error procssing event: %v", err.Error())
			}
		} else {
			log.L.Warnf("Unkown type in channel %v", i)
		}
	}

	return config.State{}, nil
}

func (c *MachineCaterpillar) buildStateMachine() (*sm.Machine, *nerr.E) {

	Nodes := map[string]sm.Node{}
	//definitions of our state machine.
	Nodes["start"] = sm.Node{
		ID: "start",
		Transitions: []sm.Transition{
			sm.Transition{
				ID:           "initial-transition",
				TriggerKey:   "power",
				TriggerValue: "on",
				Destination:  "poweron",
			},
		},
	}

	Nodes["poweron"] = sm.Node{
		ID: "poweron",
		Transitions: []sm.Transition{
			sm.Transition{
				TriggerKey:  "input",
				Destination: "inputactive",
			},
			sm.Transition{
				TriggerKey:   "blanked",
				TriggerValue: "true",
				Destination:  "blank",
			},
			sm.Transition{
				TriggerKey:   "power",
				TriggerValue: "standby",
				Destination:  "powerstandby",
			},
		},
		Enter: PowerOnStore,
	}

	Nodes["inputactive"] = sm.Node{
		ID: "inputactive",
		Transitions: []sm.Transition{
			sm.Transition{
				TriggerKey:  "input",
				Destination: "inputactive",
			},
			sm.Transition{
				TriggerKey:   "blanked",
				TriggerValue: "true",
				Destination:  "blank",
			},
			sm.Transition{
				TriggerKey:   "power",
				TriggerValue: "standby",
				Actions: []func(map[string]interface{}, events.Event) ([]ci.MetricsRecord, *nerr.E){
					c.BuildUnblankedRecord,
				},
				Destination: "powerstandby",
			},
		},
		Exit:  c.BuildInputRecord,
		Enter: InputStore,
	}

	Nodes["blank"] = sm.Node{
		ID: "blank",
		Transitions: []sm.Transition{
			sm.Transition{
				TriggerKey:  "input",
				Destination: "blank",
			},
			sm.Transition{
				TriggerKey:   "blanked",
				TriggerValue: "false",
				Destination:  "inputactive",
			},
			sm.Transition{
				TriggerKey:   "power",
				TriggerValue: "standby",
				Destination:  "powerstandby",
			},
		},
		Enter: c.EnterBlank,
		Exit:  c.BuildBlankedRecord,
	}

	Nodes["powerstandby"] = sm.Node{
		ID: "powerstandby",
		Transitions: []sm.Transition{
			sm.Transition{
				TriggerKey:  "power",
				Destination: "poweron",
			},
		},
		Enter: c.StandbyEnter,
		Exit:  c.StandbyExit,
	}

	return sm.BuildStateMachine("deviceid", Nodes, "start", c.state, c)
}

//StandbyEnter .
func (c *MachineCaterpillar) StandbyEnter(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {
	//generate a 'time power on' event

	toReturn := ci.MetricsRecord{
		Power:      "on",
		RecordType: ci.Power,
	}

	startTime, ok := state["power-set"].(time.Time)
	if !ok {
		return []ci.MetricsRecord{}, nerr.Create("power-set not set to time.Time", "invalid-state")
	}
	state["power"] = "standby"
	state["power-set"] = e.Timestamp

	rec, err := c.AddMetaInfo(startTime, e, toReturn)
	return []ci.MetricsRecord{rec}, err
}

//StandbyExit .
func (c *MachineCaterpillar) StandbyExit(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {

	//generate a 'time power standby' event
	toReturn := ci.MetricsRecord{
		Power:      "standby",
		RecordType: ci.Power,
	}

	startTime, ok := state["power-set"].(time.Time)
	if !ok {
		return []ci.MetricsRecord{}, nerr.Create("power-set not set to time.Time", "invalid-state")
	}
	state["power"] = "on"
	state["power-set"] = e.Timestamp

	rec, err := c.AddMetaInfo(startTime, e, toReturn)
	return []ci.MetricsRecord{rec}, err
}

//BuildBlankedRecord .
func (c *MachineCaterpillar) BuildBlankedRecord(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {

	//generate the event
	toReturn := ci.MetricsRecord{
		Blanked:    &True,
		RecordType: ci.Blank,
	}

	startTime, ok := state["blank-set"].(time.Time)
	if !ok {
		return []ci.MetricsRecord{}, nerr.Create("blank-set not set to time.Time", "invalid-state")
	}

	state["blanked"] = false
	state["input-set"] = e.Timestamp
	state["blank-set"] = e.Timestamp

	rec, err := c.AddMetaInfo(startTime, e, toReturn)
	return []ci.MetricsRecord{rec}, err
}

//BuildUnblankedRecord .
func (c *MachineCaterpillar) BuildUnblankedRecord(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {

	//generate the event
	toReturn := ci.MetricsRecord{
		Blanked:    &False,
		RecordType: ci.Blank,
	}

	startTime, ok := state["blank-set"].(time.Time)
	if !ok {
		return []ci.MetricsRecord{}, nerr.Create("blank-set not set to time.Time", "invalid-state")
	}

	rec, err := c.AddMetaInfo(startTime, e, toReturn)
	return []ci.MetricsRecord{rec}, err
}

//BuildInputRecord .
func (c *MachineCaterpillar) BuildInputRecord(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {

	//generate the event
	toReturn := ci.MetricsRecord{
		RecordType: ci.Input,
	}
	var err *nerr.E

	//check to see if it's an input change
	var curInputStr string
	curInput, ok := state["input"]
	if ok {
		curInputStr, ok = curInput.(string)
		if ok {
			if curInputStr == e.Value {
				return []ci.MetricsRecord{}, nil
			}
			toReturn.Input = curInputStr
			if startTime, ok := state["input-set"].(time.Time); ok {
				toReturn, err = c.AddMetaInfo(startTime, e, toReturn)
				if err != nil {
					return []ci.MetricsRecord{}, err
				}
				return []ci.MetricsRecord{toReturn}, nil
			}
			return []ci.MetricsRecord{}, nerr.Create(fmt.Sprintf("input-store not set to time.Time, value %v", state["input-store"]), "invalid-state")
		}
		return []ci.MetricsRecord{}, nerr.Create(fmt.Sprintf("Invlaid type stored in input: %v", curInput), "invalid-state")

		//unkown value stored
	}
	return []ci.MetricsRecord{}, nerr.Create("Cannot create input record with input state not set.", "invalid-state")
}

//EnterBlank .
func (c *MachineCaterpillar) EnterBlank(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {
	if e.Key != "blanked" {
		return []ci.MetricsRecord{}, nil
	}

	//generate the 'time unblanked' event
	toReturn := ci.MetricsRecord{
		Blanked:    &False,
		RecordType: ci.Blank,
	}

	startTime, ok := state["blank-set"].(time.Time)
	if !ok {
		return []ci.MetricsRecord{}, nerr.Create("blank-set not set to time.Time", "invalid-state")
	}

	//Store the new power state
	state["blank-set"] = e.Timestamp
	state["blanked"] = true

	rec, err := c.AddMetaInfo(startTime, e, toReturn)
	return []ci.MetricsRecord{rec}, err
}

//UnBlankStore .
func UnBlankStore(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {
	state["blank-set"] = e.Timestamp
	state["blanked"] = false
	return []ci.MetricsRecord{}, nil
}

//InputStore .
func InputStore(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {
	state["input-set"] = e.Timestamp
	state["input"] = e.Value
	return []ci.MetricsRecord{}, nil
}

//PowerOnStore .
func PowerOnStore(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {
	state["power-set"] = e.Timestamp
	state["blank-set"] = e.Timestamp
	state["input-set"] = e.Timestamp
	state["power"] = "on"
	state["blanked"] = false
	return []ci.MetricsRecord{}, nil
}

//AddMetaInfo .
func (c *MachineCaterpillar) AddMetaInfo(startTime time.Time, e events.Event, r ci.MetricsRecord) (ci.MetricsRecord, *nerr.E) {

	r.Device = ci.DeviceInfo{ID: e.TargetDevice.DeviceID}
	r.Room = ci.RoomInfo{ID: e.TargetDevice.RoomID}

	if dev, ok := c.devices[r.Device.ID]; ok {
		r.Device = dev
		//check if it's an input deal
		if r.RecordType == ci.Input {
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

//WrapAndSend .
func (c *MachineCaterpillar) WrapAndSend(r ci.MetricsRecord) {

	printRecord(r)

	c.outChan <- nydus.BulkRecordEntry{
		Header: nydus.BulkRecordHeader{
			Index: nydus.HeaderIndex{
				Index: c.index,
				Type:  c.rectype,
			},
		},
		Body: r,
	}
}

func printRecord(r ci.MetricsRecord) {

	switch r.RecordType {
	case "input":
		log.L.Debugf("Generating %v %v Time %v Starting %v Ending %v", r.RecordType, r.Input, r.ElapsedInSeconds, r.StartTime.In(location).Format("15:04:05 01-02"), r.EndTime.In(location).Format("15:04:05 01-02"))
	case "blank":
		log.L.Debugf("Generating %v %v Time %v Starting %v Ending %v", r.RecordType, *r.Blanked, r.ElapsedInSeconds, r.StartTime.In(location).Format("15:04:05 01-02"), r.EndTime.In(location).Format("15:04:05 01-02"))
	case "power":
		log.L.Debugf("Generating %v %v Time %v Starting %v Ending %v", r.RecordType, r.Power, r.ElapsedInSeconds, r.StartTime.In(location).Format("15:04:05 01-02"), r.EndTime.In(location).Format("15:04:05 01-02"))
	}

}
