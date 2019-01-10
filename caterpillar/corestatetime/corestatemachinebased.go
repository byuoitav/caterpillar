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
	"github.com/byuoitav/common/db"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/common/v2/events"
	"github.com/byuoitav/wso2services/classschedules/registar"
)

//MachineCaterpillar .
type MachineCaterpillar struct {
	Machine *sm.Machine
	outChan chan nydus.BulkRecordEntry
	state   config.State

	rectype string
	devices map[string]ci.DeviceInfo
	rooms   map[string]ci.RoomInfo

	index string

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
		rectype: "metrics",
		devices: map[string]ci.DeviceInfo{},
		rooms:   map[string]ci.RoomInfo{},
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

	count := 0
	lastTime := state.LastEventTime
	for i := range inchan {
		if e, ok := i.(events.Event); ok {
			count++
			log.L.Debugf("Processing event %v", count)
			err = c.Machine.ProcessEvent(e)
			if err != nil {
				log.L.Errorf("Error procssing event: %v", err.Error())
				continue
			}
			lastTime = e.Timestamp
		} else {
			log.L.Warnf("Unkown type in channel %v", i)
		}
		log.L.Debugf("Waiting for next event..")
	}

	//build our state machine
	d := map[string]sm.MachineState{}

	for k, v := range c.Machine.CurStates {
		d[k] = *v
	}

	return config.State{
		LastEventTime: lastTime,
		Data:          d,
	}, nil
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
				Actions: []func(map[string]interface{}, events.Event) ([]ci.MetricsRecord, *nerr.E){
					c.BuildUnblankedRecord,
				},
			},
		},
		Enter: PowerOnStore,
		Exit:  c.BuildInputRecord,
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
				Internal:    true,
				Actions: []func(map[string]interface{}, events.Event) ([]ci.MetricsRecord, *nerr.E){
					InputStore,
				},
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
				TriggerKey:   "power",
				TriggerValue: "on",
				Destination:  "poweron",
			},
			sm.Transition{
				TriggerKey:  "input",
				Destination: "powerstandby",
				Internal:    true,
				Actions: []func(map[string]interface{}, events.Event) ([]ci.MetricsRecord, *nerr.E){
					InputStore,
				},
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

	return c.AddMetaInfo(startTime, e, toReturn)
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

	return c.AddMetaInfo(startTime, e, toReturn)
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

	return c.AddMetaInfo(startTime, e, toReturn)
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

	return c.AddMetaInfo(startTime, e, toReturn)
}

//BuildInputRecord .
func (c *MachineCaterpillar) BuildInputRecord(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {

	//generate the event
	toReturn := ci.MetricsRecord{
		RecordType: ci.Input,
	}

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
				return c.AddMetaInfo(startTime, e, toReturn)
			}
			return []ci.MetricsRecord{}, nerr.Create(fmt.Sprintf("input-store not set to time.Time, value %v", state["input-store"]), "invalid-state")
		}
		return []ci.MetricsRecord{}, nerr.Create(fmt.Sprintf("Invlaid type stored in input: %v", curInput), "invalid-state")

		//unkown value stored
	}

	log.L.Warnf("Cannot create input record with input state not set.")
	return []ci.MetricsRecord{}, nil
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
	return rec, err
}

//UnBlankStore .
func UnBlankStore(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {
	if e.Key == "blanked" {
		state["blank-set"] = e.Timestamp
		state["blanked"] = false
	}
	return []ci.MetricsRecord{}, nil
}

//InputStore .
func InputStore(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {
	if e.Key == "input" && e.Value != "" {
		state["input-set"] = e.Timestamp
		state["input"] = e.Value
	}
	return []ci.MetricsRecord{}, nil
}

//PowerOnStore .
func PowerOnStore(state map[string]interface{}, e events.Event) ([]ci.MetricsRecord, *nerr.E) {
	if e.Key == "power" {
		state["power-set"] = e.Timestamp
		state["blank-set"] = e.Timestamp
		state["input-set"] = e.Timestamp
		state["power"] = "on"
		state["blanked"] = false
	}
	return []ci.MetricsRecord{}, nil
}

//AddMetaInfo .
func (c *MachineCaterpillar) AddMetaInfo(startTime time.Time, e events.Event, r ci.MetricsRecord) ([]ci.MetricsRecord, *nerr.E) {

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
		return []ci.MetricsRecord{r}, err
	}

	if room, ok := c.rooms[r.Room.ID]; ok {
		r.Room = room
	} else {
		err := nerr.Create(fmt.Sprintf("unkown room %v", r.Device.ID), "invalid-room")
		log.L.Errorf("%v", err.Error())
		return []ci.MetricsRecord{r}, err
	}

	return AddClassTimes(startTime, e.Timestamp, r)
}

//RegisterGobStructs .
func (c *MachineCaterpillar) RegisterGobStructs() {
	c.GobRegisterOnce.Do(func() {
		gob.Register(map[string]sm.MachineState{})
		gob.Register(time.Time{})
	})
}

//AddClassTimes .
func AddClassTimes(start, end time.Time, r ci.MetricsRecord) ([]ci.MetricsRecord, *nerr.E) {
	log.L.Debugf("Adding class times for %v", r.Room.ID)

	//we split the record into multiple recors, each with the class info filled out for that class.
	schedules, err := registar.GetClassScheduleForTimeBlock(r.Room.ID, start, end)
	if err != nil {
		return []ci.MetricsRecord{}, err.Addf("Couldn't add class info to event")
	}
	log.L.Debugf("Got the schedules.")

	loc, er := time.LoadLocation("America/Denver")
	if er != nil {
		log.L.Errorf("Couldn't load America/Denver time zone: %v", er.Error())
		return []ci.MetricsRecord{}, nerr.Translate(er)
	}

	adjust := 0 * time.Hour //hours to add

	//we need to check if the times in questions are daylight savings or not... things are read in the -7 time zone, so we're moving to the -6
	_, offset := start.In(loc).Zone()
	if offset == -6*60*60 {
		adjust = 1 * time.Hour
	}

	log.L.Debugf("Adding class times to event-type %+v. StartTime %v, end Time %v", r.RecordType, start.In(time.Local), end.In(time.Local))

	toReturn := []ci.MetricsRecord{}

	lastClassEnd := start
	//we need to go through add class info
	for _, v := range schedules {
		v.StartTime.Add(adjust)
		v.EndTime.Add(adjust)

		curInfo := ci.ClassInfo{
			DeptName:        v.DeptName,
			CatalogNumber:   v.CatalogNumber,
			ClassName:       fmt.Sprintf("%v-%v", v.DeptName, v.CatalogNumber),
			CreditHours:     v.CreditHours,
			ClassSize:       v.SectionSize,
			ClassEnrollment: v.TotalEnr,
			Instructor:      v.InstructorName,

			ClassStart: v.StartTime,
			ClassEnd:   v.EndTime,
		}

		if lastClassEnd.Before(v.StartTime) {
			//I need to generate one for the time from last class end to v.StartTime

			tmp := r
			tmp.StartTime = lastClassEnd
			tmp.EndTime = v.StartTime
			tmp.ElapsedInSeconds = int64((end.Sub(start)) / time.Second)

			log.L.Debugf("Adding front-padded time block %v %v ", tmp.StartTime.In(time.Local), tmp.EndTime.In(time.Local))
			toReturn = append(toReturn, tmp)
		}

		//now we need to figure out the end time
		tmpend := v.EndTime
		if tmpend.After(end) {
			tmpend = end
		}

		tmp := r
		tmp.StartTime = v.StartTime
		tmp.EndTime = tmpend
		tmp.ElapsedInSeconds = int64((end.Sub(start)) / time.Second)
		tmp.Class = curInfo
		toReturn = append(toReturn, tmp)
		log.L.Debugf("Adding class time block for %v from %v to %v ", tmp.Class.ClassName, tmp.StartTime.In(time.Local), tmp.EndTime.In(time.Local))

		lastClassEnd = tmpend
	}

	if !lastClassEnd.Equal(end) {
		//we need to generate one for the end block
		tmp := r
		tmp.StartTime = lastClassEnd
		tmp.EndTime = end
		tmp.ElapsedInSeconds = int64((end.Sub(start)) / time.Second)
		log.L.Debugf("Adding back-padded time block %v %v ", tmp.StartTime.In(time.Local), tmp.EndTime.In(time.Local))

		toReturn = append(toReturn, tmp)
	}

	return toReturn, nil
}

//WrapAndSend .
func (c *MachineCaterpillar) WrapAndSend(r ci.MetricsRecord) {
	if r.ElapsedInSeconds < 1 {
		return
	}

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

//GetDeviceAndRoomInfo .
func GetDeviceAndRoomInfo() (map[string]ci.DeviceInfo, map[string]ci.RoomInfo, *nerr.E) {
	toReturnDevice := map[string]ci.DeviceInfo{}
	toReturnRooms := map[string]ci.RoomInfo{}

	devs, err := db.GetDB().GetAllDevices()
	if err != nil {
		if v, ok := err.(*nerr.E); ok {
			return toReturnDevice, toReturnRooms, v.Addf("Coudn't get device and room info.")
		}
		return toReturnDevice, toReturnRooms, nerr.Translate(err).Addf("Couldn't get device and room info.")
	}
	log.L.Infof("Initializing device list with %v devices", len(devs))

	for i := range devs {
		tmp := ci.DeviceInfo{
			ID:         devs[i].ID,
			DeviceType: devs[i].Type.ID,
			Tags:       devs[i].Tags,
		}

		//build the roles
		for j := range devs[i].Roles {
			tmp.DeviceRoles = append(tmp.DeviceRoles, devs[i].Roles[j].ID)
		}

		toReturnDevice[devs[i].ID] = tmp
	}

	rooms, err := db.GetDB().GetAllRooms()
	if err != nil {
		if v, ok := err.(*nerr.E); ok {
			return toReturnDevice, toReturnRooms, v.Addf("Coudn't get device and room info.")
		}
		return toReturnDevice, toReturnRooms, nerr.Translate(err).Addf("Couldn't get device and room info.")
	}

	log.L.Infof("Initializing room list with %v rooms", len(rooms))
	for i := range rooms {
		toReturnRooms[rooms[i].ID] = ci.RoomInfo{
			ID:              rooms[i].ID,
			Tags:            rooms[i].Tags,
			DeploymentGroup: rooms[i].Designation,
		}
	}

	return toReturnDevice, toReturnRooms, nil
}
