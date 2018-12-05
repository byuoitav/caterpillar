package corestatetime

import (
	"encoding/gob"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/byuoitav/caterpillar/caterpillar/catinter"
	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/nydus"
	"github.com/byuoitav/common/db"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/common/v2/events"
)

var gobRegisterOnce sync.Once

func init() {
	gobRegisterOnce = sync.Once{}
}

//Caterpillar  .
type Caterpillar struct {
	curState map[string]State //mapping of device to that devices' state .
	devices  map[string]catinter.DeviceInfo
	rooms    map[string]catinter.RoomInfo

	outChan chan nydus.BulkRecordEntry
	index   string
	rectype string
}

//State .
type State struct {
	CurVolume int
	VolumeSet bool //so we know the difference between nothing and 0

	Muted           bool
	AudioChangeTime time.Time

	CurInput          string
	Blanked           bool
	VideoChangeTime   time.Time
	BlankedChangeTime time.Time

	CurPower        string
	PowerChangeTime time.Time
}

//GetCaterpillar .
func GetCaterpillar() (catinter.Caterpillar, *nerr.E) {

	toReturn := &Caterpillar{
		rectype:  "metrics",
		devices:  map[string]catinter.DeviceInfo{},
		rooms:    map[string]catinter.RoomInfo{},
		curState: map[string]State{},
	}

	err := toReturn.GetDeviceAndRoomInfo()
	if err != nil {
		return toReturn, err.Addf("Couldn't initialize corestatetime caterpillar.")
	}
	return toReturn, nil
}

//Run .
func (c *Caterpillar) Run(id string, recordCount int, state config.State, outChan chan nydus.BulkRecordEntry, cnfg config.Caterpillar, GetData func(int) (chan interface{}, *nerr.E)) (config.State, *nerr.E) {
	log.L.Infof("Starting run of corestatetime caterpillar %v", cnfg.ID)
	log.L.Debugf("Config: %v", cnfg)
	log.L.Debugf("Count: %v", recordCount)
	log.L.Debugf("State: %v", state)

	index, ok := cnfg.TypeConfig["output-index"]
	if !ok {
		return state, nerr.Create(fmt.Sprintf("Missing config item for Caterpillar type %v. Need output-index", cnfg.Type), "invalid-config")
	}

	c.index = index
	c.outChan = outChan

	//assert our past state
	if state.Data != nil {
		v, ok := state.Data.(map[string]State)
		if !ok {
			log.L.Errorf("Unkown state map retrieved: %v", state.Data)
		} else {
			c.curState = v
		}
	}

	//So we can track how long this thing takes to run.
	startTime := time.Now()

	inchan, err := GetData(100)
	if err != nil {
		return state, err.Addf("Couldn't run caterpillar. Issue with initial data retreival")
	}

	//	var curEventTime time.Time
	for i := range inchan {
		e, ok := i.(events.Event)
		if !ok {
			log.L.Infof("Unkown event format %v", i)
		}
		c.processEvent(e)
		//	curEventTime = e.Timestamp
	}

	log.L.Debugf("%v-%v-%v", index, startTime, time.Now())

	return config.State{}, nil
}

//processEvent
func (c *Caterpillar) processEvent(e events.Event) {

	log.L.Debugf("Processing event: %v", e)

	//get the display from the statemap
	cur, ok := c.curState[e.TargetDevice.DeviceID]
	if !ok {
		//it's a new device, or one that doesn't have current state.
		cur = State{}
	}
	switch e.Key {
	case "power":
		if e.Value == "standby" {

			//check to see if the device was on before, if it was we need to generate an event.
			if cur.CurPower == "on" {
				//generate a 'time on' event
				r := catinter.MetricsRecord{
					Power:      "on",
					RecordType: catinter.Power,
				}
				c.AddMetaInfoAndSend(cur.PowerChangeTime, e, r)

				if cur.Blanked {
					//generate a 'time blanked' event.
					r := catinter.MetricsRecord{
						Blanked:    &True,
						RecordType: catinter.Blank,
					}
					c.AddMetaInfoAndSend(cur.BlankedChangeTime, e, r)
				} else {
					r := catinter.MetricsRecord{
						Blanked:    &False,
						RecordType: catinter.Blank,
					}
					c.AddMetaInfoAndSend(cur.BlankedChangeTime, e, r)
				}
				if cur.CurInput != "" && !cur.Blanked {

					//generate a 'time on input' event.
					r := catinter.MetricsRecord{
						Input:      cur.CurInput,
						RecordType: catinter.Input,
					}
					c.AddMetaInfoAndSend(cur.VideoChangeTime, e, r)
				}
				if cur.VolumeSet && !cur.Muted {
					vol := cur.CurVolume
					//generate a 'time at volume' event.
					r := catinter.MetricsRecord{
						Volume:     &vol,
						RecordType: catinter.Volume,
					}
					c.AddMetaInfoAndSend(cur.VideoChangeTime, e, r)

				}
				if cur.Muted {
					//generate a 'time muted' event.
					r := catinter.MetricsRecord{
						Muted:      &True,
						RecordType: catinter.Mute,
					}
					c.AddMetaInfoAndSend(cur.AudioChangeTime, e, r)
				}

				cur.CurPower = "standby"
				cur.PowerChangeTime = e.Timestamp
				cur.VideoChangeTime = e.Timestamp
				cur.BlankedChangeTime = e.Timestamp
				cur.AudioChangeTime = e.Timestamp
			} else if cur.CurPower == "" {
				//first event, start.
				cur.PowerChangeTime = e.Timestamp
				cur.BlankedChangeTime = e.Timestamp
				cur.CurPower = "on"
			}

			//otherwise it's a duplicate event, ignore.
		}
		if e.Value == "on" {
			if cur.CurPower == "standby" {
				//we need to generate a 'time standby' record.
				r := catinter.MetricsRecord{
					Power:      "standby",
					RecordType: catinter.Power,
				}
				c.AddMetaInfoAndSend(cur.PowerChangeTime, e, r)

				cur.PowerChangeTime = e.Timestamp
				cur.BlankedChangeTime = e.Timestamp
				cur.VideoChangeTime = e.Timestamp
				cur.AudioChangeTime = e.Timestamp

				cur.CurPower = "on"
			} else if cur.CurPower == "" {
				log.L.Debugf("Processing power event: value: %v", e.Value)
				//first event, start.
				cur.PowerChangeTime = e.Timestamp
				cur.BlankedChangeTime = e.Timestamp
				cur.CurPower = "on"
			}

			//otherwise it's a duplicate event, ignore.
		}
	case "input":
		if !cur.Blanked {
			if cur.CurInput != e.Value && cur.CurInput != "" {
				//it's a change
				r := catinter.MetricsRecord{Input: cur.CurInput, RecordType: catinter.Input}
				c.AddMetaInfoAndSend(cur.VideoChangeTime, e, r)

				cur.CurInput = e.Value
				cur.VideoChangeTime = e.Timestamp
			}
			//duplicate event
		}
		if cur.CurInput == "" {
			cur.VideoChangeTime = e.Timestamp
		}

		// TODO: Validate that changing input doesn't unblank.
		//we're blanked, so we don't care.
		cur.CurInput = e.Value

	case "blanked":
		v, err := strconv.ParseBool(e.Value)
		if err != nil {
			log.L.Errorf("couldn't parse blanked event value %v", e.Value)
			return
		}
		if v != cur.Blanked {
			//it's a change
			if v == true {
				//we're blanking, set a time on input, as well as a time on unblanked
				r := catinter.MetricsRecord{Input: cur.CurInput, RecordType: catinter.Input}
				c.AddMetaInfoAndSend(cur.VideoChangeTime, e, r)

				r = catinter.MetricsRecord{Blanked: &False, RecordType: catinter.Blank}
				c.AddMetaInfoAndSend(cur.BlankedChangeTime, e, r)

				cur.Blanked = v
				cur.VideoChangeTime = e.Timestamp
				cur.BlankedChangeTime = e.Timestamp

			} else {
				//we're unblanking, send a time on blank event
				var r catinter.MetricsRecord
				if cur.Blanked {
					r = catinter.MetricsRecord{Blanked: &True, RecordType: catinter.Blank}
				} else {
					r = catinter.MetricsRecord{Blanked: &False, RecordType: catinter.Blank}

				}
				c.AddMetaInfoAndSend(cur.BlankedChangeTime, e, r)

				cur.Blanked = v
				cur.VideoChangeTime = e.Timestamp
				cur.BlankedChangeTime = e.Timestamp
			}
		}
		if cur.VideoChangeTime.Equal(time.Time{}) {
			//first time, set
			cur.VideoChangeTime = e.Timestamp
		}
		if cur.BlankedChangeTime.Equal(time.Time{}) {
			//first time, set
			cur.BlankedChangeTime = e.Timestamp
		}

	case "volume":
		v, err := strconv.Atoi(e.Value)
		if err != nil {
			log.L.Errorf("couldn't parse volume event value %v", e.Value)
			return
		}
		if cur.Muted {
			//just set the current state
			cur.CurVolume = v
		} else {
			if cur.CurVolume != v {
				//it's a change
				vol := cur.CurVolume
				r := catinter.MetricsRecord{Volume: &vol, RecordType: catinter.Volume}
				c.AddMetaInfoAndSend(cur.AudioChangeTime, e, r)

				cur.CurVolume = v
				cur.AudioChangeTime = e.Timestamp
			}
		}
		if cur.AudioChangeTime.Equal(time.Time{}) {
			//first time, set
			cur.AudioChangeTime = e.Timestamp
		}
	case "muted":
		v, err := strconv.ParseBool(e.Value)
		if err != nil {
			log.L.Errorf("couldn't parse muted event value %v", e.Value)
			return
		}
		if v != cur.Muted {
			//it's a change
			if v == true {
				//we're blanking, set a time on input
				vol := cur.CurVolume
				r := catinter.MetricsRecord{Volume: &vol, RecordType: catinter.Volume}
				c.AddMetaInfoAndSend(cur.AudioChangeTime, e, r)

				cur.Muted = v
				cur.AudioChangeTime = e.Timestamp

			} else {
				//we're unblanking, send a time on blank event
				var r catinter.MetricsRecord
				if cur.Muted {
					r = catinter.MetricsRecord{Muted: &True, RecordType: catinter.Mute}
				} else {
					r = catinter.MetricsRecord{Muted: &False, RecordType: catinter.Mute}
				}
				c.AddMetaInfoAndSend(cur.AudioChangeTime, e, r)

				cur.Muted = v
				cur.AudioChangeTime = e.Timestamp
			}
		}
		if cur.AudioChangeTime.Equal(time.Time{}) {
			//first time, set
			cur.AudioChangeTime = e.Timestamp
		}
	}

	//set back the state.
	c.curState[e.TargetDevice.DeviceID] = cur

}

//AddTimeFields .
func AddTimeFields(start, end time.Time, r *catinter.MetricsRecord) {
	r.StartTime = start
	r.EndTime = end
	r.ElapsedInSeconds = int64((end.Sub(start)) / time.Second)
}

//AddMetaInfoAndSend .
func (c *Caterpillar) AddMetaInfoAndSend(startTime time.Time, e events.Event, r catinter.MetricsRecord) *nerr.E {

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
		return err
	}

	if room, ok := c.rooms[r.Room.ID]; ok {
		r.Room = room
	} else {
		err := nerr.Create(fmt.Sprintf("unkown room %v", r.Device.ID), "invalid-room")
		log.L.Errorf("%v", err.Error())
		return err
	}

	AddTimeFields(startTime, e.Timestamp, &r)
	c.WrapAndSend(r)

	return nil
}

//GetDeviceAndRoomInfo .
func GetDeviceAndRoomInfo() (map[string]catinter.DeviceInfo, map[string]catinter.RoomInfo, *nerr.E) {
	toReturnDevice := map[string]catinter.DeviceInfo{}
	toReturnRooms := map[string]catinter.RoomInfo{}

	devs, err := db.GetDB().GetAllDevices()
	if err != nil {
		if v, ok := err.(*nerr.E); ok {
			return toReturnDevice, toReturnRooms, v.Addf("Coudn't get device and room info.")
		}
		return toReturnDevice, toReturnRooms, nerr.Translate(err).Addf("Couldn't get device and room info.")
	}
	log.L.Infof("Initializing device list with %v devices", len(devs))

	for i := range devs {
		log.L.Debugf("%v", devs[i].ID)
		tmp := catinter.DeviceInfo{
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
		toReturnRooms[rooms[i].ID] = catinter.RoomInfo{
			ID:              rooms[i].ID,
			Tags:            rooms[i].Tags,
			DeploymentGroup: rooms[i].Designation,
		}
	}

	return toReturnDevice, toReturnRooms, nil
}

//GetDeviceAndRoomInfo .
func (c *Caterpillar) GetDeviceAndRoomInfo() *nerr.E {
	var err *nerr.E
	c.devices, c.rooms, err = GetDeviceAndRoomInfo()
	if err != nil {
		return err
	}
	return nil
}

//WrapAndSend .
func (c *Caterpillar) WrapAndSend(r catinter.MetricsRecord) {

	log.L.Debugf("Generated record: %v", r)

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

//RegisterGobStructs .
func (c *Caterpillar) RegisterGobStructs() {
	gobRegisterOnce.Do(func() {
		gob.Register(State{})
	})
}
