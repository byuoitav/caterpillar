package statemachine

import (
	"fmt"
	"time"

	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/common/v2/events"
)

//ProcessEvent .
func (m *Machine) ProcessEvent(e events.Event) *nerr.E {
	location, er := time.LoadLocation("America/Denver")
	if er != nil {
		log.L.Fatalf("Couldn't load timezone: %v", er.Error())
	}

	k, err := GetScope(m.ScopeKey, e)
	if err != nil {
		return err.Add("Couldn't process event.")
	}
	cur, ok := m.CurStates[k]
	if !ok {
		tmp := MachineState{
			CurNode:    m.StartNode,
			ValueStore: map[string]interface{}{},
		}
		cur = &tmp
		m.CurStates[k] = cur
	}

	log.L.Debugf("Current state %v", cur.CurNode)
	log.L.Debugf("Processing event %v, %v, %v, %v", k, e.Key, e.Value, e.Timestamp.In(location).Format("15:04:05 01-02"))
	//check the transitions from the current state of m
	curNode, ok := m.Nodes[cur.CurNode]
	if !ok {
		return nerr.Create("unkown current node: %v", cur.CurNode)
	}

	for i, t := range curNode.Transitions {
		if e.Key != t.TriggerKey {
			continue
		}

		//check to see if we need to match on a value
		if t.TriggerValue != nil {
			//check the value we need to match on
			if v, ok := t.TriggerValue.(TransitionStoreValue); ok {
				//get the field from the storea
				if checkValue, ok := cur.ValueStore[v.StoreValue]; ok {
					//assert that checkValue is a string, if not, we coerce it
					valueString, ok := checkValue.(string)
					if !ok {
						valueString = fmt.Sprintf("%v", valueString)
					}
					if e.Value != valueString {
						continue
					}
					//we transition
					m.transition(e, t, cur)

				} else {
					//nothing in the store? Error or continue.
					log.L.Errorf("No value of name %v stored", v.StoreValue)
					continue
				}
			} else if v, ok := t.TriggerValue.(string); ok {
				if e.Value != v {
					continue
				}
				if len(t.ID) > 0 {
					log.L.Debugf("Transitioning on %v", t.ID)
				} else {
					log.L.Debugf("Transitioning on transition %v from state %v", i, curNode.ID)
				}

				log.L.Debugf("Starting transition.")
				err := m.transition(e, t, cur)
				log.L.Debugf("Back from transition")
				if err != nil {
					if len(t.ID) > 0 {
						err = err.Addf("Error with transition %v", t.ID)
					} else {
						err = err.Addf("Error with transition number %v for state %v", i, curNode.ID)
					}
					log.L.Errorf("%v", err.Error())
					return err
				}

				return nil

			} else {
				log.L.Errorf("Unkown triggerValue %v", t.TriggerValue)
				return nerr.Create(fmt.Sprintf("INvalid TriggerValue on transition %v", t), "invalid-config")
			}
		}
		//otherwise we transition
		m.transition(e, t, cur)

	}

	return nil
}

//transition
func (m *Machine) transition(e events.Event, t Transition, CurState *MachineState) *nerr.E {

	internal := t.Internal && t.Destination == CurState.CurNode
	if internal {
		log.L.Debugf("Internal transition")
	}

	log.L.Debugf("Running external transition")
	//do node exit
	if m.Nodes[CurState.CurNode].Exit != nil && !internal {
		records, err := m.Nodes[CurState.CurNode].Exit(CurState.ValueStore, e)
		if err != nil {
			err.Add("Couldn't generate record")
			return err
		}
		log.L.Debugf("Exit generated %v records", len(records))
		for i := range records {
			m.Caterpillar.WrapAndSend(records[i])
		}
	}
	log.L.Debugf("Done with exit.")

	for i := range t.Actions {
		records, err := t.Actions[i](CurState.ValueStore, e)
		if err != nil {
			err.Add("Couldn't generate record")
			return err
		}
		for i := range records {
			m.Caterpillar.WrapAndSend(records[i])
		}
	}
	log.L.Debugf("Done with actions.")
	if m.Nodes[t.Destination].Enter != nil && !internal {
		//do node enter
		records, err := m.Nodes[t.Destination].Enter(CurState.ValueStore, e)
		if err != nil {
			err.Add("Couldn't generate record")
			return err
		}
		for i := range records {
			m.Caterpillar.WrapAndSend(records[i])
		}
	}
	log.L.Debugf("Done with enter.")

	//set currentnodea
	CurState.CurNode = t.Destination

	return nil
}

//GetScope .
func GetScope(key string, e events.Event) (string, *nerr.E) {
	switch key {
	case "deviceid":
		return e.TargetDevice.DeviceID, nil
	case "roomid":
		return e.TargetDevice.RoomID, nil
	case "buildingid":
		return e.TargetDevice.BuildingID, nil

	}
	return "", nil
}
