package statemachine

import (
	"fmt"

	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/common/v2/events"
)

//ProcessEvent .
func (m *Machine) ProcessEvent(e events.Event) *nerr.E {

	k, err := GetScope(m.ScopeKey, e)
	if err != nil {
		return err.Add("Couldn't process event.")
	}
	cur := m.CurStates[k]

	//check the transitions from the current state of m
	curNode, ok := m.Nodes[cur.CurNode]
	if !ok {
		return nerr.Create("unkown current node: %v", cur.CurNode)
	}

	for _, t := range curNode.Transitions {
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
				//otherwise we transition
				m.transition(e, t, cur)

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
	//do node exit

	if m.Nodes[CurState.CurNode].Exit != nil {
		record, err := m.Nodes[CurState.CurNode].Exit(CurState.ValueStore, e)
		if err != nil {
			err.Add("Couldn't generate record")
			return err
		}
		m.Caterpillar.WrapAndSend(record)
	}

	if t.Before != nil {
		//do before
		record, err := t.Before(CurState.ValueStore, e)
		if err != nil {
			err.Add("Couldn't generate record")
			return err
		}

		m.Caterpillar.WrapAndSend(record)
	}

	if t.Store != nil {
		//store values
		t.Store(CurState.ValueStore, e)
	}

	if t.After != nil {
		//do after
		record, err := t.After(CurState.ValueStore, e)
		if err != nil {
			err.Add("Couldn't generate record")
			return err
		}
		m.Caterpillar.WrapAndSend(record)
	}
	if m.Nodes[t.Destination].Enter != nil {
		//do node enter
		record, err := m.Nodes[t.Destination].Enter(CurState.ValueStore, e)
		if err != nil {
			err.Add("Couldn't generate record")
			return err
		}
		m.Caterpillar.WrapAndSend(record)
	}

	//set currentnodea
	CurState.CurNode = t.Destination

	return nil
}

//GetScope .
func GetScope(key string, e events.Event) (string, *nerr.E) {
	return e.TargetDevice.DeviceID, nil
}
