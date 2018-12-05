package statemachine

import (
	"github.com/byuoitav/caterpillar/caterpillar/catinter"
	cst "github.com/byuoitav/caterpillar/caterpillar/catinter"
	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/nydus"
	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/common/v2/events"
)

const (
	//GenAction .
	GenAction = "gen-action"
)

//BuildStateMachine ScopeType corresponds to a field in the event to use as the 'key' for the statemachine. Currently we only accept 'deviceid'
func BuildStateMachine(scopeKey string, nodes map[string]Node, startNode string, state config.State, cat catinter.Caterpillar) (*Machine, *nerr.E) {

	m := Machine{
		ScopeKey:    scopeKey,
		Nodes:       nodes,
		StartNode:   startNode,
		CurStates:   map[string]*MachineState{},
		Caterpillar: cat,
	}

	if state.Data != nil {
		if v, ok := state.Data.(map[string]MachineState); ok {
			for k := range v {
				val := v[k]
				m.CurStates[k] = &val
			}
		}
	}

	return &m, nil
}

//Machine .
type Machine struct {
	ScopeKey  string
	Nodes     map[string]Node
	OutChan   chan nydus.BulkRecordHeader
	StartNode string

	CurStates map[string]*MachineState

	Caterpillar catinter.Caterpillar
}

//MachineState .
type MachineState struct {
	CurNode    string
	ValueStore map[string]interface{}
}

//Node .
type Node struct {
	ID          string //must be unique .
	Enter       func(map[string]interface{}, events.Event) ([]cst.MetricsRecord, *nerr.E)
	Exit        func(map[string]interface{}, events.Event) ([]cst.MetricsRecord, *nerr.E)
	Transitions []Transition //if match multiple transitions, the first declared will be taken.
}

//Transition .
type Transition struct {
	ID           string      //only used to reference purposes
	TriggerKey   string      //check for the event.key
	TriggerValue interface{} //Corresponds to event.Value. Either a concrete value (string), or if you want to tigger on a == or != relationship with a store value, you can use a TransitionTrigger value.

	Actions  []func(map[string]interface{}, events.Event) ([]cst.MetricsRecord, *nerr.E) //runs before ANY transitionbbbb
	Internal bool                                                                        //if true and destination and source nodes are the same, it won't run th enter and exit jobs

	Destination string
}

//TransitionStoreValue .
type TransitionStoreValue struct {
	StoreValue string
}

//PrintDotFile .
func (m *Machine) PrintDotFile() {

}
