package statemachine

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/awalterschulze/gographviz"
	"github.com/byuoitav/caterpillar/caterpillar/catinter"
	cst "github.com/byuoitav/caterpillar/caterpillar/catinter"
	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/nydus"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/common/v2/events"
)

const (
	//GenAction .
	GenAction = "gen-action"
)

//BuildStateMachine ScopeType corresponds to a field in the event to use as the 'key' for the statemachine. Currently we only accept 'deviceid', 'roomid', or 'buildingid'
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

//PrintSimpleDotFile .
func (m *Machine) PrintSimpleDotFile() *nerr.E {
	graph := gographviz.NewGraph()
	graph.SetDir(true)

	for i := range m.Nodes {
		err := graph.AddNode("Machines", m.Nodes[i].ID, map[string]string{
			"shape":     "octagon",
			"color":     "\"#e53935\"",
			"fontcolor": "\"#e53935\"",
		})
		if err != nil {
			log.L.Errorf("%v", err.Error())
			return nerr.Translate(err)
		}

		for j, t := range m.Nodes[i].Transitions {
			pid := strconv.Itoa(j) //pathid, grows as it moves
			//add a node for this exit (if any)
			name, err := AddExit(m.Nodes[i], pid, t, graph)
			if err != nil {
				return err
			}
			//now we add one for each action
			for k, a := range t.Actions {

				pid += strconv.Itoa(k)
				name, err = AddAction(m.Nodes[i], name, pid, t, a, graph, name == m.Nodes[i].ID)
				if err != nil {
					return err
				}
			}
			//now we check to see if there's an entry node for our destination
			name, err = AddEntry(m.Nodes[i], m.Nodes[t.Destination], name, pid, t, graph, name == m.Nodes[i].ID)
			if err != nil {
				return err
			}
		}
	}
	fmt.Printf("%v\n", graph.String())
	err := ioutil.WriteFile("simple-out.dot", []byte(graph.String()), 0777)
	if err != nil {
		log.L.Errorf("Coudn't write dot file: %v.", err)
		return nerr.Translate(err)
	}

	return nil
}

//AddAction .
func AddAction(n Node, prev, transitionID string, t Transition, a func(map[string]interface{}, events.Event) ([]catinter.MetricsRecord, *nerr.E), g *gographviz.Graph, transitionLabel bool) (string, *nerr.E) {
	name := runtime.FuncForPC(reflect.ValueOf(a).Pointer()).Name()
	label := strings.TrimRight(name[strings.LastIndex(name, ".")+1:], "-fm")
	name = fmt.Sprintf("%v%v%v", label, n.ID, transitionID)

	err := g.AddNode("Machines", name, map[string]string{
		"shape": "box",
		"label": fmt.Sprintf("\"%v\"", label),
	})
	if err != nil {
		log.L.Errorf("%v", err.Error())
		return "", nerr.Translate(err)
	}

	//add an edge
	err = g.AddEdge(prev, name, true, map[string]string{
		"label": generateTransitionLabel(t),
	})
	if err != nil {
		log.L.Errorf("%v", err.Error())
		return "", nerr.Translate(err)
	}
	return name, nil
}

//AddEntry .
func AddEntry(src, dst Node, prev, transitionID string, t Transition, g *gographviz.Graph, transitionLabel bool) (string, *nerr.E) {

	if dst.Enter != nil && !t.Internal {
		name := runtime.FuncForPC(reflect.ValueOf(dst.Enter).Pointer()).Name()
		label := strings.TrimRight(name[strings.LastIndex(name, ".")+1:], "-fm")
		name = fmt.Sprintf("%v%v%v", label, src.ID, transitionID)

		err := g.AddNode("Machines", name, map[string]string{
			"shape":     "box",
			"label":     fmt.Sprintf("\"%v\"", label),
			"color":     "\"#4caf50\"",
			"fontcolor": "\"#4caf50\"",
		})
		if err != nil {
			log.L.Errorf("%v", err.Error())
			return "", nerr.Translate(err)
		}

		//add an edge
		if transitionLabel {
			err = g.AddEdge(prev, name, true, map[string]string{
				"label": generateTransitionLabel(t),
			})
		} else {
			err = g.AddEdge(prev, name, true, nil)
		}
		if err != nil {
			log.L.Errorf("%v", err.Error())
			return "", nerr.Translate(err)
		}

		//add an edge from the exit fun to the actual node
		if transitionLabel {
			err = g.AddEdge(name, dst.ID, true, map[string]string{
				"label": generateTransitionLabel(t),
			})
		} else {
			err = g.AddEdge(name, dst.ID, true, nil)
		}

		if err != nil {
			log.L.Errorf("%v", err.Error())
			return "", nerr.Translate(err)
		}

		return dst.ID, nil

	}

	var err error
	//add an edge from the exit fun to the actual node
	if transitionLabel {
		err = g.AddEdge(prev, dst.ID, true, map[string]string{
			"label": generateTransitionLabel(t),
		})
	} else {
		err = g.AddEdge(prev, dst.ID, true, nil)
	}

	if err != nil {
		log.L.Errorf("%v", err.Error())
		return "", nerr.Translate(err)
	}

	return dst.ID, nil
}

//AddExit .
func AddExit(n Node, transitionID string, t Transition, g *gographviz.Graph) (string, *nerr.E) {

	if n.Exit != nil && !t.Internal {
		name := runtime.FuncForPC(reflect.ValueOf(n.Exit).Pointer()).Name()
		label := strings.TrimRight(name[strings.LastIndex(name, ".")+1:], "-fm")
		name = fmt.Sprintf("%v%v%v", label, n.ID, transitionID)

		err := g.AddNode("Machines", name, map[string]string{
			"shape":     "box",
			"label":     fmt.Sprintf("\"%v\"", label),
			"color":     "\"#7E57C2\"",
			"fontcolor": "\"#7E57C2\"",
		})
		if err != nil {
			log.L.Errorf("%v", err.Error())
			return "", nerr.Translate(err)
		}

		//add an edge
		err = g.AddEdge(n.ID, name, true, map[string]string{
			"label": generateTransitionLabel(t),
		})
		if err != nil {
			log.L.Errorf("%v", err.Error())
			return "", nerr.Translate(err)
		}
		return name, nil

	}
	return n.ID, nil
}

func generateTransitionLabel(t Transition) string {

	if t.TriggerValue == nil {
		return fmt.Sprintf("\"Key %v Value: *\"", t.TriggerKey)
	}
	return fmt.Sprintf("\"Key %v Value: %v\"", t.TriggerKey, t.TriggerValue)
}
