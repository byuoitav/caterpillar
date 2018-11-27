package config

import "time"

//State is the struct stored by each caterpillar between runs
type State struct {
	LastEventTime time.Time
	Data          interface{}
}
