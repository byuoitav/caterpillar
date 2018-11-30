package catinter

import (
	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/caterpillar/nydus"
	"github.com/byuoitav/common/nerr"
)

//Caterpillar returns error and the state that will be passed in as the 'state' variable on the next run of this caterpillar
type Caterpillar interface {
	Run(id string, recordCount int, state config.State, outChan chan nydus.BulkRecordEntry, c config.Caterpillar, GetData func(cap int) (chan interface{}, *nerr.E)) (config.State, *nerr.E)

	RegisterGobStructs() //It's assumed that you'll initialize gob in this case with the interfaces that Data will be used for in your case.
}
