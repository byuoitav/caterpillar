package caterpillar

import (
	"encoding/gob"
	"time"

	"github.com/byuoitav/common/nerr"
)

//Caterpillar returns error and the state that will be passed in as the 'state' variable on the next run of this caterpillar
type Caterpillar interface {
	Run(id string, recordCount int, start, end time.Time, GetData func() (records []interface{}, err *nerr.E), state interface{}) (*nerr.E, interface{})

	BuildDeencoder() gob.Decoder //It's assumed that you'll initialize gob in this case with the interfaces that Data will be used for in your case.
}
