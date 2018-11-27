package caterpillar

import (
	"time"

	"github.com/byuoitav/common/nerr"
)

//Caterpillar returns error and the state that will be passed in as the 'state' variable on the next run of this caterpillar
type Caterpillar interface {
	Run(id string, recordCount int, start, end time.Time, GetData func() (records []interface{}, err *nerr.E), state interface{}) (*nerr.E, interface{})
}
