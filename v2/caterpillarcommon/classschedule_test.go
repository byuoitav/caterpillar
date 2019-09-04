package caterpillarcommon

import (
	"testing"

	"github.com/byuoitav/common/log"
)

func Test(t *testing.T) {
	log.SetLevel("debug")
	SyncClassScheduleWithDatabase()
}
