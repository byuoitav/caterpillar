package caterpillar

import (
	"fmt"

	"github.com/byuoitav/caterpillar/caterpillar/catinter"
	"github.com/byuoitav/caterpillar/caterpillar/corestatetime"
	"github.com/byuoitav/caterpillar/caterpillar/test"
	"github.com/byuoitav/common/nerr"
)

var caterpillarRegistry map[string]func() (catinter.Caterpillar, *nerr.E)

func init() {
	caterpillarRegistry = map[string]func() (catinter.Caterpillar, *nerr.E){
		"joe_test":                test.GetCaterpillar,
		"core-state-time-machine": corestatetime.GetMachineCaterpillar,
	}
}

//GetCaterpillar .
func GetCaterpillar(cattype string) (catinter.Caterpillar, *nerr.E) {

	cat, ok := caterpillarRegistry[cattype]
	if !ok {
		return nil, nerr.Create(fmt.Sprintf("Unkown caterpillar type %v", cattype), "unkown-type")
	}

	return cat()

}
