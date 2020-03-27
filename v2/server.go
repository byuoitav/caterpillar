package main

import (
	"os"

	"github.com/byuoitav/caterpillar/v2/displayinputcaterpillar"
	"github.com/byuoitav/common/log"
)

func main() {
	log.SetLevel("debug")
	displayinputcaterpillar.StartDisplayInputCaterpillar(os.Args[1])
}
