package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"

	"github.com/byuoitav/common/nerr"
)

//Config represents the config for an entire hatchery, e.g. multiple caterpillars.
type Config struct {
	Caterpillars  []Caterpillar `json:"caterpillars"`
	StoreLocation string        `json:"store-location"` //the location of the file to use for data persistance.
}

//Caterpillar is the configuration for a single caterpillar instance.
type Caterpillar struct {
	ID        string      `json:"id,omitempty"`         //Identifier, must be unique to other caterpillars spawned by this hatchery. If left blank an identifier will be generated.
	Type      string      `json:"type"`                 //link to code to write.
	QueryFile string      `json:"query-file,omitempty"` //the file to find the ELK query in, must specify either this or query.
	Query     interface{} `json:"query,omitempty"`      //The ELK query in, must specify either this or query-file.
	Interval  string      `json:"interval,omitempty"`   //How often to spawn this caterpillar, in crontab format. See https://godoc.org/github.com/robfig/cron.

}

var once sync.Once

var config Config
var configerr *nerr.E

//GetConfig .
func GetConfig() (Config, *nerr.E) {
	once.Do(func() {
		var file string
		if file = os.Getenv("CONFIG_LOCATION"); file == "" {
			file = "./service-config.json"
		}

		b, err := ioutil.ReadFile(file)
		if err != nil {
			configerr = nerr.Translate(err).Addf("Couldn't read the config file at %v", file)
		}
		err = json.Unmarshal(b, &config)
		if err != nil {
			configerr = nerr.Translate(err).Addf("Couldn't unmarshal the config file from %v", file)
		}
	})

	return config, configerr
}
