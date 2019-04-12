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
	ID          string            `json:"id,omitempty"`           //Identifier, must be unique to other caterpillars spawned by this hatchery. If left blank an identifier will be generated.
	Type        string            `json:"type"`                   //link to code to write.
	Index       string            `json:"index"`                  // the index or type of data to run this caterpillar against.
	QueryFile   string            `json:"query-file,omitempty"`   //the file to find the ELK query in, must specify either this or query.
	Query       interface{}       `json:"query,omitempty"`        //The ELK query in, must specify either this or query-file.
	Interval    string            `json:"interval,omitempty"`     //How often to spawn this caterpillar, in crontab format. See https://godoc.org/github.com/robfig/cron.
	MaxInterval string            `json:"max-interval,omitempty"` //If there isn't a last run time how far back do we create events for. Defaults to forever.
	TimeField   string            `json:"time-field"`             //The field in the elk index to use for time-based filtering. We'll use this to batch our requests for events.
	TypeConfig  map[string]string `json:"type-config"`            //This is for configuration specific to that type. For example, output-index is a common field here.
	AbsStart    string            `json:"absolute-start-time"`    //if you want to run a caterpillar on a specific time frame (non recurring). Must be defined with the AbsEnd. Format is YYYY-MM-DD hh:mm:ss. Caterpillar will exit after initial run if defined.
	AbsEnd      string            `json:"absolute-end-time"`
}

var once sync.Once

var config Config
var configerr *nerr.E
var configMutex *sync.Mutex

func init() {
	configMutex = &sync.Mutex{}
}

//GetConfig .
func GetConfig() (Config, *nerr.E) {
	configMutex.Lock()
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
	configMutex.Unlock()

	return config, configerr
}
