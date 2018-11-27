package store

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/byuoitav/caterpillar/config"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
	"github.com/dgraph-io/badger"
)

var once sync.Once
var db *badger.DB

func initializeStore() {
	c, err := config.GetConfig()
	if err != nil {
		log.L.Fatalf("Couldn't initialize store: Couldn't get config: %v", err.Error())
	}

	var er error

	//build our opts
	opts := badger.DefaultOptions
	opts.Dir = c.StoreLocation
	opts.ValueDir = c.StoreLocation

	db, er = badger.Open(opts)
	if er != nil {
		log.L.Fatalf("Couldn't initialize store: couldn't open database: %v", err.Error())
	}
}

//CloseDB is to be defered in the main func
func CloseDB() {
	db.Close()
}

//GetInfo assumes that gob has been initialized with the needed interfaces.
func GetInfo(id string) (config.State, *nerr.E) {
	once.Do(initializeStore)
	var toReturn config.State

	values := []byte{}

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
		if err != nil {
			return err
		}
		values, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return config.State{}, nerr.Translate(err).Addf("Couldn't get %v from store", id)
	}

	//build our decoder out of values
	dec := gob.NewDecoder(bytes.NewBuffer(values))

	//we assume that value is a gob that can be decoded with dec
	err = dec.Decode(&toReturn)
	if err != nil {
		return config.State{}, nerr.Translate(err).Addf("Couldn't get %v from the datastore, couldn't decode.", id)
	}

	return toReturn, nil
}

//PutInfo .
func PutInfo(id string, info config.State) *nerr.E {
	once.Do(initializeStore)

	buf := &bytes.Buffer{}

	//build our decoder out of values
	enc := gob.NewEncoder(buf)
	err := enc.Encode(info)
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't write %v to the datastore, couldn't encode.", id)
	}

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(id), buf.Bytes())
	})
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't write %v to store", id)
	}

	return nil
}
