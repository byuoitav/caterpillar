package displayinputcaterpillar

import "github.com/byuoitav/common/log"

func startDisplayInputCaterpillar() {

	log.L.Debugf("Starting Display Input Caterpillar")

	//run the aggregation query to get the list of devices

	//launch a go routine for each of the devices

	//wait for them all to finish

	//wait for turn off message, run now message, or the timeout and then do it again
}

func caterpillarDevice(deviceName string) {	
	//Get from SQL the last state known

	//Delete anything in SQL / Kibana that is older than the date we're starting at (so if we're redoing we don't have to worry about duplicates)

	//Get all events from delta since that date

	//Go through and create records for each change (should be each event)
	//send to the slicer

	//Update the current state record to be up to the latest whole hour that is more than an hour old
	//send to the slicer

	//update last state known in sql
}

