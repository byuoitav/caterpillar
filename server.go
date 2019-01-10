package main

import (
	"net/http"

	"github.com/byuoitav/caterpillar/hatchery"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
	"github.com/labstack/echo"
)

const port = ":10012"

var hatch *hatchery.Hatchery

func main() {
	log.SetLevel("debug")
	var err *nerr.E
	hatch, err = hatchery.InitializeHatchery()
	if err != nil {
		log.L.Fatalf("%v", err.Error())
	}

	router := echo.New()

	router.GET("/status", getStatus)

	server := http.Server{
		Addr:           port,
		MaxHeaderBytes: 1024 * 10,
	}

	router.StartServer(&server)
}

func getStatus(context echo.Context) error {
	log.L.Debug("Getting status")
	status := hatch.GetStatus()
	log.L.Debugf("Status: %v", status)

	return context.JSON(http.StatusOK, status)
}
