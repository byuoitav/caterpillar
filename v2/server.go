package main

import (
	"net/http"
	
	"github.com/byuoitav/common/log"	
	"github.com/labstack/echo"
)

const port = ":10012"

func main() {
	//log.SetLevel("debug")
	
	

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
	status := ""
	log.L.Debugf("Status: %v", status)

	return context.JSON(http.StatusOK, status)
}
