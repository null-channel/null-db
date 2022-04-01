package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/mux"
	"github.com/null-channel/null-db/pkg/db"
	"github.com/null-channel/null-db/pkg/server"
	"github.com/teris-io/shortid"
)

var (
	createlogfile sync.Once
	logfile       = "log.db"
	id, _         = shortid.Generate()

	// unique server id incase this ever becomes distributed??
	serverid = fmt.Sprintf("[logdb-%s] ", id)
	l        = log.New(os.Stdout, serverid, log.LstdFlags)
)

func main() {

	// create log file once server starts up
	createlogfile.Do(func() {
		_, err := os.Create(logfile)
		if err != nil {
			fmt.Println(err)
		}
	})
	DB := db.NewDB(l, logfile)
	loghandler := server.NewDbServer(l, DB)
	router := mux.NewRouter()
	router.HandleFunc("/get", loghandler.GetHandler).Methods(http.MethodPost)
	router.HandleFunc("/set", loghandler.InsertHandler).Methods(http.MethodPost)
	router.HandleFunc("/pop", loghandler.DeleteHandler).Methods(http.MethodPost)
	l.Println("starting server on port 4000")
	http.Handle("/", router)
	err := http.ListenAndServe(":4000", nil)
	if err != nil {
		l.Fatalf("unable to start server on 4000 %s", err.Error())
	}
}
