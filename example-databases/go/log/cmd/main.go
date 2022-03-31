package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/null-channel/null-db/pkg/db"
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
}
