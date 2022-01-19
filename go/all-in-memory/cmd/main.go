package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/null-channel/null-db/pkg/kv"
	"github.com/null-channel/null-db/pkg/server"
	"github.com/teris-io/shortid"
)

var (
	 id, _ = shortid.Generate()
	 serverid = fmt.Sprintf("[kvstore-%s] ",id)
 	 l = log.New(os.Stdout,serverid,log.LstdFlags)
 )	

func main(){
	kvstore := kv.NewKv()
	kvhanlder := server.NewKvServer(l,kvstore)
 	router := mux.NewRouter()
	router.HandleFunc("/get",kvhanlder.GetHandler).Methods(http.MethodPost)
	router.HandleFunc("/set",kvhanlder.InsertHandler).Methods(http.MethodPost)
	l.Println("starting server on port 4000")
	http.Handle("/",router)
	err := http.ListenAndServe(":4000",nil) 
	if err != nil {
		l.Fatalf("unable to start server on 4000 %s", err.Error())
		os.Exit(1)
	}
}
