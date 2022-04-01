package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/null-channel/null-db/pkg/db"
)

type DbServer struct {
	l     *log.Logger
	logdb *db.DB
}

type InsertRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GetRequest struct {
	Key string `json:"key"`
}

type DeleteRequest struct {
	Key string `json:"key"`
}

func NewDbServer(l *log.Logger, db *db.DB) *DbServer {
	return &DbServer{l, db}
}

func (s *DbServer) InsertHandler(rw http.ResponseWriter, r *http.Request) {
	s.l.Println("Recived Insert request")
	req := InsertRequest{}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		s.l.Println("unable to decode request")
		http.Error(rw, "unable to decode request", 500)
		return
	}
	err = s.logdb.Set(req.Key, req.Value)
	if err != nil {
		s.l.Println("unable to process request")
		http.Error(rw, fmt.Sprintf("unable to process request reason %s", err), 404)
		return
	}
	s.l.Printf("Set %s to value %s", req.Key, req.Value)

	rw.Write([]byte(req.Key))
}

func (s *DbServer) GetHandler(rw http.ResponseWriter, r *http.Request) {
	s.l.Println("received Get request")
	req := GetRequest{}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		s.l.Printf("unable to process get request for key %s", req.Key)
		http.Error(rw, "unable to process request", http.StatusBadRequest)
		return

	}
	s.l.Printf("obtaining value for key %s", req.Key)
	val, err := s.logdb.Get(req.Key)
	if err != nil {
		s.l.Println("unable to process request")
		http.Error(rw, fmt.Sprintf("%s", err), 404)
		return
	}
	rw.Write([]byte(val))
}
func (s *DbServer) DeleteHandler(rw http.ResponseWriter, r *http.Request) {
	s.l.Printf("received delete request")
	req := DeleteRequest{}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		s.l.Println("unable to process request")
		http.Error(rw, "unable to process", 500)
		return
	}
	s.l.Printf("deleting key %s", req.Key)
	resp, err := s.logdb.Delete(req.Key)
	if err != nil {
		s.l.Println("unable to process request")
		http.Error(rw, fmt.Sprintf("%s", err), 404)
		return
	}
	rw.Write([]byte(resp))

}
