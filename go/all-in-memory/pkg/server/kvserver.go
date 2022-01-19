package server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/null-channel/null-db/pkg/kv"
)


type KvServer struct{
	l *log.Logger
	kv *kv.KVStore
}

type InsertRequest struct {
	Key string `json:"key"`
	Value string `json:"value"`
}

type GetRequest struct {
	Key string `json:"key"`
}
func NewKvServer(l *log.Logger, kv *kv.KVStore) *KvServer{
	return &KvServer{l,kv}
}

func (s *KvServer) InsertHandler(rw http.ResponseWriter, r *http.Request) {
	s.l.Println("Recived Insert request")
	req  := InsertRequest{}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil{
		s.l.Println("unable to decode request")
		http.Error(rw,"unable to decode request",500)
		return
	}
	s.l.Printf("Set %s to value %s",req.Key,req.Value)
	val := s.kv.Set(req.Key,req.Value)
	rw.Write([]byte(val))
}

func (s *KvServer) GetHandler(rw http.ResponseWriter, r *http.Request){
	s.l.Println("received Get request") 
	req := GetRequest{}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil{
		s.l.Printf("unable to process get request for key %s",req.Key)
		http.Error(rw,"unable to process request",http.StatusBadRequest)
		return
	
	}
      s.l.Printf("obtaining value for key %s",req.Key)
      val := s.kv.Get(req.Key)
      rw.Write([]byte(val))
}

