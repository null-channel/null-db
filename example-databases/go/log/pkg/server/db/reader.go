package db

import (
	"bufio"
	"errors"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
)

// Represents a deleted value
const  Tombstone = "~tombstone~" 
var ErrorKeyNotFound = errors.New("key not found")

type DB struct {
	mu      sync.RWMutex
	l       *log.Logger
	logfile string
}


func ReverseSlice(data interface{}) {
	value := reflect.ValueOf(data)
	if value.Kind() != reflect.Slice {
		log.Fatal("invalid data type")
	}
	valueLen := value.Len()
	for i := 0; i <= int((valueLen-1)/2); i++ {
		reverseIndex := valueLen -1  - i 
		tmp := value.Index(reverseIndex).Interface()
		value.Index(reverseIndex).Set(value.Index(i))
		value.Index(i).Set(reflect.ValueOf(tmp	))	}
} 

func (db *DB) Get(K string) (string, error) {
	defer db.mu.RUnlock()
	file , err := os.Open(db.logfile)
	if err != nil{
		log.Printf("an error occured opening the file: %s",err.Error())
		return "", err
	}
	defer file.Close()
	db.mu.RLock()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var data []string 
	for scanner.Scan() {
		data = append(data,scanner.Text())
	}
	ReverseSlice(data)
	for _ , kv := range data {
		key := strings.Split(kv,":")
		if key[1] == Tombstone{
			return "", ErrorKeyNotFound
		}
		if key[0] == K {
			return key[1] , nil
		} 
	}
	return "", ErrorKeyNotFound


}

