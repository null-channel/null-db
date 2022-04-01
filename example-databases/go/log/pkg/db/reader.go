package db

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

// Represents a deleted value
const Tombstone = "~tombstone~"

var ErrorKeyNotFound = errors.New("key not found")

type DB struct {
	mu      sync.RWMutex
	l       *log.Logger
	logfile string
}

func NewDB(l *log.Logger, lf string) *DB {
	return &DB{
		l:       l,
		logfile: lf,
	}
}

func ReverseSlice(data []string) []string {
	m := len(data) - 1
	var out = []string{}
	for i := m; i >= 0; i-- {
		out = append(out, data[i])
	}
	return out
}

func (db *DB) Get(K string) (string, error) {
	defer db.mu.RUnlock()
	file, err := os.Open(db.logfile)
	if err != nil {
		db.l.Printf("an error occured opening the file: %s", err.Error())
		return "", err
	}
	defer file.Close()
	db.mu.RLock()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var data []string
	for scanner.Scan() {
		data = append(data, scanner.Text())
	}
	data = ReverseSlice(data)
	for _, kv := range data {
		key := strings.Split(kv, ":")
		if key[1] == Tombstone {
			return "", ErrorKeyNotFound
		}
		if key[0] == K {
			return key[1], nil
		}
	}
	return "", ErrorKeyNotFound
}

func (db *DB) Set(k, v string) error {
	defer db.mu.Unlock()
	db.mu.Lock()
	file, err := os.OpenFile(db.logfile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	text := fmt.Sprintf("%s:%s\n", k, v)
	_, err = file.WriteString(text)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) Delete(K string) (string, error) {
	file, err := os.Open(db.logfile)
	if err != nil {
		db.l.Printf("an error occured opening the file: %s", err.Error())
		return "", err
	}
	db.mu.Lock()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var data []string
	for scanner.Scan() {
		data = append(data, scanner.Text())
	}
	ReverseSlice(data)
	file.Close()
	for _, kv := range data {
		key := strings.Split(kv, ":")
		if key[0] == K {
			file, err := os.OpenFile(db.logfile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
			if err != nil {
				return "", err
			}
			text := fmt.Sprintf("%s:%s\n", K, Tombstone)
			_, err = file.WriteString(text)
			if err != nil {
				return "", err
			}

		}
	}
	defer db.mu.Unlock()
	return "", ErrorKeyNotFound
}
