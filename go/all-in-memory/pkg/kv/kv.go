package kv

import "sync"

type KVStore struct {
     mu sync.RWMutex
     store map[string]string
}

func NewKv() *KVStore{
	return &KVStore{
		store: map[string]string{},
	}
}

// Get returns the value of k  
func ( kv *KVStore) Get(k string) string{
	defer kv.mu.Unlock()
	kv.mu.Lock()
	val := kv.store[k]
	return val 
}

func (kv *KVStore) Set(k,v string) string {
	defer kv.mu.Unlock()
	kv.mu.Lock()
	kv.store[k] =  v 
	return k 
}


