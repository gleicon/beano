package main

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/facebookgo/inmem"
)

type InmemBackend struct {
	cache             inmem.Cache
	memMutex          sync.RWMutex
	cacheSize         int
	defaultExpiration time.Time
}

/*
	NewInmemBackend receives a size parameter and creates a new Facebook inmem Backend instance
*/
func NewInmemBackend(size int) (*InmemBackend, error) {

	b := InmemBackend{cache: nil, cacheSize: size}
	b.cache = inmem.NewLocked(size)
	// 24h default expiration time
	b.defaultExpiration = time.Now().Add(24 * time.Hour)
	return &b, nil
}

/*
	Set the value for key
*/
func (be InmemBackend) Set(key []byte, value []byte) error {
	be.cache.Add(key, value, be.defaultExpiration)
	return nil
}

/*
	Add value to key, store data only if the server doesnt holds it yet
*/
func (be InmemBackend) Add(key []byte, value []byte) error {
	if _, found := be.cache.Get(key); found {
		return errors.New("Key already exists")
	}
	be.cache.Add(key, value, be.defaultExpiration)
	return nil
}

/*
	Replace value for key, store data only if the server already holds this key
*/
func (be InmemBackend) Replace(key []byte, value []byte) error {
	if _, found := be.cache.Get(key); !found {
		return errors.New("Key do not exists")
	}
	be.cache.Add(key, value, be.defaultExpiration)
	return nil
}

/*
	Incr data, yields error if the represented value doesnt maps to int.
	Starts from 0, no negative values
*/

func (be InmemBackend) Incr(key []byte, value uint) (int, error) {
	return be.Increment(key, int(value), false)
}

/*
   Decr data, yields error if the represented value doesnt maps to int.
   Stops at 0, no negative values
*/
func (be InmemBackend) Decr(key []byte, value uint) (int, error) {
	return be.Increment(key, int(value)*-1, false)
}

/*
	Increment - Generic get and set for incr/decr tx
*/

func (be InmemBackend) Increment(key []byte, incrValue int, createIfNotExists bool) (int, error) {
	be.memMutex.Lock()
	defer be.memMutex.Unlock()

	var value interface{}
	var found bool

	if !createIfNotExists {
		if value, found = be.cache.Get(key); !found {
			return -1, fmt.Errorf("Key %s do not exists, createIfNotExists set to false", string(key))
		}
	}

	if value == nil {
		be.cache.Add(key, []byte("0"), be.defaultExpiration)
		return 0, nil
	}
	i, err := strconv.Atoi(value.(string))
	if err != nil {
		return -1, fmt.Errorf("Data cannot be incr/decr for key %s - %s", string(key), value.(string))
	}
	i = i + incrValue
	s := fmt.Sprintf("%d", i)
	be.cache.Add(key, []byte(s), be.defaultExpiration)
	return i, nil

}

/*
	Get data for key
*/
func (be InmemBackend) Get(key []byte) ([]byte, error) {
	v, _ := be.cache.Get(key)
	return v.([]byte), nil
}

/*
	Delete key, optional check to see if it exists.
	Returns deleted boolean and error
*/
func (be InmemBackend) Delete(key []byte, onlyIfExists bool) (bool, error) {
	if onlyIfExists == true {
		if _, found := be.cache.Get(key); !found {
			return false, errors.New("Key do not exists")
		}
	}
	be.cache.Remove(key)
	return true, nil
}

/*
   Stats returns db statuses
*/
func (be InmemBackend) Stats() string {
	return ""
}

/*
	GetDbPath returns the filesystem path for the database
*/
func (be InmemBackend) GetDbPath() string {
	return ""
}

/*
   Flush flushes all data from the database, not implemented
*/
func (be InmemBackend) Flush() error { return nil }

/*
   BucketStats implement statuses for db that used the bucket idea (boltdb)
*/
func (be InmemBackend) BucketStats() error { return nil }

func (be InmemBackend) SwitchBucket(bucket string) {}
func (be InmemBackend) Close()                     {}

func (be InmemBackend) Range(key []byte, limit int, from []byte, reverse bool) (map[string][]byte, error) {
	return nil, nil
}
func (be InmemBackend) Put([]byte, []byte, bool, bool) error {
	return nil
}
