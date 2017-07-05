package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/facebookgo/inmem"
)

type InmemBackend struct {
	size int
	data inmem.Cache
}

func NewInmemBackend(size int) (*InmemBackend, error) {
	dd := inmem.NewLocked(size)
	b := InmemBackend{size: size, data: dd}
	return &b, nil
}

func (be InmemBackend) Set(key []byte, value []byte) error {
	return be.Put(key, value, false, true)
}

// store data only if the server doesnt holds it yet
func (be InmemBackend) Add(key []byte, value []byte) error {
	return be.Put(key, value, false, false)
}

// store data only if the server already holds this key
func (be InmemBackend) Replace(key []byte, value []byte) error {
	return be.Put(key, value, true, false)
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

// Generic get and set for incr/decr tx
func (be InmemBackend) Increment(key []byte, value int, create_if_not_exists bool) (int, error) {
	return 0, nil
}

func (be InmemBackend) Put(key []byte, value []byte, replace bool, passthru bool) error {
	be.data.Add(string(key), value, time.Now())
	return nil
}

func (be InmemBackend) Get(key []byte) ([]byte, error) {
	r, ok := be.data.Get(string(key))
	if !ok {
		return nil, errors.New("Error getting value from inmem")
	}
	return r.([]byte), nil
}

// returns deleted, error
func (be InmemBackend) Delete(key []byte, only_if_exists bool) (bool, error) {
	if only_if_exists == true {
		x, err := be.Get(key)
		if err != nil {
			return false, err
		}
		if x == nil {
			return false, nil
		}
	}
	be.data.Remove(key)
	return true, nil
}

func (be InmemBackend) Flush() error {
	return nil
}

func (be InmemBackend) BucketStats() error {
	return nil
}

func (be InmemBackend) GetDbPath() string {
	rm := fmt.Sprintf("Inmem database: %d bytes", be.data.Len())
	return rm
}

func (be InmemBackend) SwitchBucket(bucket string) {}
func (be InmemBackend) Range([]byte, int, []byte, bool) (map[string][]byte, error) {
	return nil, nil
}
func (be InmemBackend) Close()        {}
func (be InmemBackend) Stats() string { return "" }
