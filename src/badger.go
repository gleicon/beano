package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/dgraph-io/badger"
)

/*
KVDBBackend is the KeyValue DB abstraction. Contains a Mutex to coordinate
file changes
*/
type badgerBackend struct {
	dirname string
	db      *badger.KV
	dbMutex sync.RWMutex
}

/*
NewbadgerBackend receives a dirname with path and creates a new Backend instance
*/
func NewBadgerBackend(dirname string) (*badgerBackend, error) {
	var err error
	info, err := os.Stat(dirname)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("Directory not found" + err.Error())
	}
	opt := badger.DefaultOptions
	opt.Dir = dirname
	opt.ValueDir = dirname
	kv, _ := badger.NewKV(&opt)
	b := badgerBackend{db: kv, dirname: dirname}
	return &b, nil
}

func (be badgerBackend) NormalizedGet(key []byte) ([]byte, error) {
	var item badger.KVItem
	err := be.db.Get(key, &item)
	if item.Value() == nil { // Not Found
		return nil, nil
	}
	return item.Value(), err
}

/*
Set the value for key
*/
func (be badgerBackend) Set(key []byte, value []byte) error {
	return be.Put(key, value, false, true)
}

/*
Add value to key, store data only if the server doesnt holds it yet
*/
func (be badgerBackend) Add(key []byte, value []byte) error {
	return be.Put(key, value, false, false)
}

/*
Replace value for key, store data only if the server already holds this key
*/
func (be badgerBackend) Replace(key []byte, value []byte) error {
	return be.Put(key, value, true, false)
}

/*
Incr data, yields error if the represented value doesnt maps to int.
Starts from 0, no negative values
*/
func (be badgerBackend) Incr(key []byte, value uint) (int, error) {
	return be.Increment(key, int(value), false)
}

/*
Decr data, yields error if the represented value doesnt maps to int.
Stops at 0, no negative values
*/
func (be badgerBackend) Decr(key []byte, value uint) (int, error) {
	return be.Increment(key, int(value)*-1, false)
}

/*
Increment - Generic get and set for incr/decr tx
*/
func (be badgerBackend) Increment(key []byte, value int, createIfNotExists bool) (int, error) {
	be.dbMutex.Lock()
	defer be.dbMutex.Unlock()
	v, err := be.NormalizedGet(key)
	if createIfNotExists == false {
		if v == nil || err != nil {
			return -1, fmt.Errorf("Key %s do not exists, createIfNotExists set to false - %s", string(key), err)
		}
	}
	if v == nil {
		err = be.db.Set(key, []byte("0"))
		return 0, nil
	}
	i, err := strconv.Atoi(string(v))
	if err != nil {
		return -1, fmt.Errorf("Data cannot be incr/decr for key %s - %s", string(key), string(v))
	}
	i = i + value
	s := fmt.Sprintf("%d", i)
	err = be.db.Set(key, []byte(s))
	if err != nil {
		return -1, fmt.Errorf("Error key %s - %s", string(key), err)
	}
	return i, nil

}

/*
Put data checking if it should be replaced or exists. Generic method
*/
func (be badgerBackend) Put(key []byte, value []byte, replace bool, passthru bool) error {
	be.dbMutex.Lock()
	defer be.dbMutex.Unlock()
	if passthru == false {
		if replace == true {
			v, err := be.NormalizedGet(key)
			if v == nil || err != nil {
				return fmt.Errorf("Key %s do not exists, replace set to true - %s", string(key), err)
			}
		} else {
			v, err := be.NormalizedGet(key)
			if v != nil {
				return fmt.Errorf("Key %s exists, replace set to false - %s", string(key), err)
			}
		}
	}

	err := be.db.Set(key, value)
	return err
}

/*
Get data for key
*/
func (be badgerBackend) Get(key []byte) ([]byte, error) {
	be.dbMutex.RLock()
	defer be.dbMutex.RUnlock()
	v, err := be.NormalizedGet(key)
	return v, err
}

/*
Range query by key prefix. If limit == -1 no limit is applyed. Take care
*/
func (be badgerBackend) Range(keyPrefix []byte, limit int, from []byte, reverse bool) (map[string][]byte, error) {
	be.dbMutex.RLock()
	defer be.dbMutex.RUnlock()
	var counter int

	ret := make(map[string][]byte)

	itrOpt := badger.IteratorOptions{
		PrefetchSize: 100, // conservative, should be configurable
		FetchValues:  true,
		Reverse:      reverse,
	}

	itr := be.db.NewIterator(itrOpt)

	counter = 0
	for itr.Rewind(); itr.ValidForPrefix(keyPrefix); itr.Next() {
		item := itr.Item()
		k := string(item.Key())
		v := item.Value()
		ret[k] = make([]byte, len(v))
		copy(ret[k], v)
		if limit >= 0 && counter == limit {
			break
		}
		counter++
	}

	return ret, nil
}

/*
Delete key, optional check to see if it exists.
Returns deleted boolean and error
*/
func (be badgerBackend) Delete(key []byte, onlyIfExists bool) (bool, error) {
	be.dbMutex.Lock()
	defer be.dbMutex.Unlock()

	if onlyIfExists == true {
		x, err := be.NormalizedGet(key)
		if err != nil {
			return false, err
		}
		if x == nil {
			return false, nil
		}
	}
	err := be.db.Delete(key)
	return true, err
}

/*
Close database
*/
func (be badgerBackend) Close() {
	be.db.Close()
}

/*
Stats returns db statuses
*/
func (be badgerBackend) Stats() string { return "" }

/*
GetDbPath returns the filesystem path for the database
*/
func (be badgerBackend) GetDbPath() string {
	return be.dirname
}

/*
Flush flushes all data from the database, not implemented
*/
func (be badgerBackend) Flush() error { return nil }

/*
BucketStats implement statuses for db that used the bucket idea (boltdb)
*/
func (be badgerBackend) BucketStats() error { return nil }
