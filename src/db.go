package main

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

/*
KVDBBackend is the KeyValue DB abstraction. Contains a Mutex to coordinate
file changes
*/
type KVDBBackend struct {
	filename string
	db       *leveldb.DB
	ro       *opt.ReadOptions
	wo       *opt.WriteOptions
	dbMutex  sync.RWMutex
}

/*
NewKVDBBackend receives a filename with path and creates a new Backend instance
*/
func NewKVDBBackend(filename string) (*KVDBBackend, error) {
	var err error

	b := KVDBBackend{db: nil, ro: nil, wo: nil}
	b.filename = filename
	opts := opt.Options{
		Filter: filter.NewBloomFilter(32),
	}

	b.db, err = leveldb.OpenFile(filename, &opts)
	b.ro = new(opt.ReadOptions)
	b.wo = new(opt.WriteOptions)

	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (be KVDBBackend) NormalizedGet(key []byte, ro *opt.ReadOptions) ([]byte, error) {
	v, err := be.db.Get(key, be.ro)
	// impedance mismatch w/ levigo: v should be nil, err should be nil for key not found
	if err == leveldb.ErrNotFound {
		err = nil
		v = nil
	}
	return v, err
}

/*
Set the value for key
*/
func (be KVDBBackend) Set(key []byte, value []byte) error {
	return be.Put(key, value, false, true)
}

/*
Add value to key, store data only if the server doesnt holds it yet
*/
func (be KVDBBackend) Add(key []byte, value []byte) error {
	return be.Put(key, value, false, false)
}

/*
Replace value for key, store data only if the server already holds this key
*/
func (be KVDBBackend) Replace(key []byte, value []byte) error {
	return be.Put(key, value, true, false)
}

/*
Incr data, yields error if the represented value doesnt maps to int.
Starts from 0, no negative values
*/
func (be KVDBBackend) Incr(key []byte, value uint) (int, error) {
	return be.Increment(key, int(value), false)
}

/*
Decr data, yields error if the represented value doesnt maps to int.
Stops at 0, no negative values
*/
func (be KVDBBackend) Decr(key []byte, value uint) (int, error) {
	return be.Increment(key, int(value)*-1, false)
}

/*
Increment - Generic get and set for incr/decr tx
*/
func (be KVDBBackend) Increment(key []byte, value int, createIfNotExists bool) (int, error) {
	be.dbMutex.Lock()
	v, err := be.NormalizedGet(key, be.ro)
	if createIfNotExists == false {
		if v == nil || err != nil {
			be.dbMutex.Unlock()
			return -1, fmt.Errorf("Key %s do not exists, createIfNotExists set to false - %s", string(key), err)
		}
	}
	if v == nil {
		err = be.db.Put(key, []byte("0"), be.wo)
		be.dbMutex.Unlock()
		return 0, nil
	}
	i, err := strconv.Atoi(string(v))
	if err != nil {
		be.dbMutex.Unlock()
		return -1, fmt.Errorf("Data cannot be incr/decr for key %s - %s", string(key), string(v))
	}
	i = i + value
	s := fmt.Sprintf("%d", i)
	err = be.db.Put(key, []byte(s), be.wo)
	if err != nil {
		be.dbMutex.Unlock()
		return -1, fmt.Errorf("Error key %s - %s", string(key), err)
	}
	be.dbMutex.Unlock()
	return i, nil

}

/*
Put data checking if it should be replaced or exists. Generic method
*/
func (be KVDBBackend) Put(key []byte, value []byte, replace bool, passthru bool) error {
	be.dbMutex.Lock()
	defer be.dbMutex.Unlock()
	if passthru == false {
		if replace == true {
			v, err := be.NormalizedGet(key, be.ro)
			if v == nil || err != nil {
				be.dbMutex.Unlock()
				return fmt.Errorf("Key %s do not exists, replace set to true - %s", string(key), err)
			}
		} else {
			v, err := be.NormalizedGet(key, be.ro)
			if v != nil {
				be.dbMutex.Unlock()
				return fmt.Errorf("Key %s exists, replace set to false - %s", string(key), err)
			}
		}
	}

	err := be.db.Put(key, value, be.wo)
	return err
}

/*
Get data for key
*/
func (be KVDBBackend) Get(key []byte) ([]byte, error) {
	be.dbMutex.RLock()
	defer be.dbMutex.RUnlock()
	v, err := be.NormalizedGet(key, be.ro)
	return v, err
}

/*
Range query by key prefix. If limit == -1 no limit is applyed. Take care
*/
func (be KVDBBackend) Range(key []byte, limit int, from []byte, reverse bool) (map[string][]byte, error) {
	be.dbMutex.RLock()
	defer be.dbMutex.RUnlock()

	var f func() bool
	ret := make(map[string][]byte)

	it := be.db.NewIterator(util.BytesPrefix(key), be.ro)

	if reverse == true {
		it.Last()
	}

	if from != nil {
		it.Seek(from)
	}

	if reverse == true {
		f = it.Prev
	} else {
		f = it.Next
	}

	for l := 1; f(); l++ {
		k := string(it.Key())
		ret[k] = make([]byte, len(it.Value()))
		copy(ret[k], it.Value())
		if limit >= 0 && limit == l {
			break
		}
	}

	it.Release()
	err := it.Error()
	if err != nil {
		return nil, fmt.Errorf("Error iterating: %s", string(key))
	}
	return ret, nil
}

/*
Delete key, optional check to see if it exists.
Returns deleted boolean and error
*/
func (be KVDBBackend) Delete(key []byte, onlyIfExists bool) (bool, error) {
	be.dbMutex.Lock()
	defer be.dbMutex.Unlock()

	if onlyIfExists == true {
		x, err := be.NormalizedGet(key, be.ro)
		if err != nil {
			return false, err
		}
		if x == nil {
			return false, nil
		}
	}
	err := be.db.Delete(key, be.wo)
	return true, err
}

/*
Close database
*/
func (be KVDBBackend) Close() {
	be.db.Close()
}

/*
Stats returns db statuses
*/
func (be KVDBBackend) Stats() string {
	s, _ := be.db.GetProperty("leveldb.stats")
	return s
}

/*
GetDbPath returns the filesystem path for the database
*/
func (be KVDBBackend) GetDbPath() string {
	return be.filename
}

/*
Flush flushes all data from the database, not implemented
*/
func (be KVDBBackend) Flush() error { return nil }

/*
BucketStats implement statuses for db that used the bucket idea (boltdb)
*/
func (be KVDBBackend) BucketStats() error { return nil }
