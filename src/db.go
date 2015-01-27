package main

import (
	"fmt"
	"github.com/jmhodges/levigo"
	"strconv"
	"sync"
)

/*
InternalValue is a tentative representation of memcached data
*/
type InternalValue struct {
	key        []byte
	flags      int32
	expiration int
	cas        int64
	value      []byte
}

/*
KVDBBackend is the KeyValue DB abstraction. Contains a Mutex to coordinate
file changes
*/
type KVDBBackend struct {
	filename string
	db       *levigo.DB
	ro       *levigo.ReadOptions
	wo       *levigo.WriteOptions
	dbMutex  sync.RWMutex
}

/*
NewKVDBBackend receives a filename with path and creates a new Backend instance
*/
func NewKVDBBackend(filename string) (*KVDBBackend, error) {
	var err error

	b := KVDBBackend{db: nil}
	b.filename = filename
	opts := levigo.NewOptions()
	filter := levigo.NewBloomFilter(32)
	opts.SetFilterPolicy(filter)
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)
	b.db, err = levigo.Open(filename, opts)
	b.ro = levigo.NewReadOptions()
	b.wo = levigo.NewWriteOptions()

	if err != nil {
		return nil, err
	}
	return &b, nil
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
	v, err := be.db.Get(be.ro, key)
	if createIfNotExists == false {
		if v == nil || err != nil {
			be.dbMutex.Unlock()
			return -1, fmt.Errorf("Key %s do not exists, createIfNotExists set to false - %s", string(key), err)
		}
	}
	if v == nil {
		err = be.db.Put(be.wo, key, []byte("0"))
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
	err = be.db.Put(be.wo, key, []byte(s))
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
	if passthru == false {
		if replace == true {
			v, err := be.db.Get(be.ro, key)
			if v == nil || err != nil {
				be.dbMutex.Unlock()
				return fmt.Errorf("Key %s do not exists, replace set to true - %s", string(key), err)
			}
		} else {
			v, err := be.db.Get(be.ro, key)
			if v != nil {
				be.dbMutex.Unlock()
				return fmt.Errorf("Key %s exists, replace set to false - %s", string(key), err)
			}
		}
	}
	err := be.db.Put(be.wo, key, value)
	be.dbMutex.Unlock()
	return err
}

/*
Get data for key
*/
func (be KVDBBackend) Get(key []byte) ([]byte, error) {
	ro := levigo.NewReadOptions()
	be.dbMutex.RLock()
	v, err := be.db.Get(ro, key)
	be.dbMutex.RUnlock()
	return v, err
}

/*
Range query by key prefix. If limit == -1 no limit is applyed. Take care
*/
func (be KVDBBackend) Range(key []byte, limit int) (map[string][]byte, error) {
	ret := make(map[string][]byte)
	ro := levigo.NewReadOptions()
	be.dbMutex.RLock()
	it := be.db.NewIterator(ro)
	defer it.Close()
	it.Seek(key)
	l := 0
	for it = it; it.Valid(); it.Next() {
		ret[string(it.Key())] = it.Value()
		l++
		if limit >= 0 && limit == l {
			break
		}
	}
	err := it.GetError()
	if err != nil {
		be.dbMutex.RUnlock()
		return nil, fmt.Errorf("Error iterating: %s", string(key))
	}
	be.dbMutex.RUnlock()
	return ret, nil
}

/*
Delete key, optional check to see if it exists.
Returns deleted boolean and error
*/
func (be KVDBBackend) Delete(key []byte, onlyIfExists bool) (bool, error) {
	be.dbMutex.Lock()
	if onlyIfExists == true {
		x, err := be.db.Get(be.ro, key)
		if err != nil {
			be.dbMutex.Unlock()
			return false, err
		}
		if x == nil {
			be.dbMutex.Unlock()
			return false, nil
		}
	}
	err := be.db.Delete(be.wo, key)
	be.dbMutex.Unlock()
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
	return be.db.PropertyValue("leveldb.stats")
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
