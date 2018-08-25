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
	db      *badger.DB
	dbMutex *sync.RWMutex
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
	kv, _ := badger.Open(opt)
	b := badgerBackend{db: kv, dirname: dirname}
	return &b, nil
}

func (be badgerBackend) NormalizedGet(key []byte) ([]byte, error) {
	var item *badger.Item
	err := be.db.View(func(txn *badger.Txn) error {
		var err error
		item, err = txn.Get(key)
		return err
	})
	if err != nil {
		return nil, err
	}
	return item.Value() // []byte value, error
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

	txn := be.db.NewTransaction(true)
	defer txn.Discard()

	item, err := txn.Get(key)

	if err == badger.ErrKeyNotFound && createIfNotExists == false {
		return -1, fmt.Errorf("Key %s do not exists, createIfNotExists set to false - %s", string(key), err)
	}

	if err != nil {
		return 0, err
	}

	itemValue, err := item.Value()

	if err != nil {
		return 0, err
	}

	i, err := strconv.Atoi(string(itemValue))
	if err != nil {
		return -1, fmt.Errorf("Data cannot be incr/decr for key %s - %s", string(key), string(itemValue))
	}

	i = i + value
	s := fmt.Sprintf("%d", i)
	err = txn.Set(key, []byte(s))
	if err != nil {
		return -1, fmt.Errorf("Error key %s - %s", string(key), err)
	}

	if err := txn.Commit(nil); err != nil {
		return 0, err
	}

	return i, nil

}

/*
Put data checking if it should be replaced or exists. Passthru enforces the replacement check
*/
func (be badgerBackend) Put(key []byte, value []byte, replace bool, passthru bool) error {
	be.dbMutex.Lock()
	defer be.dbMutex.Unlock()

	err := be.db.Update(func(txn *badger.Txn) error {
		keyExists := true

		_, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			keyExists = false
		}

		if err != nil {
			return err
		}
		if passthru == false {
			if replace == true {
				if !keyExists {
					return fmt.Errorf("Key %s do not exists, replace set to true - %s", string(key), err)
				}
			} else {
				if keyExists {
					return fmt.Errorf("Key %s exists, replace set to false - %s", string(key), err)
				}
			}
		}

		err = txn.Set(key, value)
		return err
	})

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
	itrOpt := badger.DefaultIteratorOptions
	itrOpt.PrefetchSize = 100
	itrOpt.Reverse = reverse

	err := be.db.View(func(txn *badger.Txn) error {
		itr := txn.NewIterator(itrOpt)
		counter = 0
		for itr.Rewind(); itr.ValidForPrefix(keyPrefix); itr.Next() {
			item := itr.Item()
			k := string(item.Key())
			v, err := item.Value()
			if err != nil {
				return err
			}
			ret[k] = make([]byte, len(v))
			copy(ret[k], v)
			if limit >= 0 && counter == limit {
				break
			}
			counter++
		}

		return nil
	})

	return ret, err
}

/*
Delete key, optional check to see if it exists.
Returns deleted boolean and error
*/
func (be badgerBackend) Delete(key []byte, onlyIfExists bool) (bool, error) {
	be.dbMutex.Lock()
	defer be.dbMutex.Unlock()

	err := be.db.Update(func(txn *badger.Txn) error {
		// enforces deletion only if the key exists

		if onlyIfExists == true {
			keyExists := true
			_, err := txn.Get(key)
			if err == badger.ErrKeyNotFound {
				keyExists = false
			}

			if err != nil {
				return err
			}
			if !keyExists {
				return fmt.Errorf("DELETE: key %s doesn't exist", key)
			}

		}
		err := txn.Delete(key)

		return err
	})

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
