package main

import (
	"fmt"
	"github.com/jmhodges/levigo"
)

/*

bucket design
given bucket BUCKET:
    - BUCKET:DATA is key/value data with denormalized data
    - BUCKET:EXP pointer to expiration + creation time
    - BUCKET:CAS cas to key
    - BUCKET:FLAG key flags ?
The quick search over existing key is done thru counting bloom filter

Expiration ideas:
Expiration worker keeps in memory list of next expirations (queue deletes to BUCKET:DATA + bloom filter almost realtime, the others when it can)
crontab like expirator ?
expiration cache before get ?

Q: any gains by dividing buckets in files ?

*/

/* what to implement from memcached:
https://github.com/memcached/memcached/blob/master/doc/protocol.txt
- GET/SET/ADD/INCR/DECR/STAT
- passthru flags
- opaque cas ?
- expiration ?
*/

/*
The "standard protocol stuff" of memcached involves running a command against an "item". An item consists of:

A key (arbitrary string up to 250 bytes in length. No space or newlines for ASCII mode)
A 32bit "flag" value
An expiration time, in seconds. Can be up to 30 days. After 30 days, is treated as a unix timestamp of an exact date.
A 64bit "CAS" value, which is kept unique.
Arbitrary data

*/

type InternalValue struct {
	key        []byte
	flags      int32
	expiration int
	cas        int64
	value      []byte
}

type KVDBBackend struct {
	filename         string
	db               *levigo.DB
	maxKeysPerBucket int
	ro               *levigo.ReadOptions
	wo               *levigo.WriteOptions
}

func NewKVDBBackend(filename string, bucketName string, maxKeysPerBucket int) *KVDBBackend {
	var err error

	b := KVDBBackend{filename: filename, db: nil, maxKeysPerBucket: maxKeysPerBucket}
	opts := levigo.NewOptions()
	filter := levigo.NewBloomFilter(128)
	opts.SetFilterPolicy(filter)
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)
	b.db, err = levigo.Open(filename, opts)
	b.ro = levigo.NewReadOptions()
	b.wo = levigo.NewWriteOptions()

	if err != nil {
		return nil
	}
	return &b
}

func (be KVDBBackend) Set(key []byte, value []byte) error {
	return be.Put(key, value, false, true)
}

// store data only if the server doesnt holds it yet
func (be KVDBBackend) Add(key []byte, value []byte) error {
	return be.Put(key, value, false, false)
}

// store data only if the server already holds this key
func (be KVDBBackend) Replace(key []byte, value []byte) error {
	return be.Put(key, value, true, false)
}

// INCR data, yields error if the represented value doesnt maps to int. Starts from 0, no negative values
func (be KVDBBackend) Incr(key []byte, value uint) (int, error) {
	return be.Increment(key, int(value), false)
}

// DECR data, yields error if the represented value doesnt maps to int. Stops at 0, no negative values
func (be KVDBBackend) Decr(key []byte, value uint) (int, error) {
	return be.Increment(key, int(value)*-1, false)
}

// Generic get and set for incr/decr tx
func (be KVDBBackend) Increment(key []byte, value int, create_if_not_exists bool) (int, error) {
	return 0, nil
}

func (be KVDBBackend) Put(key []byte, value []byte, replace bool, passthru bool) error {
	if passthru == false {
		if replace == true {
			v, err := be.db.Get(be.ro, key)
			if v == nil || err != nil {
				return fmt.Errorf("Key %s do not exists, replace set to true - %s", string(key), err)
			}
		} else {
			v, err := be.db.Get(be.ro, key)
			if v != nil {
				return fmt.Errorf("Key %s exists, replace set to false - %s", string(key), err)
			}
		}
	}
	err := be.db.Put(be.wo, key, value)
	return err
}

func (be KVDBBackend) Get(key []byte) ([]byte, error) {
	v, err := be.db.Get(be.ro, key)
	return v, err
}

// returns deleted, error
func (be KVDBBackend) Delete(key []byte, only_if_exists bool) (bool, error) {
	if only_if_exists == true {
		x, err := be.db.Get(be.ro, key)
		if err != nil {
			return false, err
		}
		if x == nil {
			return false, nil
		}
	}
	err := be.db.Delete(be.wo, key)
	return true, err
}

func (be KVDBBackend) CloseDB() {
	be.db.Close()
}

func (be KVDBBackend) Flush() error       { return nil }
func (be KVDBBackend) Stats() error       { return nil }
func (be KVDBBackend) BucketStats() error { return nil }
