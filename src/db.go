package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	bloom "github.com/pmylund/go-bloom"
	"strconv"
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

type KVBoltDBBackend struct {
	filename         string
	bucketName       string
	db               *bolt.DB
	expirationdb     *bolt.DB
	bloomFilter      map[string]*bloom.CountingFilter
	maxKeysPerBucket int
}

func NewKVBoltDBBackend(filename string, bucketName string, maxKeysPerBucket int) *KVBoltDBBackend {
	var err error
	b := KVBoltDBBackend{filename, bucketName, nil, nil, nil, maxKeysPerBucket}
	b.db, err = bolt.Open(filename, 0644, nil)
	if err != nil {
		return nil
	}
	b.bloomFilter = make(map[string]*bloom.CountingFilter)
	b.bloomFilter[bucketName] = bloom.NewCounting(maxKeysPerBucket, 0.01)
	return &b
}

func (be KVBoltDBBackend) Set(key []byte, value []byte) error {
	return be.Put(key, value, true)
}

// store data only if the server doesnt holds it yet
func (be KVBoltDBBackend) Add(key []byte, value []byte) error {
	return be.Put(key, value, false)
}

// store data only if the server already holds this key
func (be KVBoltDBBackend) Replace(key []byte, value []byte) error {
	return be.Put(key, value, true)
}

// INCR data, yields error if the represented value doesnt maps to int. Starts from 0, no negative values
func (be KVBoltDBBackend) Incr(key []byte, value uint) (int, error) {
	return be.Increment(key, int(value), false)
}

// DECR data, yields error if the represented value doesnt maps to int. Stops at 0, no negative values
func (be KVBoltDBBackend) Decr(key []byte, value uint) (int, error) {
	return be.Increment(key, int(value)*-1, false)
}

// Generic get and set for incr/decr tx
func (be KVBoltDBBackend) Increment(key []byte, value int, create_if_not_exists bool) (int, error) {
	var ret int
	err := be.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(be.bucketName))

		if err != nil {
			return err
		}

		if be.bloomFilter[be.bucketName].Test(key) == false {
			if create_if_not_exists == false {
				return fmt.Errorf("Increment: Key %s exists", string(key))
			}
			i := string(0 + value)
			err := bucket.Put(key, []byte(i))
			if err != nil {
				return fmt.Errorf("Error storing incr/decr value for key %s - %d", string(key), i)
			}
			ret = 0 + value
		} else {
			v := bucket.Get(key)
			i, err := strconv.Atoi(string(v))
			if err != nil {
				return fmt.Errorf("Data cannot be incr/decr for key %s - %s", string(key), string(v))
			}
			i = i + value
			s := fmt.Sprintf("%d", i)
			err = bucket.Put(key, []byte(s))
			if err != nil {
				return fmt.Errorf("Error storing incr/decr value for key %s - %d", string(key), i)
			}
			ret = i
		}
		return nil
	})
	return ret, err
}

func (be KVBoltDBBackend) Put(key []byte, value []byte, override bool) error {
	err := be.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(be.bucketName))

		if err != nil {
			return err
		}

		// check if item exists at two levels, provided override is true
		if override == false {
			if be.bloomFilter[be.bucketName].Test(key) == true {
				if bucket.Get(key) != nil {
					return fmt.Errorf("Key %s already exists, override set to false", string(key))
				}
			}
		}

		be.bloomFilter[be.bucketName].Add(key)
		err = bucket.Put(key, value)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func (be KVBoltDBBackend) Get(key []byte) ([]byte, error) {
	var val []byte
	if be.bloomFilter[be.bucketName].Test(key) == false {
		return nil, nil
	}
	err := be.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(be.bucketName))
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", be.bucketName)
		}

		val = bucket.Get(key)
		return nil
	})

	if err != nil {
		return nil, err
	}
	return val, nil

}

func (be KVBoltDBBackend) Delete(key []byte) error {
	be.db.Update(func(tx *bolt.Tx) error {
		be.bloomFilter[be.bucketName].Remove(key)
		return tx.Bucket([]byte(be.bucketName)).Delete(key)
	})
	return nil
}

func (be KVBoltDBBackend) Flush() error {
	be.db.Update(func(tx *bolt.Tx) error {
		be.bloomFilter[be.bucketName].Reset()
		return tx.Bucket([]byte(be.bucketName)).DeleteBucket([]byte("foo")) // TODO: flush properly
	})
	return nil
}

func (be KVBoltDBBackend) Stats() error       { return nil }
func (be KVBoltDBBackend) BucketStats() error { return nil }
func (be KVBoltDBBackend) CloseDB() {
	be.db.Close()
}

func (be KVBoltDBBackend) SwitchBucket(bucket string) {
	if be.bloomFilter[bucket] == nil {
		be.bloomFilter[bucket] = bloom.NewCounting(be.maxKeysPerBucket, 0.01)
	}
	be.bucketName = bucket
}
