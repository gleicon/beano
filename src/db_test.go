package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var vdb *KVBoltDBBackend

func init() {
	vdb = NewKVBoltDBBackend("bolt.db", "memcached", 10000)
	rand.Seed(time.Now().UTC().UnixNano())
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func errUnexpected(msg interface{}) string {
	return fmt.Sprintf("Unexpected response: %#v\n", msg)
}

func TestDelete(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vdb.Set(key, value)
	vdb.Delete(key)
	if v, err := vdb.Get(key); err != nil {
		t.Error(err)
	} else if v != nil {
		t.Error(errUnexpected(v))
	}
	vdb.Delete(key)
}

func TestSet(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vdb.Delete(key)
	vdb.Set(key, value)
	if v, err := vdb.Get(key); err != nil {
		t.Error(err)
	} else if v == nil {
		t.Error(errUnexpected(v))
	}
	vdb.Delete(key)
}

func TestGet(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vdb.Delete(key)
	if v, err := vdb.Get(key); err != nil {
		t.Error(err)
	} else if v != nil {
		t.Error(errUnexpected(v))
	}

	vdb.Set(key, value)
	if v, err := vdb.Get(key); err != nil {
		t.Error(err)
	} else if v == nil {
		t.Error(errUnexpected(v))
	}
	vdb.Delete(key)
}

func TestAdd(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vdb.Delete(key)

	vdb.Add(key, value)
	err := vdb.Add(key, value)
	if err == nil {
		t.Error(err)
	}
	vdb.Delete(key)
}

func TestReplace(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	newvalue := []byte("eric")
	vdb.Delete(key)

	vdb.Add(key, value)
	vdb.Replace(key, newvalue)
	if v, err := vdb.Get(key); err != nil {
		t.Error(err)
	} else if string(v) != "eric" {
		t.Error(errUnexpected(string(v)))
	}
	vdb.Delete(key)
}

func TestIncr(t *testing.T) {
	key := []byte("beano")
	value := []byte("10")
	vdb.Delete(key)

	vdb.Set(key, value)
	v, err := vdb.Incr(key, 1)
	if err != nil {
		t.Error(err)
	} else if v != 11 {
		t.Error(errUnexpected(v))
	}

	if v, err := vdb.Get(key); err != nil {
		t.Error(err)
	} else if string(v) != "11" {
		t.Error(errUnexpected(string(v)))
	}
	vdb.Delete(key)
}

func TestDecr(t *testing.T) {
	key := []byte("beano")
	value := []byte("10")
	vdb.Delete(key)

	vdb.Set(key, value)
	v, err := vdb.Decr(key, 1)

	if err != nil {
		t.Error(err)
	} else if v != 9 {
		t.Error(errUnexpected(string(v)))
	}

	if v, err := vdb.Get(key); err != nil {
		t.Error(err)
	} else if string(v) != "9" {
		t.Error(errUnexpected(string(v)))
	}
	vdb.Delete(key)
}

func TestFlush(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vdb.Delete(key)
	vdb.Set(key, value)
	if v, err := vdb.Get(key); err != nil {
		t.Error(err)
	} else if v == nil {
		t.Error(errUnexpected(v))
	}
	vdb.Flush()
	if v, err := vdb.Get(key); err != nil {
		t.Error(err)
	} else if v != nil {
		t.Error(errUnexpected(v))
	}
}
