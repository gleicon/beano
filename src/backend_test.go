package main

import (
	"fmt"
	"math/rand"
	"time"
)

var vboltdb *KVBoltDBBackend
var vleveldb *LevelDBBackend

func init() {
	vleveldb, _ = NewLevelDBBackend("test_leveldb_beano.db")
	vboltdb, _ = NewKVBoltDBBackend("bolt.db", "memcached", 10000)
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
