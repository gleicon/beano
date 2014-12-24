package main

import (
	"fmt"
	"log"
	"runtime"
)

func main() {
	var cpuinfo string
	if n := runtime.NumCPU(); n > 1 {
		runtime.GOMAXPROCS(n)
		cpuinfo = fmt.Sprintf("%d CPUs", n)
	} else {
		cpuinfo = "1 CPU"
	}
	log.Printf("beano (%s)", cpuinfo)

	vdb := NewKVBoltDBBackend("memcached.db", "memcached", 10000)
	mc := NewMemcachedProtocolServer("127.0.0.1:11211", vdb)
	defer mc.Close()
	mc.Start()
}
