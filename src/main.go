package main

import (
	"fmt"
	"github.com/davecheney/profile"
	"log"
	"runtime"
)

func main() {
	c := profile.Config{BlockProfile: true, CPUProfile: true, ProfilePath: "/tmp", MemProfile: true, Quiet: false}
	defer profile.Start(&c).Stop()
	var cpuinfo string
	if n := runtime.NumCPU(); n > 1 {
		runtime.GOMAXPROCS(n)
		cpuinfo = fmt.Sprintf("%d CPUs", n)
	} else {
		cpuinfo = "1 CPU"
	}
	log.Printf("beano (%s)", cpuinfo)

	vdb, err := NewKVDBBackend("memcached.db")
	if err != nil {
		log.Printf("Error opening db: %s\n", err)
	}
	mc := NewMemcachedProtocolServer("127.0.0.1:11211", vdb)
	defer mc.Close()
	mc.Start()
}
