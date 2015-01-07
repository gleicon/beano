package main

import (
	"fmt"
	"github.com/davecheney/profile"
	logging "github.com/op/go-logging"
	"os"
	"runtime"
)

func setLogger() *logging.Logger {
	var log = logging.MustGetLogger("beano")
	var format = logging.MustStringFormatter(
		"%{color}%{time:15:04:05.000000} %{level:.5s} %{id:04d}%{color:reset} %{message}",
	)
	var logBackend = logging.NewLogBackend(os.Stdout, "", 0)
	//	bel := logging.AddModuleLevel(logBackend)
	//	bel.SetLevel(logging.ERROR, "")
	var bf = logging.NewBackendFormatter(logBackend, format)
	logging.SetBackend(bf)
	return log
}

var log = setLogger()

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

	log.Info("beano (%s)", cpuinfo)

	vdb, err := NewKVDBBackend("memcached.db")

	if err != nil {
		log.Error("Error opening db: %s\n", err)
	}

	initializeMetrics(vdb.GetDbPath())

	mc := NewMemcachedProtocolServer("127.0.0.1:11211", vdb)

	defer mc.Close()
	mc.Start()
}
