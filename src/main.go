package main

import (
	"flag"
	"fmt"
	"os"

	logging "github.com/op/go-logging"
	"github.com/pkg/profile"
)

func setLogger() *logging.Logger {
	var log = logging.MustGetLogger("beano")
	var format = logging.MustStringFormatter(
		"%{color}%{time:15:04:05.000000} %{level:.5s} %{id:04d}%{color:reset} %{message}",
	)
	var logBackend = logging.NewLogBackend(os.Stdout, "", 0)
	var bf = logging.NewBackendFormatter(logBackend, format)
	logging.SetBackend(bf)
	return log
}

var log = setLogger()

func main() {
	address := flag.String("s", "127.0.0.1", "Bind Address")
	port := flag.String("p", "11211", "Bind Port")
	filename := flag.String("f", "./memcached.db", "path and file for database. for badger it needs to be a directory")
	backend := flag.String("b", "leveldb", "backend: leveldb, boltdb, inmem or badger")
	pf := flag.Bool("q", false, "Enable profiling")
	dumpLogs := flag.Bool("m", false, "Enable metric dump each 60 seconds")

	flag.Usage = func() {
		fmt.Println("Usage: beano [-s ip] [-p port] [-f /path/to/db/file -q -b leveldb|boltdb|inmem|badger]")
		fmt.Println("default ip: 127.0.0.1")
		fmt.Println("default port: 11211")
		fmt.Println("default backend: leveldb")
		fmt.Println("default file: ./memcached.db")
		fmt.Println("-q enables profiling to /tmp/*.prof")
		os.Exit(1)
	}
	flag.Parse()
	if *pf == true {
		c := profile.Start(profile.CPUProfile, profile.ProfilePath("/tmp"), profile.NoShutdownHook)
		defer c.Stop()
	}

	log.Info("Beano backend: %s | db file: %s", *backend, *filename)

	initializeMetrics(*filename, *dumpLogs)

	serve(*address, *port, *filename, *backend)

}
