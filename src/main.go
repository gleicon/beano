package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	logging "github.com/op/go-logging"
	"github.com/pkg/profile"
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
	address := flag.String("s", "127.0.0.1", "Bind Address")
	port := flag.String("p", "11211", "Bind Port")
	filename := flag.String("f", "./memcached.db", "path and file for database")
	pf := flag.Bool("q", false, "Enable profiling")

	flag.Usage = func() {
		fmt.Println("Usage: beano [-s ip] [-p port] [-f /path/to/db/file -q]")
		fmt.Println("default ip: 127.0.0.1")
		fmt.Println("default port: 11211")
		fmt.Println("default file: ./memcached.db")
		fmt.Println("-q enables profiling to /tmp/*.prof")
		os.Exit(1)
	}
	flag.Parse()
	if *pf == true {
		c := profile.Start(profile.CPUProfile, profile.ProfilePath("/tmp"), profile.NoShutdownHook)
		defer c.Stop()
	}
	var cpuinfo string
	if n := runtime.NumCPU(); n > 1 {
		runtime.GOMAXPROCS(n)
		cpuinfo = fmt.Sprintf("%d CPUs", n)
	} else {
		cpuinfo = "1 CPU"
	}

	log.Info("beano (%s)", cpuinfo)

	initializeMetrics(*filename)

	serve(*address, *port, *filename)

}
