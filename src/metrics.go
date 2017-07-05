package main

import (
	"expvar"
	"fmt"
	"os"
	"time"

	logging "github.com/op/go-logging"
	"github.com/rcrowley/go-metrics"
)

var pid = expvar.NewInt("pid")
var version = expvar.NewString("version")
var upSince = expvar.NewString("up_since")
var dbPath = expvar.NewString("db_path")

var currItems = metrics.NewCounter()        //"curr_items"
var totalItems = metrics.NewCounter()       //"total_items"
var totalConnections = metrics.NewCounter() //"total_connections"
var totalThreads = metrics.NewCounter()     //"total_threads"
var currThreads = metrics.NewCounter()      //"curr_threads"
var cmdGet = metrics.NewCounter()           //"cmd_get"
var cmdSet = metrics.NewCounter()           //"cmd_set"
var getHits = metrics.NewCounter()          //"get_hits"
var getMisses = metrics.NewCounter()        //"get_misses"
var protocolErrors = metrics.NewCounter()   //"protocol_errors"
var networkErrors = metrics.NewCounter()    //"network_errors"
var readonlyErrors = metrics.NewCounter()   //"readonly_errors"
var responseTiming = metrics.NewTimer()     // response_timing

func initializeMetrics(dbp string, dumpLogs bool) {
	pid.Set(int64(os.Getpid()))
	version.Set("BEANO Server")
	upSince.Set(time.Now().Format(time.RFC3339))
	dbPath.Set(dbp)

	metrics.Register("current_items", currItems)
	metrics.Register("total_items", totalItems)
	metrics.Register("total_connections", totalConnections)
	metrics.Register("total_threads", totalThreads)
	metrics.Register("curr_threads", currThreads)
	metrics.Register("cmd_get", cmdGet)
	metrics.Register("cmd_set", cmdSet)
	metrics.Register("get_hits", getHits)
	metrics.Register("get_misses", getMisses)
	metrics.Register("protocol_errors", protocolErrors)
	metrics.Register("network_errors", networkErrors)
	metrics.Register("readonly_errors", readonlyErrors)
	metrics.Register("response_timing", responseTiming)
	if dumpLogs {
		go metrics.Log(metrics.DefaultRegistry, time.Duration(60*time.Second), logging.NewLogBackend(os.Stdout, "", 0).Logger)
	}
}

func metrics2expvar(r metrics.Registry) {
	du := float64(time.Nanosecond)
	percentiles := []float64{0.50, 0.75, 0.95, 0.99, 0.999}
	r.Each(func(name string, i interface{}) {
		switch m := i.(type) {
		case metrics.Counter:
			f := func() interface{} { return m.Count() }
			expvar.Publish(fmt.Sprintf("%s.counter", name), expvar.Func(f))
		case metrics.Meter:
			r1 := func() interface{} { return m.Rate1() }
			r5 := func() interface{} { return m.Rate5() }
			r15 := func() interface{} { return m.Rate15() }
			rMean := func() interface{} { return m.RateMean() }
			expvar.Publish(fmt.Sprintf("%s.rate.1", name), expvar.Func(r1))
			expvar.Publish(fmt.Sprintf("%s.rate.5", name), expvar.Func(r5))
			expvar.Publish(fmt.Sprintf("%s.rate.15", name), expvar.Func(r15))
			expvar.Publish(fmt.Sprintf("%s.rate.mean", name), expvar.Func(rMean))

		case metrics.Histogram:
			f := func() interface{} { return m.Count() }
			Mean := func() interface{} { return m.Mean() }
			Min := func() interface{} { return m.Min() }
			Max := func() interface{} { return m.Max() }
			StdDev := func() interface{} { return m.StdDev() }
			Variance := func() interface{} { return m.Variance() }
			expvar.Publish(fmt.Sprintf("%s.count", name), expvar.Func(f))
			expvar.Publish(fmt.Sprintf("%s.mean", name), expvar.Func(Mean))
			expvar.Publish(fmt.Sprintf("%s.min", name), expvar.Func(Min))
			expvar.Publish(fmt.Sprintf("%s.max", name), expvar.Func(Max))
			expvar.Publish(fmt.Sprintf("%s.stddev", name), expvar.Func(StdDev))
			expvar.Publish(fmt.Sprintf("%s.variance", name), expvar.Func(Variance))
			for _, p := range percentiles {
				pc := func() interface{} { return m.Percentile(p) }
				expvar.Publish(fmt.Sprintf("%s.percentile.%2.3f", name, p), expvar.Func(pc))
			}
		case metrics.Timer:
			r1 := func() interface{} { return m.Rate1() }
			r5 := func() interface{} { return m.Rate5() }
			r15 := func() interface{} { return m.Rate15() }
			rMean := func() interface{} { return m.RateMean() }
			Mean := func() interface{} { return du * m.Mean() }
			Min := func() interface{} { return int64(du) * m.Min() }
			Max := func() interface{} { return int64(du) * m.Max() }
			Variance := func() interface{} { return du * m.Variance() }
			StdDev := func() interface{} { return du * m.StdDev() }
			expvar.Publish(fmt.Sprintf("%s.rate.1", name), expvar.Func(r1))
			expvar.Publish(fmt.Sprintf("%s.rate.5", name), expvar.Func(r5))
			expvar.Publish(fmt.Sprintf("%s.rate.15", name), expvar.Func(r15))
			expvar.Publish(fmt.Sprintf("%s.rate.mean", name), expvar.Func(rMean))
			expvar.Publish(fmt.Sprintf("%s.mean", name), expvar.Func(Mean))
			expvar.Publish(fmt.Sprintf("%s.min", name), expvar.Func(Min))
			expvar.Publish(fmt.Sprintf("%s.max", name), expvar.Func(Max))
			expvar.Publish(fmt.Sprintf("%s.stddev", name), expvar.Func(StdDev))
			expvar.Publish(fmt.Sprintf("%s.variance", name), expvar.Func(Variance))
			for _, p := range percentiles {
				pc := func() interface{} { return m.Percentile(p) }
				expvar.Publish(fmt.Sprintf("%s.percentile.%2.3f", name, p), expvar.Func(pc))
			}
		}
	})
	tt := func() interface{} { return time.Now().Format(time.RFC3339Nano) }
	expvar.Publish("time", expvar.Func(tt))
}
