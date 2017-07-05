package main

import (
	"fmt"
	"net"
	"net/http"
	"time"
)

var messages chan string

func loadDB(backend string, filename string) BackendDatabase {
	var vdb BackendDatabase
	var err error
	switch backend {
	case "boltdb":
		vdb, err = NewKVBoltDBBackend(filename, "memcached", 1000000)
		break

	case "badger":
		vdb, err = NewBadgerBackend(filename)
		break

	case "inmem":
		vdb, err = NewInmemBackend(1000000)
		break

	default:
	case "leveldb":
		vdb, err = NewLevelDBBackend(filename)
		break

	}
	if err != nil {
		log.Error("Error opening db %s", err)
		return nil
	}
	return vdb

}

func switchDBHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(w, "405 Method not allowed", 405)
		return
	}
	filename := req.FormValue("filename")
	if filename == "" {
		http.Error(w, "500 Internal error", 500)
		return
	}
	messages <- filename
	w.Write([]byte("OK"))
}

func serve(ip string, port string, filename string, backend string) {
	var err error
	messages = make(chan string)

	go func() {
		http.HandleFunc("/api/v1/switchdb", switchDBHandler)
		http.ListenAndServe(":8080", nil)
	}()
	addr := fmt.Sprintf("%s:%s", ip, port)
	listener, err := net.Listen("tcp", addr)
	defer listener.Close()

	vdb := loadDB(backend, filename)
	defer vdb.Close()

	ms := NewMemcachedProtocolServer(false)

	go func() {
		for {
			filename := <-messages
			if filename != "" {
				if vdb.GetDbPath() == filename {
					log.Error("DB Switch from %s to %s - Aborted, db already open", vdb.GetDbPath(), filename)
					continue
				}
				ms.ReadOnly(true)
				log.Info("DB Switch from %s to %s", vdb.GetDbPath(), filename)
				currentVdb := vdb
				time.Sleep(2 * time.Second)
				vdb = loadDB(backend, filename)
				time.Sleep(2 * time.Second)
				currentVdb.Close()
				log.Info("DB Switch from %s to %s done", vdb.GetDbPath(), filename)
				ms.ReadOnly(false)
			}
		}
	}()

	if err == nil {
		for {
			if conn, err := listener.Accept(); err == nil {
				totalConnections.Inc(1)
				go ms.Parse(conn, vdb)
			} else {
				networkErrors.Inc(1)
				log.Error(err.Error())
			}
		}
	} else {
		networkErrors.Inc(1)
		log.Fatal(err.Error())
	}
}
