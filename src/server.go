package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

type MemcachedProtocolServer struct {
	address  string
	listener net.Listener
	vdb      *KVBoltDBBackend
}

func NewMemcachedProtocolServer(address string, vdb *KVBoltDBBackend) *MemcachedProtocolServer {
	ms := MemcachedProtocolServer{address, nil, vdb}
	return &ms
}

func (ms MemcachedProtocolServer) Close() {
	ms.Close()
}

func (ms MemcachedProtocolServer) Start() {
	var err error
	ms.listener, err = net.Listen("tcp", ms.address)
	if err == nil {
		for {
			if conn, err := ms.listener.Accept(); err == nil {
				conn.SetDeadline(time.Now().Add(time.Duration(10) * time.Second))
				go ms.handle(conn)
			} else {
				log.Print(err.Error())
			}
		}
	} else {
		log.Fatal(err.Error())
	}
}
func (ms MemcachedProtocolServer) sendMessage(conn net.Conn, msg string, noreply bool) {
	if noreply == true {
		return
	}
	m := fmt.Sprintf("%s\r\n", msg)
	conn.Write([]byte(m))
	log.Printf("RESPONSE: %s\n", m)
}

func (ms MemcachedProtocolServer) handle(conn net.Conn) {
	defer conn.Close()
	for {
		noreply := false
		scanner := bufio.NewScanner(conn)
		scanner.Scan()
		line := scanner.Text()

		if line == "" {
			conn.Write([]byte("ERROR\r\n"))
			continue
		}
		log.Printf("REQUEST: %s", line)
		args := strings.Split(line, " ")
		cmd := strings.ToLower(args[0])
		log.Println(args)

		if args[len(args)-1] == "noreply" {
			noreply = true
		} else {
			noreply = false
		}
		log.Printf("NOREPLY: %b\n", noreply)
		switch true {
		case cmd == "get":
			if len(args) < 2 {
				ms.sendMessage(conn, "ERROR", noreply)
				continue
			}
			for _, arg := range args[1:] {
				if arg == " " || arg == "" {
					break
				}
				v, err := ms.vdb.Get([]byte(arg))
				if v == nil {
					continue
				}
				if err != nil {
					break
				}
				if noreply == false {
					conn.Write([]byte(fmt.Sprintf("VALUE %s 0 %d\r\n", arg, len(v))))
					conn.Write(v)
					conn.Write([]byte("\r\n"))
				}
			}
			ms.sendMessage(conn, "END", noreply)

		case cmd == "set":
			if len(args) < 2 {
				ms.sendMessage(conn, "ERROR", noreply)
				continue
			}
			// retrieve body
			scanner.Scan()
			body := scanner.Bytes()
			if len(body) == 0 {
				ms.sendMessage(conn, "ERROR", noreply)
			} else {
				ms.vdb.Set([]byte(args[1]), []byte(body))
				ms.sendMessage(conn, "STORED", noreply)
			}
			continue

		case cmd == "replace":
			if len(args) < 2 {
				ms.sendMessage(conn, "ERROR", noreply)
				continue
			}
			// retrieve body
			scanner.Scan()
			body := scanner.Bytes()
			if len(body) == 0 {
				ms.sendMessage(conn, "STORED", noreply)
				continue
			} else {
				err := ms.vdb.Replace([]byte(args[1]), []byte(body))
				if err != nil {
					log.Println(err)
					ms.sendMessage(conn, "NOT_STORED", noreply)
				} else {
					ms.sendMessage(conn, "STORED", noreply)
				}
			}

		case cmd == "add":
			if len(args) < 2 {
				ms.sendMessage(conn, "ERROR", noreply)
				continue
			}

			// retrieve body
			scanner.Scan()
			body := scanner.Bytes()
			if len(body) == 0 {
				ms.sendMessage(conn, "ERROR", noreply)
				continue
			} else {
				err := ms.vdb.Add([]byte(args[1]), []byte(body))
				if err != nil {
					log.Println(err)
					ms.sendMessage(conn, "NOT_STORED", noreply)
				} else {
					ms.sendMessage(conn, "STORED", noreply)
				}
			}

		case cmd == "quit":
			if len(args) > 1 {
				ms.sendMessage(conn, "ERROR", false)
				continue
			} else {
				conn.Close()
				return
			}

		case cmd == "version":
			if len(args) > 1 {
				ms.sendMessage(conn, "ERROR", false)
			} else {
				ms.sendMessage(conn, "VERSION BEANO", false)
			}
			continue

		case cmd == "flush_all":
			if len(args) > 1 {
				ms.sendMessage(conn, "ERROR", noreply)
			} else {
				ms.vdb.Flush()
				ms.sendMessage(conn, "OK", noreply)
			}
			continue

		case cmd == "verbosity":
			if len(args) < 2 {
				ms.sendMessage(conn, "ERROR", noreply)
			} else {
				ms.sendMessage(conn, "OK", noreply)
			}
			continue

		case cmd == "delete":
			if len(args) < 2 {
				ms.sendMessage(conn, "ERROR", noreply)
				continue
			}
			if len(args) > 3 {
				ms.sendMessage(conn, "ERROR", noreply)
				continue
			}

			deleted, err := ms.vdb.Delete([]byte(args[1]), true)
			if err != nil {
				log.Println(err)
			}
			if deleted == true {
				ms.sendMessage(conn, "DELETED", noreply)
			} else if deleted == false {
				ms.sendMessage(conn, "NOT_FOUND", noreply)
			}
			continue

		default:
			log.Printf("NOT IMPLEMENTED: %s\n", args[0])
			ms.sendMessage(conn, "ERROR", noreply)
			continue

		}
	}
}
