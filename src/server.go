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
	var id int

	id = 0
	ms.listener, err = net.Listen("tcp", ms.address)
	if err == nil {
		for {
			if conn, err := ms.listener.Accept(); err == nil {
				conn.SetDeadline(time.Now().Add(time.Duration(10) * time.Second))
				go ms.handle(conn, id)
				id++
			} else {
				log.Print(err.Error())
			}
		}
	} else {
		log.Fatal(err.Error())
	}
}
func (ms MemcachedProtocolServer) sendMessage(conn net.Conn, msg string, noreply bool, id int) {
	if noreply == true {
		log.Printf("%d NOREPLY RESPONSE: %s", id, msg)
		return
	}
	m := fmt.Sprintf("%s\r\n", msg)
	conn.Write([]byte(m))
	log.Printf("%d RESPONSE: %s\n", id, m)
}

func (ms MemcachedProtocolServer) handle(conn net.Conn, id int) {
	log.Printf("Spawning new goroutine %d\n", id)
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

		log.Printf("%d REQUEST: %s", id, line)
		args := strings.Split(line, " ")
		cmd := strings.ToLower(args[0])

		if args[len(args)-1] == "noreply" {
			noreply = true
		} else {
			noreply = false
		}
		log.Printf("%d NOREPLY STATUS: %b\n", id, noreply)
		switch true {
		case cmd == "get":
			if len(args) < 2 {
				ms.sendMessage(conn, "ERROR", false, id)
				break
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
			ms.sendMessage(conn, "END", noreply, id)

		case cmd == "set":
			if len(args) < 2 {
				ms.sendMessage(conn, "ERROR", false, id)
				break
			}
			// retrieve body
			scanner.Scan()
			body := scanner.Bytes()
			if len(body) == 0 {
				ms.sendMessage(conn, "ERROR", false, id)
			} else {
				ms.vdb.Set([]byte(args[1]), []byte(body))
				ms.sendMessage(conn, "STORED", noreply, id)
			}
			break

		case cmd == "replace":
			if len(args) < 2 {
				ms.sendMessage(conn, "ERROR", false, id)
				break
			}
			// retrieve body
			scanner.Scan()
			body := scanner.Bytes()
			if len(body) == 0 {
				ms.sendMessage(conn, "ERROR", noreply, id)
				break
			} else {
				err := ms.vdb.Replace([]byte(args[1]), []byte(body))
				if err != nil {
					log.Println(err)
					ms.sendMessage(conn, "NOT_STORED", noreply, id)
				} else {
					ms.sendMessage(conn, "STORED", noreply, id)
				}
			}
			break

		case cmd == "add":
			if len(args) < 2 {
				ms.sendMessage(conn, "ERROR", false, id)
				break
			}

			// retrieve body
			scanner.Scan()
			body := scanner.Bytes()
			if len(body) == 0 {
				ms.sendMessage(conn, "ERROR", false, id)
				break
			} else {
				err := ms.vdb.Add([]byte(args[1]), []byte(body))
				if err != nil {
					log.Println(err)
					ms.sendMessage(conn, "NOT_STORED", noreply, id)
				} else {
					ms.sendMessage(conn, "STORED", noreply, id)
				}
			}
			break

		case cmd == "quit":
			if len(args) > 1 {
				ms.sendMessage(conn, "ERROR", false, id)
				break
			} else {
				conn.Close()
				return
			}

		case cmd == "version":
			if len(args) > 1 {
				ms.sendMessage(conn, "ERROR", false, id)
			} else {
				ms.sendMessage(conn, "VERSION BEANO", false, id)
			}
			break

		case cmd == "flush_all":
			ms.vdb.Flush()
			ms.sendMessage(conn, "OK", noreply, id)
			break

		case cmd == "verbosity":
			if len(args) < 2 || len(args) > 3 {
				ms.sendMessage(conn, "ERROR", false, id)
			} else {
				ms.sendMessage(conn, "OK", noreply, id)
			}
			break

		case cmd == "delete":
			if len(args) < 2 {
				ms.sendMessage(conn, "ERROR", noreply, id)
				break
			}
			if len(args) > 3 {
				ms.sendMessage(conn, "ERROR", noreply, id)
				break
			}

			deleted, err := ms.vdb.Delete([]byte(args[1]), true)
			if err != nil {
				log.Println(err)
			}
			if deleted == true {
				ms.sendMessage(conn, "DELETED", noreply, id)
			} else if deleted == false {
				ms.sendMessage(conn, "NOT_FOUND", noreply, id)
			}
			break

		default:
			log.Printf("NOT IMPLEMENTED: %s\n", args[0])
			ms.sendMessage(conn, "ERROR", false, id)
			break

		}
	}
}
