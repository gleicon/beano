package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
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
				go ms.handle(conn)
			} else {
				log.Print(err.Error())
			}
		}
	} else {
		log.Fatal(err.Error())
	}
}
func (ms MemcachedProtocolServer) serverError(conn net.Conn, msg string) {
	conn.Write([]byte(fmt.Sprintf("SERVER_ERROR %s\r\nEND\r\n", msg)))
	log.Printf("Server error: %s", msg)
}

func (ms MemcachedProtocolServer) startsWith(line, cmd string) bool {
	l := strings.HasPrefix(line, cmd)
	u := strings.HasPrefix(line, strings.ToUpper(cmd))
	return (l || u)
}

func (ms MemcachedProtocolServer) handle(conn net.Conn) {
	for {
		scanner := bufio.NewScanner(conn)
		scanner.Scan()
		line := scanner.Text()

		if line == "" {
			conn.Write([]byte("ERROR\r\n"))
			continue
		}
		log.Printf("REQUEST: %s", line)
		args := strings.Split(line, " ")

		switch true {
		case ms.startsWith(args[0], "get"):
			if len(args) < 2 {
				conn.Write([]byte("ERROR\r\n"))
				continue
			}

			for _, arg := range args[1:] {
				if arg == " " || arg == "" {
					break
				}
				v, err := ms.vdb.Get([]byte(arg))
				if err != nil || v == nil {
					conn.Write([]byte("ERROR\r\n"))
					continue
				}
				conn.Write([]byte(fmt.Sprintf("VALUE %s 0 %d\r\n", arg, len(v))))
				conn.Write(v)
				conn.Write([]byte("\r\n"))
			}
			conn.Write([]byte("END\r\n"))

		case ms.startsWith(args[0], "set") || ms.startsWith(args[0], "replace"):
			if len(args) < 2 {
				conn.Write([]byte("ERROR\r\n"))
				continue
			}
			// retrieve body
			scanner.Scan()
			body := scanner.Bytes()
			if len(body) == 0 {
				conn.Write([]byte("ERROR\r\n"))
				continue
			} else {
				ms.vdb.Set([]byte(args[1]), []byte(body))
				conn.Write([]byte("STORED\r\n"))
			}
		case ms.startsWith(args[0], "quit"):
			conn.Close()
			return
		default:
			conn.Write([]byte("ERROR\r\n"))
			continue

		}
	}
}
