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
	vdb      *KVDBBackend
}

func NewMemcachedProtocolServer(address string, vdb *KVDBBackend) *MemcachedProtocolServer {
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

func (ms MemcachedProtocolServer) readLine(conn net.Conn, buf *bufio.ReadWriter) ([]byte, error) {
	conn.SetReadDeadline(time.Now().Add(time.Second * 10))
	d, _, err := buf.ReadLine()
	return d, err
}

func (ms MemcachedProtocolServer) writeLine(buf *bufio.ReadWriter, s string) error {
	_, err := buf.WriteString(fmt.Sprintf("%s\r\n", s))
	if err != nil {
		return err
	}
	err = buf.Flush()
	return err
}

func (ms MemcachedProtocolServer) sendMessage(conn net.Conn, msg string, noreply bool, id int) {
	if noreply == true {
		//log.Printf("%d NOREPLY RESPONSE: %s", id, msg)
		return
	}
	m := fmt.Sprintf("%s\r\n", msg)
	conn.Write([]byte(m))
	//log.Printf("%d RESPONSE: %s\n", id, m)
}

func (ms MemcachedProtocolServer) handle(conn net.Conn, id int) {
	//log.Printf("Spawning new goroutine %d\n", id)
	conn.SetReadDeadline(time.Now().Add(time.Second * 10))
	//conn.SetDeadline(time.Now().Add(time.Second * 10))
	defer conn.Close()
	for {
		buf := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		noreply := false
		line, err := ms.readLine(conn, buf)
		if line == nil {
			continue
		}

		if len(line) < 3 || err != nil {
			//		log.Printf("Empty line or error reading line %s\n", err)
			ms.writeLine(buf, "ERROR")
			continue
		}

		args := strings.Split(string(line), " ")
		//	log.Printf("%d REQUEST: %s", id, args)
		cmd := strings.ToLower(args[0])

		if args[len(args)-1] == "noreply" {
			noreply = true
		} else {
			noreply = false
		}

		//	log.Printf("%d NOREPLY STATUS: %b\n", id, noreply)
		switch true {
		case cmd == "get":
			if len(args) < 2 {
				ms.writeLine(buf, "ERROR")
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
					ms.writeLine(buf, fmt.Sprintf("VALUE %s 0 %d", arg, len(v)))
					ms.writeLine(buf, string(v))
				}
			}
			if noreply == false {
				ms.writeLine(buf, "END")
			}

		case cmd == "set":
			if len(args) < 2 {
				ms.writeLine(buf, "ERROR")
				break
			}
			// retrieve body
			body, err := ms.readLine(conn, buf)
			if len(body) == 0 || err != nil {
				ms.writeLine(buf, "ERROR")
			} else {
				ms.vdb.Set([]byte(args[1]), []byte(body))
				if noreply == false {
					ms.writeLine(buf, "STORED")
				}
			}
			break

		case cmd == "replace":
			if len(args) < 2 {
				ms.writeLine(buf, "ERROR")
				break
			}
			// retrieve body
			body, err := ms.readLine(conn, buf)
			if len(body) == 0 || err != nil {
				ms.writeLine(buf, "ERROR")
				break
			} else {
				err := ms.vdb.Replace([]byte(args[1]), []byte(body))
				if err != nil {
					log.Println(err)
					ms.writeLine(buf, "NOT_STORED")
				} else {
					ms.writeLine(buf, "STORED")
				}
			}
			break

		case cmd == "add":
			if len(args) < 2 {
				ms.writeLine(buf, "ERROR")
				break
			}

			// retrieve body
			body, err := ms.readLine(conn, buf)
			if len(body) == 0 || err != nil {
				ms.writeLine(buf, "ERROR")
				break
			} else {
				err := ms.vdb.Add([]byte(args[1]), []byte(body))
				if err != nil {
					log.Println(err)
					ms.writeLine(buf, "NOT_STORED")
				} else {
					ms.writeLine(buf, "STORED")
				}
			}
			break

		case cmd == "quit":
			if len(args) > 1 {
				ms.writeLine(buf, "ERROR")
				break
			} else {
				conn.Close()
				return
			}

		case cmd == "version":
			if len(args) > 1 {
				ms.writeLine(buf, "ERROR")
			} else {
				ms.writeLine(buf, "VERSION BEANO")
			}
			break

		case cmd == "flush_all":
			ms.vdb.Flush()
			ms.writeLine(buf, "OK")
			break

		case cmd == "verbosity":
			if len(args) < 2 || len(args) > 3 {
				ms.writeLine(buf, "ERROR")
			} else {
				ms.writeLine(buf, "OK")
			}
			break

		case cmd == "delete":
			if len(args) < 2 {
				ms.writeLine(buf, "ERROR")
				break
			}
			if len(args) > 3 {
				ms.writeLine(buf, "ERROR")
				break
			}

			deleted, err := ms.vdb.Delete([]byte(args[1]), true)
			if err != nil {
				log.Println(err)
			}
			if deleted == true {
				ms.writeLine(buf, "DELETED")
			} else if deleted == false {
				ms.writeLine(buf, "NOT_FOUND")
			}
			break

		default:
			//		log.Printf("NOT IMPLEMENTED: %s\n", args[0])
			ms.writeLine(buf, "ERROR")
			break

		}
	}
}
