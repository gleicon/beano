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
}

func NewMemcachedProtocolServer(address string) *MemcachedProtocolServer {
	ms := MemcachedProtocolServer{address, nil}
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
				conn.SetDeadline(time.Now().Add(time.Duration(60) * time.Second))
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
	//	reader := bufio.NewReader(conn)
	scanner := bufio.NewScanner(conn)
	for {
		/*		line, err := reader.ReadBytes('\n')
				if err != nil {
					conn.Close()
					break
				} */
		scanner.Scan()
		line := scanner.Text()

		if line == "" {
			conn.Write([]byte("ERROR\r\n"))
			continue
		}

		switch true {
		case ms.startsWith(line, "get"):
			args := strings.Split(line, " ")
			for _, arg := range args[1:] {
				if arg == " " || arg == "" {
					break
				}
				responseMessage := "that's key:" + arg
				conn.Write([]byte(fmt.Sprintf("VALUE %s 0 %d\r\n", arg, len(responseMessage))))
				conn.Write([]byte(responseMessage))
				conn.Write([]byte("\r\n"))
			}
			conn.Write([]byte("END\r\n"))
		case ms.startsWith(line, "set"):
			args := strings.Split(line, " ")
			fmt.Println(args)
			scanner.Scan()
			body := scanner.Bytes()
			if len(body) == 0 {
				conn.Write([]byte("ERROR\r\n"))
				continue
			} else {
				conn.Write([]byte("STORED\r\n"))
			}
		}
	}
}

func main() {
	mc := NewMemcachedProtocolServer("127.0.0.1:11211")
	defer mc.Close()
	mc.Start()
}
