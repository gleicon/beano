package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

/*
MemcachedProtocolServer a protocol abstraction with db switching and ro mode
*/
type MemcachedProtocolServer struct {
	readonly bool
}

/*
NewMemcachedProtocolServer creates a new protocol parser
*/
func NewMemcachedProtocolServer(readonly bool) *MemcachedProtocolServer {
	ms := MemcachedProtocolServer{readonly: readonly}
	return &ms
}

/*
ReadOnly changes server state to readonly(true) or rw (false)
*/
func (ms MemcachedProtocolServer) ReadOnly(readonly bool) {
	ms.readonly = readonly
}

/*
SwitchDB not implemented. For leveldb it's cheaper to create a new instance
*/
func (ms MemcachedProtocolServer) SwitchDB(newDB string) error {
	return nil
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

func (ms MemcachedProtocolServer) checkRO(buf *bufio.ReadWriter) bool {
	if ms.readonly {
		ms.writeLine(buf, "ERROR")
		readonlyErrors.Inc(1)
	}
	return ms.readonly
}

/*
Parse memcachedprotocol and bind it with a DB Backend ops
*/
func (ms MemcachedProtocolServer) Parse(conn net.Conn, vdb BackendDatabase) {
	totalThreads.Inc(1)
	currThreads.Inc(1)
	defer currThreads.Dec(1)
	conn.SetReadDeadline(time.Now().Add(time.Second * 10))
	defer conn.Close()
	startTime := time.Now()
	for {
		buf := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		noreply := false
		line, err := ms.readLine(conn, buf)
		if err != nil {
			if err != io.EOF {
				networkErrors.Inc(1)
				log.Error("Connection closed: error %s\n", err)
			}
			return
		}

		if line == nil {
			if time.Now().Sub(startTime) > time.Second*3 {
				conn.Close()
				networkErrors.Inc(1)
				log.Info("Closing idle connection after timeout")
				return
			}
			continue

		} else {
			startTime = time.Now()
		}

		if len(line) < 3 || err != nil {
			protocolErrors.Inc(1)
			ms.writeLine(buf, "ERROR")
			continue
		}

		args := strings.Split(string(line), " ")
		cmd := strings.ToLower(args[0])

		if args[len(args)-1] == "noreply" {
			noreply = true
		} else {
			noreply = false
		}

		switch true {
		case cmd == "get":
			if len(args) < 2 {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
				break
			}
			cmdGet.Inc(1)
			for _, arg := range args[1:] {
				if arg == " " || arg == "" {
					break
				}
				v, err := vdb.Get([]byte(arg))
				if v == nil {
					getMisses.Inc(1)
					continue
				}
				if err != nil {
					log.Error("GET: %s", err)
					break
				}

				if noreply == false {
					ms.writeLine(buf, fmt.Sprintf("VALUE %s 0 %d", arg, len(v)))
					ms.writeLine(buf, string(v))
					getHits.Inc(1)
				}
			}
			if noreply == false {
				ms.writeLine(buf, "END")
			}

		case cmd == "set":
			if ms.checkRO(buf) {
				break
			}
			if len(args) < 2 {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
				break
			}
			// retrieve body
			body, err := ms.readLine(conn, buf)
			if len(body) == 0 || err != nil {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
			} else {
				err = vdb.Set([]byte(args[1]), []byte(body))
				if err != nil {
					log.Error("SET: %s", err)
					ms.writeLine(buf, "ERROR")
					protocolErrors.Inc(1)
					break
				}
				cmdSet.Inc(1)
				totalItems.Inc(1)
				currItems.Inc(1)
				if noreply == false {
					ms.writeLine(buf, "STORED")
				}
			}
			break

		case cmd == "replace":
			if ms.checkRO(buf) {
				break
			}
			if len(args) < 2 {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
				break
			}
			// retrieve body
			body, err := ms.readLine(conn, buf)
			if len(body) == 0 || err != nil {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
				break
			} else {
				err := vdb.Replace([]byte(args[1]), []byte(body))
				if err != nil {
					log.Error("REPLACE: %s", err)
					ms.writeLine(buf, "NOT_STORED")
				} else {
					ms.writeLine(buf, "STORED")
				}
			}
			break

		case cmd == "add":
			if ms.checkRO(buf) {
				break
			}
			if len(args) < 2 {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
				break
			}

			// retrieve body
			body, err := ms.readLine(conn, buf)
			if len(body) == 0 || err != nil {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
				break
			} else {
				err := vdb.Add([]byte(args[1]), []byte(body))
				if err != nil {
					log.Error("ADD: %s", err)
					ms.writeLine(buf, "NOT_STORED")
				} else {
					ms.writeLine(buf, "STORED")
				}
			}
			break

		case cmd == "quit":
			if len(args) > 1 {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
				break
			} else {
				conn.Close()
				return
			}

		case cmd == "version":
			if len(args) > 1 {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
			} else {
				ms.writeLine(buf, "VERSION BEANO")
			}
			break

		case cmd == "flush_all":
			if ms.checkRO(buf) {
				break
			}
			vdb.Flush()
			ms.writeLine(buf, "OK")
			break

		case cmd == "verbosity":
			if len(args) < 2 || len(args) > 3 {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
			} else {
				ms.writeLine(buf, "OK")
			}
			break

		case cmd == "switchdb":
			if ms.checkRO(buf) {
				break
			}
			if len(args) < 2 || len(args) > 3 {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
			} else {
				err := ms.SwitchDB(args[1])
				if err != nil {
					ms.writeLine(buf, "ERROR")
					protocolErrors.Inc(1)
					log.Error("SWITCHDB: %s", err)
				}
				s := fmt.Sprintf("%s\nOK", args[1])
				ms.writeLine(buf, s)
			}
			break

		case cmd == "delete":
			if ms.checkRO(buf) {
				break
			}
			if len(args) < 2 {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
				break
			}
			if len(args) > 3 {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
				break
			}

			deleted, err := vdb.Delete([]byte(args[1]), true)
			if err != nil {
				log.Error("DELETE: %s", err)
			}
			if deleted == true {
				ms.writeLine(buf, "DELETED")
				currItems.Dec(1)
			} else if deleted == false {
				ms.writeLine(buf, "NOT_FOUND")
			}
			break

		case cmd == "dbstats":
			if len(args) > 1 {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
			} else {
				ms.writeLine(buf, "VERSION BEANO")
			}
			s := vdb.Stats()
			ms.writeLine(buf, s)
			ms.writeLine(buf, "OK")
			break
		case cmd == "range" || cmd == "gets":
			if len(args) < 2 || len(args) > 3 {
				ms.writeLine(buf, "ERROR")
				protocolErrors.Inc(1)
				break
			}
			limit := -1
			if len(args) == 3 {
				limit, err = strconv.Atoi(args[2])
				if err != nil {
					log.Error("RANGE: couldn't parse LIMIT")
					limit = -1
				}
			}
			v, err := vdb.Range([]byte(args[1]), limit, nil, false)
			if err != nil {
				log.Error("RANGE: %s", err)
				break
			}
			if v == nil {
				getMisses.Inc(1)
				continue
			}
			cmdGet.Inc(1)
			for key, value := range v {
				if noreply == false {
					ms.writeLine(buf, fmt.Sprintf("VALUE %s 0 %d", key, len(value)))
					ms.writeLine(buf, string(value))
					getHits.Inc(1)
				}
			}
			if noreply == false {
				ms.writeLine(buf, "END")
			}

		default:
			log.Error("NOT IMPLEMENTED: %s", args[0])
			ms.writeLine(buf, "ERROR")
			protocolErrors.Inc(1)
			break

		}
		responseTiming.Update(time.Since(startTime))
	}
}
