# Beano

## Beano is a key value database 

  - speaks memcached ascii protocol
  - persists to leveldbm bolddb or memory
  - cache keys using bloomfilter (leveldb) or couting bloom filter (boltdb) to save I/O
  - can switch databases on the fly
  - can be set readonly
  - metrics ridden (expvar and go-metrics)
  - range queries by key prefix
  - uses goleveldb (native), boltdb and facebook's inmem

## Build
  - Build locally with make
    - check src/Makefile.defs for specific settings on lib and include paths. Type make. 

  - Use Vagrant + ansible (provided) to spin an Ubuntu server with mc-benchmark
    - vagrant up; vagrant ssh

  - Use ansible to build in your VPS 
    - ansible-playbook -i hosts.ini golang.yml
   
  - mc-benchmark used more as concurrency benchmark than speed. Currently it gets near ~~20~~40k writes/sec

## Running
	$ beano [-s ip] [-p port] [-f /path/to/db/file -q -b leveldb|boltdb|inmem]")
		- default ip: 127.0.0.1
		- default port: 11211
		- default backend: leveldb
		- default db path+file: ./memcached.db
		- (-q enables profiling to /tmp/*.prof")

## Commands
  - any regular memcached client will do
    - ascii quit                              [pass]
    - ascii version                           [pass]
    - ascii set                               [pass]
    - ascii set noreply                       [pass]
    - ascii get                               [pass]
    - ascii mget                              [pass]
    - ascii add                               [pass]
    - ascii replace                           [pass]
    - ascii delete                            [pass]

  - not in memcached specs: 
    - statdb - stats from leveldb
    - switchdb <dbname> - switch to new db file
    - range <prefix> [limit] - range query of keys that begin w/ prefix, limited by [limit]. no limit or -1 means bring it all.

- modified behaviour than memcached
    - gets - alias to range so all drivers can work.

## API
  - /api/v1/switchdb
    - changes database on the fly
    - example: curl -d "filename=/tmp/memcached2.db" http://127.0.0.1:8080/api/v1/switchdb

  - /debug/vars
    - expvar json

## TODO
   - It already pass the basics of memcapable -a for set/get/replace. Incr and Decr are wip. 
   - Better log configure (for now stats are dumped each 60 secs to log handler, not properly formatted)

![github analytics](http://perfmetrics.co/api/track/github.com:beano/?t=u&type_navigate=navigate&host=https%253A%252F%252Fgithub.com%252Fgleicon%252F/beano)

## Licensing: MIT



