# Beano

## Beano is a key value database 

  - speaks memcached ascii protocol
  - persists to leveldb or boltdb(wip)
  - cache keys using bloomfilter (leveldb) or couting bloom filter (boltdb)
  - can switch databases on the fly
  - can be set readonly
  - metrics ridden (expvar and go-metrics)

## Build
  - Build locally with make
  	check src/Makefile.defs for specific settings on lib and include paths. Type make. 

  - Use Vagrant + ansible (provided) to spin an Ubuntu server with mc-benchmark
  	vagrant up; vagrant ssh

  - Use ansible to build in your VPS 
  	ansible-playbook -i hosts.ini golang.yml

## Commands
  - any regular memcached client will do
	ascii quit                              [pass]
	ascii version                           [pass]
	ascii set                               [pass]
	ascii set noreply                       [pass]
	ascii get                               [pass]
	ascii mget                              [pass]
	ascii add                               [pass]
	ascii replace                           [pass]
	ascii delete                            [pass]

 - not in memcached specs: 
	statdb - stats from leveldb
	switchdb <dbname> - switch to new db file

## API
  - /api/v1/switchdb
    changes database on the fly
    example: curl -d "filename=/tmp/memcached2.db" http://127.0.0.1:11211/api/v1/switchdb

  - /debug/vars
    expvar json

## TODO
   - It already pass the basics of memcapable -a for set/get/replace. Incr and Decr are wip. 
   - mc-memcached is used more as concurrency benchmark than speed. Currently it gets near 20k writes/sec
   - I've been using levigo which is a nice library but cgo do not plays well with libraries that implement their own signal handling.
   - Better log configure (for now stats are dumped each 60 secs to log handler, not properly formatted)

## Nice to Have
   - Pluggable backend (BoltDB plus memory)
   - Keep looking into the original memcached -E backend to see if it's worth porting back to C. I've started that a couple of years ago and it worked well (https://github.com/gleicon/leveldb_engine)
 
## Licensing: MIT

