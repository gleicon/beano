package main

func main() {
	vdb := NewKVBoltDBBackend("memcached.db", "memcached", 10000)
	mc := NewMemcachedProtocolServer("127.0.0.1:11211", vdb)
	defer mc.Close()
	mc.Start()
}
