package main

/*
 Backend interface
*/

type BackendDatabase interface {
	Set([]byte, []byte) error
	Add([]byte, []byte) error
	Replace([]byte, []byte) error
	Incr([]byte, uint) (int, error)
	Decr([]byte, uint) (int, error)
	Increment([]byte, int, bool) (int, error)
	Put([]byte, []byte, bool, bool) error
	Get([]byte) ([]byte, error)
	Range([]byte, int, []byte, bool) (map[string][]byte, error)
	Delete([]byte, bool) (bool, error)
	Close()
	Stats() string
	GetDbPath() string
	Flush() error
	BucketStats() error
}
