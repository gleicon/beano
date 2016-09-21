package main

import "testing"

func TestLevelDBDelete(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vleveldb.Set(key, value)
	vleveldb.Delete(key, false)
	if v, err := vleveldb.Get(key); err != nil {
		t.Error(err)
	} else if v != nil {
		t.Error(errUnexpected(v))
	}
	vleveldb.Delete(key, false)
}

func TestLevelDBSet(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vleveldb.Delete(key, false)
	vleveldb.Set(key, value)
	if v, err := vleveldb.Get(key); err != nil {
		t.Error(err)
	} else if v == nil {
		t.Error(errUnexpected(v))
	}
	vleveldb.Delete(key, false)
}

func TestLevelDBGet(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vleveldb.Delete(key, false)
	if v, err := vleveldb.Get(key); err != nil {
		t.Error(err)
	} else if v != nil {
		t.Error(errUnexpected(v))
	}

	vleveldb.Set(key, value)
	if v, err := vleveldb.Get(key); err != nil {
		t.Error(err)
	} else if v == nil {
		t.Error(errUnexpected(v))
	}
	vleveldb.Delete(key, false)
}

func TestLevelDBAdd(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vleveldb.Delete(key, false)

	vleveldb.Add(key, value)
	err := vleveldb.Add(key, value)
	if err == nil {
		t.Error(err)
	}
	vleveldb.Delete(key, false)
}

func TestLevelDBReplace(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	newvalue := []byte("eric")
	vleveldb.Delete(key, false)

	vleveldb.Add(key, value)
	vleveldb.Replace(key, newvalue)
	if v, err := vleveldb.Get(key); err != nil {
		t.Error(err)
	} else if string(v) != "eric" {
		t.Error(errUnexpected(string(v)))
	}
	vleveldb.Delete(key, false)
}

func TestLevelDBIncr(t *testing.T) {
	key := []byte("beano")
	value := []byte("10")
	vleveldb.Delete(key, false)

	vleveldb.Set(key, value)
	v, err := vleveldb.Incr(key, 1)
	if err != nil {
		t.Error(err)
	} else if v != 11 {
		t.Error(errUnexpected(v))
	}

	if v, err := vleveldb.Get(key); err != nil {
		t.Error(err)
	} else if string(v) != "11" {
		t.Error(errUnexpected(string(v)))
	}
	vleveldb.Delete(key, false)
}

func TestLevelDBDecr(t *testing.T) {
	key := []byte("beano")
	value := []byte("10")
	vleveldb.Delete(key, false)

	vleveldb.Set(key, value)
	v, err := vleveldb.Decr(key, 1)

	if err != nil {
		t.Error(err)
	} else if v != 9 {
		t.Error(errUnexpected(string(v)))
	}

	if v, err := vleveldb.Get(key); err != nil {
		t.Error(err)
	} else if string(v) != "9" {
		t.Error(errUnexpected(string(v)))
	}
	vleveldb.Delete(key, false)
}
