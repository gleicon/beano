package main

import "testing"

func TestBoltDBDelete(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vboltdb.Set(key, value)
	vboltdb.Delete(key, false)
	if v, err := vboltdb.Get(key); err != nil {
		t.Error(err)
	} else if v != nil {
		t.Error(errUnexpected(v))
	}
	vboltdb.Delete(key, false)
}

func TestBoltDBSet(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vboltdb.Delete(key, false)
	vboltdb.Set(key, value)
	if v, err := vboltdb.Get(key); err != nil {
		t.Error(err)
	} else if v == nil {
		t.Error(errUnexpected(v))
	}
	vboltdb.Delete(key, false)
}

func TestBoltDBGet(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vboltdb.Delete(key, false)
	if v, err := vboltdb.Get(key); err != nil {
		t.Error(err)
	} else if v != nil {
		t.Error(errUnexpected(v))
	}

	vboltdb.Set(key, value)
	if v, err := vboltdb.Get(key); err != nil {
		t.Error(err)
	} else if v == nil {
		t.Error(errUnexpected(v))
	}
	vboltdb.Delete(key, false)
}

func TestBoltDBAdd(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vboltdb.Delete(key, false)

	vboltdb.Add(key, value)
	err := vboltdb.Add(key, value)
	if err == nil {
		t.Error(err)
	}
	vboltdb.Delete(key, false)
}

func TestBoltDBReplace(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	newvalue := []byte("eric")
	vboltdb.Delete(key, false)

	vboltdb.Add(key, value)
	vboltdb.Replace(key, newvalue)
	if v, err := vboltdb.Get(key); err != nil {
		t.Error(err)
	} else if string(v) != "eric" {
		t.Error(errUnexpected(string(v)))
	}
	vboltdb.Delete(key, false)
}

func TestBoltDBIncr(t *testing.T) {
	key := []byte("beano")
	value := []byte("10")
	vboltdb.Delete(key, false)

	vboltdb.Set(key, value)
	v, err := vboltdb.Incr(key, 1)
	if err != nil {
		t.Error(err)
	} else if v != 11 {
		t.Error(errUnexpected(v))
	}

	if v, err := vboltdb.Get(key); err != nil {
		t.Error(err)
	} else if string(v) != "11" {
		t.Error(errUnexpected(string(v)))
	}
	vboltdb.Delete(key, false)
}

func TestBoltDBDecr(t *testing.T) {
	key := []byte("beano")
	value := []byte("10")
	vboltdb.Delete(key, false)

	vboltdb.Set(key, value)
	v, err := vboltdb.Decr(key, 1)

	if err != nil {
		t.Error(err)
	} else if v != 9 {
		t.Error(errUnexpected(string(v)))
	}

	if v, err := vboltdb.Get(key); err != nil {
		t.Error(err)
	} else if string(v) != "9" {
		t.Error(errUnexpected(string(v)))
	}
	vboltdb.Delete(key, false)
}

func TestBoltDBFlush(t *testing.T) {
	key := []byte("beano")
	value := []byte("clapton")
	vboltdb.Delete(key, false)
	vboltdb.Set(key, value)
	if v, err := vboltdb.Get(key); err != nil {
		t.Error(err)
	} else if v == nil {
		t.Error(errUnexpected(v))
	}
	vboltdb.Flush()
	if v, err := vboltdb.Get(key); err != nil {
		t.Error(err)
	} else if v != nil {
		t.Error(errUnexpected(v))
	}
}
