// Copyright (c) 2017 The Ustore Authors.

package ustorehelper

import (
	"errors"
	"sync"
	"ustore"

	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("ustorehelper")

type dbState int32

const (
	closed dbState = iota
	opened
)

type Conf struct {
}

// DB - a wrapper on an actual store
type DB struct {
	conf    *Conf
	db      ustore.KVDB
	dbState dbState
	mux     sync.Mutex
}

// CreateDB constructs a `DB`
func CreateDB(conf *Conf) *DB {
	return &DB{
		conf:    conf,
		dbState: closed,
	}
}

// Open opens the underlying db
func (dbInst *DB) Open() {
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if dbInst.dbState == opened {
		return
	}
	dbInst.db = ustore.NewKVDB()
	dbInst.dbState = opened
}

// Close closes the underlying db
func (dbInst *DB) Close() {
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if dbInst.dbState == closed {
		return
	}
	ustore.DeleteKVDB(dbInst.db)
	dbInst.dbState = closed
}

func (dbInst *DB) isOpen() bool {
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	return dbInst.dbState == opened
}

// Get returns the value for the given key
func (dbInst *DB) Get(key []byte) ([]byte, error) {
	ret := dbInst.db.Get(string(key))
	st := ret.GetFirst()
	value := []byte(ret.GetSecond())
	err := errors.New("")
	err = nil
	if st.IsNotFound() {
		value = nil
	} else if !st.Ok() {
		err = errors.New(st.ToString())
	}
	if err != nil {
		logger.Errorf("Error while trying to retrieve key [%#v]: %s", key, err)
		return nil, err
	}
	return value, nil
}

// Put saves the key/value
func (dbInst *DB) Put(key []byte, value []byte) error {
	st := dbInst.db.Put(string(key), string(value))
	if !st.Ok() {
		err := errors.New(st.ToString())
		logger.Errorf("Error while trying to write key [%#v]", key)
		return err
	}
	return nil
}

// Delete deletes the given key
func (dbInst *DB) Delete(key []byte) error {
	st := dbInst.db.Delete(string(key))
	if !st.Ok() {
		err := errors.New(st.ToString())
		logger.Errorf("Error while trying to delete key [%#v]", key)
		return err
	}
	return nil
}

// GetIterator returns an iterator over key-value store. The iterator should be released after the use.
// The resultset contains all the keys that are present in the db between the startKey (inclusive) and the endKey (exclusive).
// A nil startKey represents the first available key and a nil endKey represent a logical key after the last available key
func (dbInst *DB) GetIterator(startKey []byte, endKey []byte) ustore.Iterator {
	it := dbInst.db.NewIterator()
	startK := string("")
	endK := string("")
	if startKey != nil {
		startK = string(startKey)
	}
	if endKey != nil {
		endK = string(endKey)
	}
	it.SetRange(startK, endK)
	it.SeekToFirst()
	return it
}

// WriteBatch writes a batch
func (dbInst *DB) WriteBatch(batch *ustore.WriteBatch) error {
	if st := dbInst.db.Write(*batch); !st.Ok() {
		return errors.New(st.ToString())
	}
	return nil
}
