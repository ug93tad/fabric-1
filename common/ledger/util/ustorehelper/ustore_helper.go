/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ustorehelper

import (
	"errors"
	"fmt"
	"sync"
	"ustore"

	"github.com/hyperledger/fabric/common/flogging"
)

var logger = flogging.MustGetLogger("ustorehelper")

type dbState int32

const (
	closed dbState = iota
	opened
)

// Conf configuration for `DB`
type Conf struct {
}

// DB - a wrapper on an actual store
type DB struct {
	conf    *Conf
	db      ustore.KVDB
	dbState dbState
	mux     sync.Mutex
	init    bool
}

// CreateDB constructs a `DB`
func CreateDB(conf *Conf) *DB {
	return &DB{
		conf:    conf,
		dbState: closed,
		init:    false,
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
	dbInst.init = true
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

// Get returns the value for the given key
func (dbInst *DB) Get(key []byte) ([]byte, error) {
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if dbInst.init == false {
		panic(fmt.Sprintf("DB is not opened yet"))
	} else if dbInst.dbState == closed {
		return nil, errors.New("")
	}
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
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if dbInst.init == false {
		panic(fmt.Sprintf("DB is not opened yet"))
	} else if dbInst.dbState == closed {
		return errors.New("")
	}
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
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if dbInst.init == false {
		panic(fmt.Sprintf("DB is not opened yet"))
	} else if dbInst.dbState == closed {
		return errors.New("")
	}
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
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if dbInst.dbState == closed {
		panic(fmt.Sprintf("DB is not opened yet"))
	}
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
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if dbInst.dbState == closed {
		return nil
	}
	if st := dbInst.db.Write(*batch); !st.Ok() {
		return errors.New(st.ToString())
	}
	return nil
}
