// Copyright (c) 2017 The Ustore Authors.

package ustorehelper

import (
	"bytes"
	"sync"
	"ustore"
)

var dbNameKeySep = []byte{0x00}
var lastKeyIndicator = byte(0x01)

// Provider enables to use a single ustore as multiple logical ustores
type Provider struct {
	db        *DB
	dbHandles map[string]*DBHandle
	mux       sync.Mutex
}

// NewProvider constructs a Provider
func NewProvider(conf *Conf) *Provider {
	db := CreateDB(conf)
	db.Open()
	return &Provider{db, make(map[string]*DBHandle), sync.Mutex{}}
}

// GetDBHandle returns a handle to a named db
func (p *Provider) GetDBHandle(dbName string) *DBHandle {
	p.mux.Lock()
	defer p.mux.Unlock()
	dbHandle := p.dbHandles[dbName]
	if dbHandle == nil {
		dbHandle = &DBHandle{dbName, p.db}
		p.dbHandles[dbName] = dbHandle
	}
	return dbHandle
}

// Close closes the underlying ustore
func (p *Provider) Close() {
	p.db.Close()
}

// DBHandle is an handle to a named db
type DBHandle struct {
	dbName string
	db     *DB
}

// Get returns the value for the given key
func (h *DBHandle) Get(key []byte) ([]byte, error) {
	return h.db.Get(constructLevelKey(h.dbName, key))
}

// Put saves the key/value
func (h *DBHandle) Put(key []byte, value []byte) error {
	return h.db.Put(constructLevelKey(h.dbName, key), value)
}

// Delete deletes the given key
func (h *DBHandle) Delete(key []byte) error {
	return h.db.Delete(constructLevelKey(h.dbName, key))
}

// WriteBatch writes a batch in an atomic way
func (h *DBHandle) WriteBatch(batch *UpdateBatch) error {
	ustoreBatch := ustore.NewWriteBatch()
	for k, v := range batch.KVs {
		key := constructLevelKey(h.dbName, []byte(k))
		if v == nil {
			ustoreBatch.Delete(string(key))
		} else {
			ustoreBatch.Put(string(key), string(v))
		}
	}
	if err := h.db.WriteBatch(&ustoreBatch); err != nil {
		return err
	}
	return nil
}

// GetIterator gets an handle to iterator. The iterator should be released after the use.
// The resultset contains all the keys that are present in the db between the startKey (inclusive) and the endKey (exclusive).
// A nil startKey represents the first available key and a nil endKey represent a logical key after the last available key
func (h *DBHandle) GetIterator(startKey []byte, endKey []byte) *Iterator {
	sKey := constructLevelKey(h.dbName, startKey)
	eKey := constructLevelKey(h.dbName, endKey)
	if endKey == nil {
		// replace the last byte 'dbNameKeySep' by 'lastKeyIndicator'
		eKey[len(eKey)-1] = lastKeyIndicator
	}
	return &Iterator{h.db.GetIterator(sKey, eKey)}
}

// UpdateBatch encloses the details of multiple `updates`
type UpdateBatch struct {
	KVs map[string][]byte
}

// NewUpdateBatch constructs an instance of a Batch
func NewUpdateBatch() *UpdateBatch {
	return &UpdateBatch{make(map[string][]byte)}
}

// Put adds a KV
func (batch *UpdateBatch) Put(key []byte, value []byte) {
	if value == nil {
		panic("Nil value not allowed")
	}
	batch.KVs[string(key)] = value
}

// Delete deletes a Key and associated value
func (batch *UpdateBatch) Delete(key []byte) {
	batch.KVs[string(key)] = nil
}

// Iterator extends actual ustore iterator
type Iterator struct {
	ustore.Iterator
}

// Key wraps actual ustore iterator method
func (itr *Iterator) Key() []byte {
	return retrieveAppKey([]byte(itr.Iterator.Key()))
}

func (itr *Iterator) Release() {
}

func constructLevelKey(dbName string, key []byte) []byte {
	return append(append([]byte(dbName), dbNameKeySep...), key...)
}

func retrieveAppKey(levelKey []byte) []byte {
	return bytes.SplitN(levelKey, dbNameKeySep, 2)[1]
}
