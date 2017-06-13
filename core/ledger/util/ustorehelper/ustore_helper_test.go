// Copyright (c) 2017 The Ustore Authors.

package ustorehelper

import (
	"fmt"
	"os"
	"testing"
	"ustore"

	"github.com/hyperledger/fabric/core/ledger/testutil"
)

const testDBPath = "./ustore_data"

func TestDBBasicWriteAndReads(t *testing.T) {
	testDBBasicWriteAndReads(t, "db1", "db2", "")
}

func testDBBasicWriteAndReads(t *testing.T, dbNames ...string) {
	p := createTestDBProvider(t)
	defer p.Close()
	for _, dbName := range dbNames {
		db := p.GetDBHandle(dbName)
		db.Put([]byte("key1"), []byte("value1_"+dbName))
		db.Put([]byte("key2"), []byte("value2_"+dbName))
		db.Put([]byte("key3"), []byte("value3_"+dbName))
	}

	for _, dbName := range dbNames {
		db := p.GetDBHandle(dbName)
		val, err := db.Get([]byte("key1"))
		testutil.AssertNoError(t, err, "")
		testutil.AssertEquals(t, val, []byte("value1_"+dbName))

		val, err = db.Get([]byte("key2"))
		testutil.AssertNoError(t, err, "")
		testutil.AssertEquals(t, val, []byte("value2_"+dbName))

		val, err = db.Get([]byte("key3"))
		testutil.AssertNoError(t, err, "")
		testutil.AssertEquals(t, val, []byte("value3_"+dbName))
	}
}

func TestIterator(t *testing.T) {
	p := createTestDBProvider(t)
	defer p.Close()
	db1 := p.GetDBHandle("db1")
	db2 := p.GetDBHandle("db2")
	db3 := p.GetDBHandle("db3")
	for i := 0; i < 20; i++ {
		db1.Put([]byte(createTestKey(i)), []byte(createTestValue("db1", i)))
		db2.Put([]byte(createTestKey(i)), []byte(createTestValue("db2", i)))
		db3.Put([]byte(createTestKey(i)), []byte(createTestValue("db3", i)))
	}

	itr1 := db2.GetIterator([]byte(createTestKey(2)), []byte(createTestKey(4)))
	checkItrResults(t, itr1, createTestKeys(2, 3), createTestValues("db2", 2, 3))

	itr2 := db2.GetIterator([]byte(createTestKey(2)), nil)
	checkItrResults(t, itr2, createTestKeys(2, 19), createTestValues("db2", 2, 19))

	itr3 := db2.GetIterator(nil, nil)
	checkItrResults(t, itr3, createTestKeys(0, 19), createTestValues("db2", 0, 19))
}

func checkItrResults(t *testing.T, itr *Iterator, expectedKeys []string, expectedValues []string) {
	defer ustore.DeleteIterator(itr.Iterator)
	var actualKeys []string
	var actualValues []string
	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		actualKeys = append(actualKeys, string(itr.Key()))
		actualValues = append(actualValues, string(itr.Value()))
	}
	testutil.AssertEquals(t, actualKeys, expectedKeys)
	testutil.AssertEquals(t, actualValues, expectedValues)
	testutil.AssertEquals(t, itr.Next(), false)
}

func createTestKey(i int) string {
	return fmt.Sprintf("key_%06d", i)
}

func createTestValue(dbname string, i int) string {
	return fmt.Sprintf("value_%s_%06d", dbname, i)
}

func createTestKeys(start int, end int) []string {
	var keys []string
	for i := start; i <= end; i++ {
		keys = append(keys, createTestKey(i))
	}
	return keys
}

func createTestValues(dbname string, start int, end int) []string {
	var values []string
	for i := start; i <= end; i++ {
		values = append(values, createTestValue(dbname, i))
	}
	return values
}

func createTestDBProvider(t *testing.T) *Provider {
	if err := os.RemoveAll(testDBPath); err != nil {
		t.Fatalf("Error:%s", err)
	}
	dbConf := &Conf{}
	return NewProvider(dbConf)
}
