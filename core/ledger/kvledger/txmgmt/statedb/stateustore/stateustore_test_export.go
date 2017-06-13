// Copyright (c) 2017 The Ustore Authors.

package stateustore

import (
	"os"
	"testing"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
)

// TestVDBEnv provides a level db backed versioned db for testing
type TestVDBEnv struct {
	t          testing.TB
	DBProvider statedb.VersionedDBProvider
}

// NewTestVDBEnv instantiates and new level db backed TestVDB
func NewTestVDBEnv(t testing.TB) *TestVDBEnv {
	t.Logf("Creating new TestVDBEnv")
	removeDBPath(t, "NewTestVDBEnv")
	dbProvider := NewVersionedDBProvider()
	return &TestVDBEnv{t, dbProvider}
}

// Cleanup closes the db and removes the db folder
func (env *TestVDBEnv) Cleanup() {
	env.t.Logf("Cleaningup TestVDBEnv")
	env.DBProvider.Close()
	removeDBPath(env.t, "Cleanup")
}

func removeDBPath(t testing.TB, caller string) {
	if err := os.RemoveAll("./ustore_data"); err != nil {
		t.Fatalf("Err: %s", err)
		t.FailNow()
	}
	logger.Debugf("Removed folder [%s] for test environment for %s", "./ustore_data", caller)
}
