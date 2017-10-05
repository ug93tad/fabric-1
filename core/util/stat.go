package util

import (
	"sync"
	"time"
  "math/rand"
)

type Stat struct {
  lock sync.Mutex
  stats map[string]time.Time  // index by transaction ID
  sampleInterval uint32 // of the form 2^x - 1
}

// Accumulator
type Accum struct {
  count uint64
  total uint64
}

// singleton that collect timing metrics
type StatUtil struct {
  Stats map[string]*Stat  // index by stat name
  Accums map[string]*Accum
}

func (su *StatUtil) NewStat(name string, sampleInterval uint32) {
  su.Stats[name] = &Stat{}
  su.Stats[name].stats = make(map[string]time.Time)
  su.Stats[name].sampleInterval = sampleInterval
}

func (su *StatUtil) NewAccum(name string) {
  su.Accums[name] = &Accum{0,0}
}

func (acc *Accum) Update(val uint64) {
  acc.count++
  acc.total += val
}

func (acc *Accum) Get() (uint64, uint64) {
  return acc.count, acc.total
}

func (stat *Stat) Start(id string) {
  if (rand.Uint32() & stat.sampleInterval) == 0 {
    stat.lock.Lock()
    defer stat.lock.Unlock()
    stat.stats[id] = time.Now()
  }
}

// return (time, OK?) where OK = true if
// the id existed (has been Start-ed)
func (stat *Stat) End(id string) (uint64, bool) {
  stat.lock.Lock()
  defer stat.lock.Unlock()
  if val, ok := stat.stats[id]; ok {
    delete(stat.stats, id)
    return uint64(time.Since(val)), ok
  } else {
    return 0, ok
  }
}

var statUtilSyncOnce sync.Once
var statUtil *StatUtil

func GetStatUtil() *StatUtil {
	statUtilSyncOnce.Do(func() {
		statUtil = &StatUtil{}
    statUtil.Stats = make(map[string]*Stat)
    statUtil.Accums = make(map[string]*Accum)
	})
	return statUtil
}
