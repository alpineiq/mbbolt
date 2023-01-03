package mbbolt

import (
	"strconv"
	"sync"
	"testing"
)

func TestMultiRace(t *testing.T) {
	mdb := NewMultiDB(t.TempDir(), ".db", nil)
	defer mdb.Close()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			mdb.MustGet("test"+strconv.Itoa(i%3), nil)
		}()
	}
	wg.Wait()
	mdb.Close()
}
