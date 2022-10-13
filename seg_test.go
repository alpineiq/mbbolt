package mbbolt

import (
	"strconv"
	"testing"
)

func TestSegDB(t *testing.T) {
	t.Run("OpenCloseLeak", func(t *testing.T) {
		d := t.TempDir()
		seg := NewSegDB(d, ".db", nil, 32)
		seg.Close()
		seg = NewSegDB(d, ".db", nil, 32)
		defer seg.Close()
	})
	t.Run("SegmentFn", func(t *testing.T) {
		m := [10]int{}
		for i := 0; i < 1000; i++ {
			m[DefaultSegmentByKey(strconv.Itoa(i))%10]++
		}
		for i, v := range &m {
			if v < 50 {
				t.Errorf("segment %d has %d values", i, v)
			}
		}
	})
}
