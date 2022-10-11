package mbbolt

import (
	"fmt"
	"hash/fnv"
	"io"
	"sync"

	"go.oneofone.dev/genh"
)

func DefaultSegmentByKey(key string) uint64 {
	h := fnv.New64()
	io.WriteString(h, key)
	return h.Sum64()
}

// NewSegDB creates a new segmented database.
// WARNING WARNING, if numSegments changes between calls, the keys will be out of sync
func NewSegDB(prefix, ext string, opts *Options, numSegments int) *SegDB {
	seg := &SegDB{
		mdb: NewMultiDB(prefix, ext, opts),
		dbs: make([]*DB, numSegments),

		SegmentFn: DefaultSegmentByKey,
	}

	mdb := NewMultiDB(prefix, ext, opts)
	for i := 0; i < numSegments; i++ {
		name := fmt.Sprintf("%006d", i)
		db := mdb.MustGet(name, opts)
		if opts == nil || opts.MarshalFn == nil {
			db.SetMarshaler(genh.MarshalMsgpack, genh.UnmarshalMsgpack)
		}
		seg.dbs[i] = db
	}
	return seg
}

type SegDB struct {
	SegmentFn func(key string) uint64
	seqMux    sync.Mutex

	mdb *MultiDB
	dbs []*DB
}

func (s *SegDB) Close() error {
	return s.mdb.Close()
}

func (s *SegDB) Get(bucket, key string, v any) error {
	return s.db(key).Get(bucket, key, v)
}

func (s *SegDB) ForEach(bucket string, fn func(k, v []byte) error) error {
	return s.db(bucket).ForEachBytes(bucket, fn)
}

func (s *SegDB) Put(bucket, key string, v any) error {
	return s.db(key).Put(bucket, key, v)
}

func (s *SegDB) Delete(bucket, key string) error {
	return s.db(key).Delete(bucket, key)
}

func (s *SegDB) SetNextIndex(bucket string, seq uint64) error {
	s.seqMux.Lock()
	defer s.seqMux.Unlock()
	for _, db := range s.dbs {
		if err := db.SetNextIndex(bucket, seq); err != nil {
			return err
		}
	}
	return nil
}

func (s *SegDB) NextIndex(bucket string) (seq uint64, err error) {
	s.seqMux.Lock()
	defer s.seqMux.Unlock()
	var last uint64
	for i, db := range s.dbs {
		if seq, err = db.NextIndex(bucket); err != nil {
			return 0, err
		}
		if i > 0 && seq != last {
			return 0, fmt.Errorf("sequence mismatch: %d != %d", seq, last)
		}
		last = seq
	}
	return
}

func (s *SegDB) db(key string) *DB {
	return s.dbs[s.SegmentFn(key)%uint64(len(s.dbs))]
}
