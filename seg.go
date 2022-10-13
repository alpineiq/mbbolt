package mbbolt

import (
	"fmt"
	"hash/fnv"
	"io"

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
		name := fmt.Sprintf("%06d", i)
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
	return s.dbs[0].SetNextIndex(bucket, seq)
}

func (s *SegDB) NextIndex(bucket string) (seq uint64, err error) {
	return s.dbs[0].NextIndex(bucket)
}

func (s *SegDB) db(key string) *DB {
	return s.dbs[s.SegmentFn(key)%uint64(len(s.dbs))]
}
