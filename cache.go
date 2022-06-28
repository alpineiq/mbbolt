package mbbolt

import (
	"log"
	"sync"
	"sync/atomic"

	"go.oneofone.dev/genh"
)

func CachedBucket[T any](db *DB, bucket string) *Cache[T] {
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucketIfNotExists(unsafeBytes(bucket))
		return err
	}); err != nil { // this never ever ever happen
		log.Panicf("%s (%s): %v", db.Path(), bucket, err)
	}

	return &Cache[T]{
		m:      map[string]T{},
		db:     TypedDB[T]{db},
		bucket: bucket,
	}
}

type Cache[T any] struct {
	mux    sync.RWMutex
	m      map[string]T
	db     TypedDB[T]
	bucket string
	miss   int64
	hit    int64
	err    int64
}

func (c *Cache[T]) LoadAll() error {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.db.ForEach(c.bucket, func(key string, v T) error {
		c.m[key] = v
		return nil
	})
}

// Use clone if T is a pointer or contains slices/maps/pointers that will be modified.
func (c *Cache[T]) Get(key string, clone bool) (v T, err error) {
	var ok bool
	c.mux.RLock()
	if v, ok = c.m[key]; ok {
		c.mux.RUnlock()
		if clone {
			v = genh.Clone(v)
		}
		atomic.AddInt64(&c.hit, 1)
		return
	}
	c.mux.RUnlock()
	atomic.AddInt64(&c.miss, 1)
	c.mux.Lock()
	if v, ok = c.m[key]; !ok {
		if v, err = c.db.Get(c.bucket, key); err == nil {
			c.m[key] = v
		} else {
			atomic.AddInt64(&c.err, 1)
		}
	}
	c.mux.Unlock()

	if ok {
		v = genh.Clone(v)
	}

	return
}

func (c *Cache[T]) Put(key string, v T) (err error) {
	v = genh.Clone(v)

	c.mux.Lock()
	defer c.mux.Unlock()

	if err = c.db.Put(c.bucket, key, v); err != nil {
		return
	}

	c.m[key] = v
	return err
}

func (c *Cache[T]) Stats() (hit, miss, errs int64) {
	hit, miss, errs = atomic.LoadInt64(&c.hit), atomic.LoadInt64(&c.miss), atomic.LoadInt64(&c.err)
	return
}
