package genbolt

import (
	"log"
	"sync"

	"go.oneofone.dev/genh"
)

func BucketCache[T any](db *DB, bucket string) *Cache[T] {
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
}

func (c *Cache[T]) Get(key string) (v T, err error) {
	var ok bool
	c.mux.RLock()
	if v, ok = c.m[key]; ok {
		c.mux.RUnlock()
		return
	}
	c.mux.RUnlock()

	c.mux.Lock()
	if v, ok = c.m[key]; !ok {
		v, err = c.db.Get(c.bucket, key)
		c.m[key] = v
	}
	c.mux.Unlock()

	if ok && err == nil {
		v = genh.TypeCopy(v)
	}

	return
}

func (c *Cache[T]) Put(key string, v T) (err error) {
	v = genh.TypeCopy(v)

	c.mux.Lock()
	defer c.mux.Unlock()

	if err = c.db.Put(c.bucket, key, v); err != nil {
		return
	}
	c.m[key] = v
	return err
}
