package mbbolt

import (
	"log"
	"sync/atomic"

	"go.oneofone.dev/genh"
	"go.oneofone.dev/oerrs"
)

const ErrDeleteKey = oerrs.String("delete")

func CacheOf[T any](db *DB, bucket string, loadAll bool) *Cache[T] {
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		return err
	}); err != nil { // this should never ever ever happen
		log.Panicf("%s (%s): %v", db.Path(), bucket, err)
	}

	c := &Cache[T]{
		db:     TypedDB[T]{db},
		bucket: bucket,
	}
	if loadAll {
		c.loadAll()
	}
	return c
}

type Cache[T any] struct {
	hits   atomic.Int64
	misses atomic.Int64

	m      genh.LMap[string, T]
	db     TypedDB[T]
	bucket string

	NoBatch bool
}

func (c *Cache[T]) loadAll() {
	if err := c.db.ForEach(c.bucket, func(key string, v T) error {
		c.m.Set(key, v)
		return nil
	}); err != nil {
		log.Printf("mbbolt: %s (%s): %v", c.db.Path(), c.bucket, err)
	}
}

// Use clone if T is a pointer or contains slices/maps/pointers that will be modified.
func (c *Cache[T]) Get(key string) (v T, err error) {
	found := true
	v = c.m.MustGet(key, func() T {
		found = false
		if v, err = c.db.Get(c.bucket, key); err == nil {
			c.m.Set(key, v)
		}
		return v
	})
	if !found {
		c.misses.Add(1)
	} else {
		c.hits.Add(1)
	}
	v = genh.Clone(v, false)
	return
}

func (c *Cache[T]) Put(key string, v T) (err error) {
	return c.Update(func(tx *Tx) (_ string, _ T, err error) {
		err = tx.PutValue(c.bucket, key, v)
		return key, v, err
	})
}

func (c *Cache[T]) Delete(key string) (err error) {
	return c.Update(func(tx *Tx) (_ string, v T, err error) {
		tx.Delete(c.bucket, key)
		return key, v, ErrDeleteKey
	})
}

func (c *Cache[T]) ForEach(fn func(k string, v T) error) error {
	return c.db.ForEach(c.bucket, func(key string, v T) error {
		c.m.Set(key, v)
		v = genh.Clone(v, false)
		return fn(key, v)
	})
}

func (c *Cache[T]) Update(fn func(tx *Tx) (key string, v T, err error)) (err error) {
	var (
		key string
		v   T
	)
	ufn := func(tx *Tx) error {
		if key, v, err = fn(tx); err == nil {
			if err = tx.PutValue(c.bucket, key, v); err == nil {
				c.m.Set(key, genh.Clone(v, false))
			}
		}
		if err == ErrDeleteKey {
			c.m.Delete(key)
			err = nil
		}
		return err
	}
	if c.NoBatch {
		return c.db.Update(ufn)
	}
	return c.db.Batch(ufn)
}

func (c *Cache[T]) Stats() (hits, misses int64) {
	return c.hits.Load(), c.misses.Load()
}
