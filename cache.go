package mbbolt

import (
	"log"

	"go.oneofone.dev/genh"
)

func CachedBucket[T any](db *DB, bucket string) *Cache[T] {
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		return err
	}); err != nil { // this should never ever ever happen
		log.Panicf("%s (%s): %v", db.Path(), bucket, err)
	}

	return &Cache[T]{
		db:     TypedDB[T]{db},
		bucket: bucket,
	}
}

type Cache[T any] struct {
	m      genh.LMap[string, T]
	db     TypedDB[T]
	bucket string
}

func (c *Cache[T]) LoadAll() error {
	return c.db.ForEachTyped(c.bucket, func(key string, v T) error {
		c.m.Set(key, v)
		return nil
	})
}

// Use clone if T is a pointer or contains slices/maps/pointers that will be modified.
func (c *Cache[T]) Get(key string, clone bool) (v T, err error) {
	v = c.m.MustGet(key, func() T {
		if v, err = c.db.Get(c.bucket, key); err == nil {
			c.m.Set(key, genh.Clone(v, false))
		}
		return v
	})
	if clone {
		v = genh.Clone(v, false)
	}
	return
}

func (c *Cache[T]) Put(key string, v T) (err error) {
	v = genh.Clone(v, false)
	if err = c.db.Put(c.bucket, key, v); err != nil {
		return
	}

	c.m.Set(key, v)
	return err
}

func (c *Cache[T]) Update(fn func(tx *Tx) (key string, v T, err error)) (err error) {
	var (
		key string
		v   T
	)
	if err = c.db.Update(func(tx *Tx) error {
		key, v, err = fn(tx)
		return err
	}); err != nil {
		return
	}

	c.m.Set(key, v)
	return err
}
