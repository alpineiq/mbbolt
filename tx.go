package genbolt

import (
	"log"

	"go.etcd.io/bbolt"
)

type (
	MarshalFn   = func(any) ([]byte, error)
	UnmarshalFn = func([]byte, any) error

	ConvertFn = func(bucket string, k, v []byte) ([]byte, bool)
)

type Tx struct {
	*RawTx
	db *DB
}

func (tx *Tx) Bucket(bucket string, createIfNotExists bool) *Bucket {
	if createIfNotExists {
		b, err := bucketTxIfNotExists(tx, bucket)
		if err != nil {
			log.Panicf("%s: %v", bucket, err)
		}
		return b
	}
	return bucketTx(tx, bucket)
}

func (tx *Tx) GetBytes(bucket, key string, clone bool) []byte {
	return getTx(tx, bucket, key, clone)
}

func (tx *Tx) PutBytes(bucket, key string, val []byte) error {
	return putTx(tx, bucket, key, val)
}

func (tx *Tx) Get(bucket, key string, out any) error {
	return tx.GetAny(bucket, key, out, tx.db.unmarshalFn)
}

func (tx *Tx) Put(bucket, key string, val any) error {
	return tx.PutAny(bucket, key, val, tx.db.marshalFn)
}

func (tx *Tx) GetAny(bucket, key string, out any, unmarshalFn UnmarshalFn) error {
	if b, ok := out.(*[]byte); ok {
		*b = tx.GetBytes(bucket, key, true)
		return nil
	}
	if unmarshalFn == nil {
		unmarshalFn = DefaultUnmarshalFn
	}
	b := tx.GetBytes(bucket, key, false)
	return unmarshalFn(b, &out)
}

func (tx *Tx) PutAny(bucket, key string, val any, marshalFn MarshalFn) error {
	if b, ok := val.([]byte); ok {
		return tx.PutBytes(bucket, key, b)
	}
	if marshalFn == nil {
		marshalFn = DefaultMarshalFn
	}
	b, err := marshalFn(val)
	if err != nil {
		return err
	}
	return tx.PutBytes(bucket, key, b)
}

func (tx *Tx) ForEach(bucket string, fn func(k, v []byte) error) error {
	return bucketTx(tx, bucket).ForEach(fn)
}

func (tx *Tx) Range(bucket string, start []byte, fn func(cursor *Cursor, k, v []byte) error, forward bool) (err error) {
	c := bucketTx(tx, bucket).Cursor()
	if forward {
		for k, v := c.Seek(start); k != nil; k, v = c.Next() {
			if err = fn(c, k, v); err != nil {
				return
			}
		}
	} else {
		for k, v := c.Seek(start); k != nil; k, v = c.Prev() {
			if err = fn(c, k, v); err != nil {
				return
			}
		}
	}
	return
}

func (tx *Tx) ForEachUpdate(bucket string, fn func(k, v []byte, setValue func(k, nv []byte)) (err error)) (err error) {
	var updateTable map[string][]byte
	b := bucketTx(tx, bucket)

	setValue := func(k, v []byte) {
		if updateTable == nil {
			updateTable = map[string][]byte{}
		}
		updateTable[string(k)] = v
	}

	if err = b.ForEach(func(k, v []byte) error {
		return fn(k, v, setValue)
	}); err != nil {
		return
	}

	for k, v := range updateTable {
		kb := unsafeBytes(k)
		if v == nil {
			err = b.Delete(kb)
		} else {
			err = b.Put(kb, v)
		}
		if err != nil {
			return
		}
	}

	return
}

func (tx *Tx) Delete(bucket, key string) error {
	return bucketTx(tx, bucket).Delete(unsafeBytes(key))
}

func GetTxAny[T any](tx *Tx, bucket, key string, unmarshalFn UnmarshalFn) (out T, err error) {
	if unmarshalFn == nil {
		unmarshalFn = DefaultUnmarshalFn
	}
	err = unmarshalFn(getTx(tx, bucket, key, false), &out)
	return
}

func GetAny[T any](db *DB, bucket, key string, unmarshalFn UnmarshalFn) (out T, err error) {
	err = db.View(func(tx *Tx) error {
		out, err = GetTxAny[T](tx, bucket, key, unmarshalFn)
		return err
	})
	return
}

func ForEachTx[T any](tx *Tx, bucket string, fn func(key []byte, val T) error, filterFn func(k, v []byte) bool, unmarshalFn UnmarshalFn) error {
	b := bucketTx(tx, bucket)
	if b == nil {
		return bbolt.ErrBucketNotFound
	}

	if unmarshalFn == nil {
		unmarshalFn = DefaultUnmarshalFn
	}

	if filterFn == nil {
		filterFn = filterOk
	}
	return b.ForEach(func(k, v []byte) (err error) {
		if !filterFn(k, v) {
			return
		}
		var val T
		if err = unmarshalFn(v, &val); err != nil {
			return
		}
		return fn(k, val)
	})
}

func bucketTx(tx *Tx, bucket string) *Bucket {
	return tx.RawTx.Bucket(unsafeBytes(bucket))
}

func bucketTxIfNotExists(tx *Tx, bucket string) (*Bucket, error) {
	return tx.CreateBucketIfNotExists(unsafeBytes(bucket))
}

func getTx(tx *Tx, bucket string, id string, clone bool) (out []byte) {
	out = bucketTx(tx, bucket).Get(unsafeBytes(id))
	if clone {
		out = append([]byte(nil), out...)
	}
	return
}

func putTx(tx *Tx, bucket string, id string, value []byte) error {
	return bucketTx(tx, bucket).Put(unsafeBytes(id), value)
}

func filterOk(_, _ []byte) bool { return true }
