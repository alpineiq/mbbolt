package mbbolt

func OpenTDB[T any](path string, opts *Options) (db TypedDB[T], err error) {
	db.DB, err = Open(path, opts)
	return
}

func OpenMultiTDB[T any](m *MultiDB, path string, opts *Options) (db TypedDB[T], err error) {
	db.DB, err = m.Get(path, opts)
	return
}

func DBToTyped[T any](db *DB) TypedDB[T] { return TypedDB[T]{db} }

type TypedDB[T any] struct {
	*DB
}

func (db TypedDB[T]) ForEach(bucket string, fn func(key string, v T) error) error {
	return db.View(func(tx *Tx) error {
		ttx := TypedTx[T]{tx, db}
		return ttx.ForEach(bucket, fn)
	})
}

func (db TypedDB[T]) Get(bucket, key string) (v T, err error) {
	err = db.GetAny(bucket, key, &v, db.unmarshalFn)
	return
}

func (db TypedDB[T]) Put(bucket, key string, val T) error {
	return db.PutAny(bucket, key, val, db.marshalFn)
}

type TypedTx[T any] struct {
	*Tx
	db TypedDB[T]
}

func (tx TypedTx[T]) ForEach(bucket string, fn func(key string, v T) error) error {
	return tx.ForEachBytes(bucket, func(k, v []byte) (err error) {
		var tv T
		if err = tx.db.unmarshalFn(v, &tv); err != nil {
			return err
		}
		return fn(string(k), tv)
	})
}
