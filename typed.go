package mbbolt

func Typed[T any](db *DB) TypedDB[T] { return TypedDB[T]{db} }

type TypedDB[T any] struct {
	*DB
}

func (db TypedDB[T]) ForEach(bucket string, fn func(key string, v T) error) error {
	return db.View(func(tx *Tx) error {
		return tx.ForEach(bucket, func(k, v []byte) (err error) {
			var tv T
			if err = db.unmarshalFn(v, &tv); err != nil {
				return err
			}
			return fn(string(k), tv)
		})
	})
}

func (db TypedDB[T]) Get(bucket, key string) (v T, err error) {
	err = db.GetAny(bucket, key, &v, db.unmarshalFn)
	return
}

func (db TypedDB[T]) Put(bucket, key string, val T) error {
	return db.PutAny(bucket, key, val, db.marshalFn)
}
