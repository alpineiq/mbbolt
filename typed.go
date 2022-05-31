package mbbolt

func Typed[T any](db *DB) TypedDB[T] { return TypedDB[T]{db} }

type TypedDB[T any] struct {
	*DB
}

func (db TypedDB[T]) Get(bucket, key string) (v T, err error) {
	err = db.GetAny(bucket, key, &v, db.unmarshalFn)
	return
}

func (db TypedDB[T]) Put(bucket, key string, val T) error {
	return db.PutAny(bucket, key, val, db.marshalFn)
}
