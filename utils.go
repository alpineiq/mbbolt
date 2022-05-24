package extbolt

import (
	"sync"
	"time"
	"unsafe"
)

func ConvertDB(src, dst *DB, bucketFn func(name string, b *Bucket) bool, fn ConvertFn) error {
	return src.View(func(stx *Tx) error {
		return dst.Update(func(dtx *Tx) error {
			return stx.RawTx.ForEach(func(name []byte, b *Bucket) error {
				sname := string(name)
				if !bucketFn(sname, b) {
					return nil
				}

				dbkt, err := dtx.CreateBucketIfNotExists(name)
				if err != nil {
					return err
				}
				return b.ForEach(func(k, v []byte) (err error) {
					if v, ok := fn(sname, k, v); ok {
						err = dbkt.Put(k, v)
					}
					return
				})
			})
		})
	})
}

type slowUpdate struct {
	sync.Mutex
	fn  OnSlowUpdateFn
	min time.Duration
}

type stringCap struct {
	string
	int
}

func unsafeBytes(s string) (out []byte) {
	return *(*[]byte)(unsafe.Pointer(&stringCap{s, len(s)}))
}
