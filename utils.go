package mbbolt

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"
)

func ConvertDB(src, dst *DB, bucketFn func(name string, b *Bucket) bool, fn ConvertFn) error {
	return src.View(func(stx *Tx) error {
		return dst.Update(func(dtx *Tx) error {
			return stx.BBoltTx.ForEach(func(name []byte, b *Bucket) error {
				bktName := string(name)
				if !bucketFn(bktName, b) {
					return nil
				}

				dstBkt, err := dtx.CreateBucketIfNotExists(name)
				if err != nil {
					return err
				}

				if err = dstBkt.SetSequence(b.Sequence()); err != nil {
					return err
				}

				return b.ForEach(func(k, v []byte) (err error) {
					if v, ok := fn(bktName, k, v); ok {
						err = dstBkt.Put(k, v)
					}
					return
				})
			})
		})
	})
}

func FramesToString(frs *runtime.Frames) string {
	var buf strings.Builder
	for {
		fr, ok := frs.Next()
		if !ok {
			break
		}
		fmt.Fprintf(&buf, "- %s:%d [%s]\n", fr.File, fr.Line, fr.Func.Name())
	}
	return buf.String()
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
