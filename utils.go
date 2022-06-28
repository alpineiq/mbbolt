package mbbolt

import (
	"bufio"
	"fmt"
	"io"
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

var bufPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewWriterSize(nil, 8*1024*1024)
	},
}

func getBuf(w io.Writer) *bufio.Writer {
	if b, ok := w.(*bufio.Writer); ok {
		return b
	}
	buf := bufPool.Get().(*bufio.Writer)
	buf.Reset(w)
	return buf
}

func putBufAndFlush(buf *bufio.Writer) error {
	err := buf.Flush()
	buf.Reset(nil)
	bufPool.Put(buf)
	return err
}
