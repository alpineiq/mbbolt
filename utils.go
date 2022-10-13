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

type DBer interface {
	CurrentIndex(bucket string) uint64
	NextIndex(bucket string) (uint64, error)
	SetNextIndex(bucket string, index uint64) error
	Buckets() []string
	Get(bucket, key string, v any) error
	ForEachBytes(bucket string, fn func(k, v []byte) error) error
	Put(bucket, key string, v any) error
	Delete(bucket, key string) error
}

var (
	_ DBer = (*DB)(nil)
	_ DBer = (*SegDB)(nil)
)

type ConvertFn = func(bucket string, k, v []byte) ([]byte, bool)

func ConvertDB(dst, src DBer, fn ConvertFn) error {
	for _, bkt := range src.Buckets() {
		if err := dst.SetNextIndex(bkt, src.CurrentIndex(bkt)); err != nil {
			return err
		}
		if err := src.ForEachBytes(bkt, func(k, v []byte) error {
			v, ok := fn(bkt, k, v)
			if !ok {
				return nil
			}
			return dst.Put(bkt, string(k), v)
		}); err != nil {
			return err
		}
	}
	return nil
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
