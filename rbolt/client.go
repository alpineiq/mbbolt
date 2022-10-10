package rbolt

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"go.oneofone.dev/genh"
	"go.oneofone.dev/gserv"
	"go.oneofone.dev/oerrs"
	"go.oneofone.dev/otk"
)

func NewClient(addr string) *Client {
	if !strings.HasSuffix(addr, "/") {
		addr += "/"
	}
	return &Client{
		c:    gserv.H2Client(),
		addr: addr,

		RetryCount: 100,
		RetrySleep: time.Millisecond * 100,
	}
}

type (
	bucketKeyVal = genh.LMultiMap[string, string, any]
	Client       struct {
		c     *http.Client
		locks genh.LMap[string, *Tx]
		m     genh.LMap[string, *bucketKeyVal]
		addr  string

		RetryCount int
		RetrySleep time.Duration
		AuthKey    string
	}
)

func (c *Client) Close() error {
	var el oerrs.ErrorList
	c.locks.ForEach(func(k string, tx *Tx) bool {
		el.PushIf(tx.Rollback())
		return true
	})
	return el.Err()
}

func (c *Client) ClearCache() {
	c.m.Clear()
}

func (c *Client) do(method, url string, body, out any) (err error) {
	var resp *http.Response
	bodyBytes, _ := body.([]byte)
	if bodyBytes == nil {
		if bodyBytes, err = genh.MarshalMsgpack(body); err != nil {
			return
		}
	}
	retry := c.RetryCount
	for {
		req, _ := http.NewRequest(method, c.addr+url, bytes.NewReader(bodyBytes))
		if c.AuthKey != "" {
			req.Header.Set("Authorization", c.AuthKey)
		}
		if resp, err = c.c.Do(req); err == nil {
			break
		}
		if retry--; retry < 1 {
			return oerrs.ErrorCallerf(2, "failed after %d retires: %w", c.RetryCount, err)
		}
		time.Sleep(c.RetrySleep)
	}

	// log.Println(method, url, string(body))
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusUnauthorized {
			return oerrs.Errorf("unauthorized")
		}
		var r gserv.Error
		if err := genh.DecodeMsgpack(resp.Body, &r); err != nil {
			return oerrs.Errorf("error decoding response for %s %s (%v): %v", method, url, resp.StatusCode, err)
		}
		return r
	}

	if out, ok := out.(*decCloser); ok {
		*out = decCloser{genh.NewMsgpackDecoder(resp.Body), resp.Body}
		return nil
	}

	defer resp.Body.Close()
	if out == nil {
		return nil
	}

	return genh.DecodeMsgpack(resp.Body, out)
}

func (c *Client) cache(db string) *bucketKeyVal {
	return c.m.MustGet(db, func() *bucketKeyVal {
		return &bucketKeyVal{}
	})
}

func (c *Client) Get(db, bucket, key string, v any) (err error) {
	vv := c.cache(db).MustGet(bucket, key, func() any {
		err = c.do("GET", "r/"+db+"/"+bucket+"/"+key, nil, v)
		return v
	})
	if err != nil {
		c.cache(db).DeleteChild(bucket, key)
	}
	genh.ReflectClone(reflect.ValueOf(v).Elem(), reflect.ValueOf(vv).Elem(), true)
	return
}

func (c *Client) Put(db, bucket, key string, v any) error {
	if err := c.do("PUT", "r/"+db+"/"+bucket+"/"+key, v, nil); err != nil {
		return err
	}
	c.cache(db).Set(bucket, key, v)
	return nil
}

func (c *Client) Delete(db, bucket, key string) error {
	if err := c.do("DELETE", "r/"+db+"/"+bucket+"/"+key, nil, nil); err != nil {
		return err
	}
	c.cache(db).DeleteChild(bucket, key)
	return nil
}

func (c *Client) Update(db string, fn func(tx *Tx) error) error {
	tx, err := c.Begin(db)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		if err2 := tx.Rollback(); err != nil {
			err = oerrs.Errorf("%v: %w", err, err2)
		}
		return err
	}
	return tx.Commit()
}

func (c *Client) Begin(db string) (*Tx, error) {
	if err := c.do("POST", "beginTx/"+db, nil, nil); err != nil {
		return nil, err
	}
	tx := &Tx{c: c, db: db, prefix: "tx/" + db + "/"}
	c.locks.Set(db, tx)
	return tx, nil
}

type Tx struct {
	c      *Client
	db     string
	prefix string

	updates []func()
}

func (tx *Tx) NextIndex(bucket string) (id uint64, err error) {
	err = tx.c.do("POST", tx.prefix+"seq/"+bucket, 0, &id)
	return
}

func (tx *Tx) SetNextIndex(bucket string, id uint64) (err error) {
	err = tx.c.do("POST", tx.prefix+"seq/"+bucket, id, nil)
	return
}

func (tx *Tx) Get(bucket, key string, v any) error {
	return tx.c.do("GET", tx.prefix+bucket+"/"+key, nil, v)
}

func (tx *Tx) Put(bucket, key string, v any) error {
	if err := tx.c.do("PUT", tx.prefix+bucket+"/"+key, v, nil); err != nil {
		return err
	}
	tx.updates = append(tx.updates, func() {
		tx.c.cache(tx.db).Set(bucket, key, v)
	})
	return nil
}

func (tx *Tx) Delete(bucket, key string) error {
	if err := tx.c.do("DELETE", tx.prefix+bucket+"/"+key, nil, nil); err != nil {
		return nil
	}
	tx.updates = append(tx.updates, func() {
		tx.c.cache(tx.db).DeleteChild(bucket, key)
	})
	return nil
}

func (tx *Tx) Commit() error {
	gotLock := false
	tx.c.locks.Update(func(m map[string]*Tx) {
		if gotLock = m[tx.db] == tx; gotLock {
			delete(m, tx.db)
		}
	})
	if !gotLock {
		return oerrs.Errorf("no lock for %s", tx.db)
	}
	if err := tx.c.do("DELETE", "commitTx/"+tx.db, nil, nil); err != nil {
		return err
	}
	for _, fn := range tx.updates {
		fn()
	}
	return nil
}

func (tx *Tx) Rollback() error {
	gotLock := false
	tx.c.locks.Update(func(m map[string]*Tx) {
		if gotLock = m[tx.db] == tx; gotLock {
			delete(m, tx.db)
		}
	})
	if !gotLock {
		return oerrs.Errorf("no lock for %s", tx.db)
	}
	if err := tx.c.do("DELETE", "rollbackTx/"+tx.db, nil, nil); err != nil {
		return err
	}
	return nil
}

type decCloser struct {
	*msgpack.Decoder
	io.Closer
}

func Get[T any](c *Client, db, bucket, key string) (v T, err error) {
	err = c.Get(db, bucket, key, &v)
	return
}

func ForEach[T any](c *Client, db, bucket string, fn func(key string, v T) error) error {
	var dec decCloser
	if err := c.do("GET", "r/"+db+"/"+bucket, nil, &dec); err != nil {
		return err
	}
	defer dec.Close()
	for {
		var kv [2][]byte
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if len(kv[0]) == 0 {
			continue
		}
		var v T
		if err := genh.UnmarshalMsgpack(kv[1], &v); err != nil {
			log.Printf("%v %v", kv, err)
			return err
		}
		key := otk.UnsafeString(kv[0])
		c.cache(db).Set(bucket, key, v)
		if err := fn(key, v); err != nil {
			return err
		}

	}
}

func ForEachTx[T any](tx *Tx, db, bucket string, fn func(key string, v T) error) error {
	var dec decCloser
	if err := tx.c.do("GET", tx.prefix+bucket, nil, &dec); err != nil {
		return err
	}
	defer dec.Close()
	for {
		var kv [2][]byte
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if len(kv[0]) == 0 {
			continue
		}
		var v T
		if err := genh.UnmarshalMsgpack(kv[1], &v); err != nil {
			return err
		}
		key := otk.UnsafeString(kv[0])
		tx.c.cache(db).Set(bucket, key, v)
		if err := fn(key, v); err != nil {
			return err
		}

	}
}
