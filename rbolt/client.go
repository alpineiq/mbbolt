package rbolt

import (
	"bytes"
	"io"
	"net/http"
	"strings"

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
	}
}

type Client struct {
	c     *http.Client
	locks genh.LMap[string, bool]
	addr  string
}

func (c *Client) Close() error {
	var el oerrs.ErrorList
	for _, db := range c.locks.Keys() {
		el.PushIf(c.Rollback(db))
	}
	return el.Err()
}

func (c *Client) do(method, url string, body []byte, out any) error {
	req, _ := http.NewRequest(method, c.addr+url, bytes.NewReader(body))
	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	// log.Println(method, url, string(body))
	if resp.StatusCode != http.StatusOK {
		var r gserv.MsgpResponse
		if err := genh.DecodeMsgpack(resp.Body, &r); err != nil {
			return oerrs.Errorf("error decoding response for %s %s: %v", method, url, err)
		}
		return r.Errors[0]
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

func (c *Client) Get(db, bucket, key string, v any) error {
	return c.do("GET", "r/"+db+"/"+bucket+"/"+key, nil, v)
}

func (c *Client) Put(db, bucket, key string, v any) error {
	b, err := genh.MarshalMsgpack(v)
	if err != nil {
		return err
	}
	return c.do("PUT", "r/"+db+"/"+bucket+"/"+key, b, nil)
}

func (c *Client) Delete(db, bucket, key string) error {
	return c.do("DELETE", "r/"+db+"/"+bucket+"/"+key, nil, nil)
}

func (c *Client) Update(db string, fn func(tx *Tx) error) error {
	tx, err := c.Begin(db)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		c.Rollback(db)
		return err
	}
	return c.Commit(db)
}

func (c *Client) Begin(db string) (*Tx, error) {
	if err := c.do("POST", "tx/begin/"+db, nil, nil); err != nil {
		return nil, err
	}
	c.locks.Set(db, true)
	return &Tx{c, "tx/" + db + "/"}, nil
}

func (c *Client) Commit(db string) error {
	gotLock := false
	c.locks.Update(func(m map[string]bool) {
		if gotLock = m[db]; gotLock {
			delete(m, db)
		}
	})
	if !gotLock {
		return oerrs.Errorf("no lock for %s", db)
	}
	if err := c.do("DELETE", "tx/commit/"+db, nil, nil); err != nil {
		return err
	}
	return nil
}

func (c *Client) Rollback(db string) error {
	gotLock := false
	c.locks.Update(func(m map[string]bool) {
		if gotLock = m[db]; gotLock {
			delete(m, db)
		}
	})
	if !gotLock {
		return oerrs.Errorf("no lock for %s", db)
	}
	if err := c.do("DELETE", "tx/rollback/"+db, nil, nil); err != nil {
		return err
	}
	return nil
}

type Tx struct {
	c *Client

	prefix string
}

func (tx *Tx) NextIndex(bucket string) (id uint64, err error) {
	err = tx.c.do("POST", tx.prefix+"nextSeq/"+bucket, nil, &id)
	return
}

func (tx *Tx) Get(bucket, key string, v any) error {
	return tx.c.do("GET", tx.prefix+bucket+"/"+key, nil, v)
}

func (tx *Tx) Put(bucket, key string, v any) error {
	b, err := genh.MarshalMsgpack(v)
	if err != nil {
		return err
	}
	return tx.c.do("PUT", tx.prefix+bucket+"/"+key, b, nil)
}

func (tx *Tx) Delete(bucket, key string) error {
	return tx.c.do("DELETE", tx.prefix+bucket+"/"+key, nil, nil)
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
			return err
		}
		if kv[0] == nil && kv[1] == nil {
			return nil
		}
		var v T
		if err := genh.UnmarshalMsgpack(kv[1], &v); err != nil {
			return err
		}
		if err := fn(otk.UnsafeString(kv[0]), v); err != nil {
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
			return err
		}
		if kv[0] == nil && kv[1] == nil {
			return nil
		}
		var v T
		if err := genh.UnmarshalMsgpack(kv[1], &v); err != nil {
			return err
		}
		if err := fn(otk.UnsafeString(kv[0]), v); err != nil {
			return err
		}

	}
}
