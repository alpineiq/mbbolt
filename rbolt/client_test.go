package rbolt

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

type S struct {
	S *S
	A string
	B int64
	C float64
}

func TestClient(t *testing.T) {
	rbs := NewServer(t.TempDir(), nil)
	rbs.MaxUnusedLock = time.Second / 10
	defer rbs.Close()
	go rbs.Run(context.Background(), ":0")

	time.Sleep(time.Millisecond * 100)
	url := "http://" + rbs.s.Addrs()[0]
	t.Log("srv addr", url)

	t.Run("NoTx", func(t *testing.T) {
		c := NewClient(url)
		defer c.Close()
		sp := &S{A: "test", B: 123, C: 123.456, S: &S{A: "-", B: 321, C: 654.321}}
		if err := c.Put("db", "bucket", "key", sp); err != nil {
			t.Fatal(err)
		}

		var s S
		if err := c.Get("db", "bucket", "key", &s); err != nil {
			t.Fatal(err)
		}

		if s.A != sp.A || s.B != sp.B || s.C != sp.C || s.S.A != sp.S.A || s.S.B != sp.S.B || s.S.C != sp.S.C {
			t.Fatal("invalid data", s, sp)
		}

		found := false
		if err := ForEach(c, "db", "bucket", func(key string, ss *S) error {
			if key == "key" && ss.A == "test" && ss.B == 123 && ss.C == 123.456 {
				found = true
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if !found {
			t.Fatal("foreach failed")
		}

		if err := c.Delete("db", "bucket", "key"); err != nil {
			t.Fatal(err)
		}

		if err := c.Get("db", "bucket", "key", &s); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("Tx", func(t *testing.T) {
		c := NewClient(url)
		defer c.Close()
		if err := c.Update("db", func(tx *Tx) error {
			for i := 0; i < 100; i++ {
				id, err := tx.NextIndex("b")
				if err != nil {
					return err
				}
				if err := tx.Put("b", strconv.Itoa(int(id)+1000), &S{A: "test", S: &S{B: int64(id)}}); err != nil {
					return err
				}
			}

			found := false
			if err := ForEachTx(tx, "db", "b", func(key string, ss *S) error {
				if key == "1005" {
					if ss.S == nil || ss.S.B != 5 {
						t.Fatal("unexpected value", ss, ss.S)
					}
					found = true
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			if !found {
				t.Fatal("foreach failed")
			}

			var s S
			if err := tx.Get("b", "1005", &s); err != nil || s.A != "test" || s.S.B != 5 {
				t.Fatal("expected error", s)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		var s S
		if err := c.Get("db", "b", "1005", &s); err != nil {
			t.Fatal(err)
		}
		if s.S == nil || s.S.B != 5 {
			t.Fatal("expected s.S.B == 5")
		}

		if err := c.Update("db", func(tx *Tx) error {
			if err := tx.Delete("b", "1005"); err != nil {
				return err
			}
			var s S
			if err := tx.Get("b", "1005", &s); err == nil {
				t.Fatal("expected error", s)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if err := c.Get("db", "b", "1005", &s); err == nil {
			t.Fatal(err)
		}
	})
	t.Run("AutoUnlock", func(t *testing.T) {
		c := NewClient(url)
		defer c.Close()
		err := c.Update("db", func(tx *Tx) error {
			if err := tx.Put("b", "1005", &S{A: "test", S: &S{B: 5}}); err != nil {
				return err
			}
			time.Sleep(time.Second / 2)
			if err := tx.Put("b", "1005", &S{A: "test", S: &S{B: 5}}); err == nil {
				t.Fatal("expected error")
			}
			return nil
		})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}
