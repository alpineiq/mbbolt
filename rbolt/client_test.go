package rbolt

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"go.oneofone.dev/oerrs"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

type S struct {
	A string
	B int64
	C float64
	S *S
}

func TestClient(t *testing.T) {
	rbs := NewServer(t.TempDir(), nil)
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
			tx.NextIndex("b")
			tx.NextIndex("b")
			tx.NextIndex("b")
			tx.NextIndex("b")
			id, err := tx.NextIndex("b")
			if err != nil {
				return err
			}
			if id != 5 {
				return oerrs.Errorf("expected 5, got %d", id)
			}
			if err := tx.Put("b", strconv.Itoa(int(1000+id)), &S{A: "test", S: &S{B: int64(id)}}); err != nil {
				return err
			}

			found := false
			if err := ForEachTx(tx, "db", "b", func(key string, ss *S) error {
				if key == "1005" {
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
}
