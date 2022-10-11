package rbolt

import (
	"context"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.oneofone.dev/genh"
	"go.oneofone.dev/gserv"
	"go.oneofone.dev/mbbolt"
	"go.oneofone.dev/oerrs"
)

var (
	RespOK       = gserv.NewMsgpResponse(nil).Cached()
	RespNotFound = gserv.NewError(http.StatusNotFound, "Not Found")

	lg = log.New(log.Default().Writer(), "", log.Lshortfile)
)

const Version = 202203022

func NewServer(dbPath string, dbOpts *mbbolt.Options) *Server {
	srv := &Server{
		s:   gserv.New(gserv.WriteTimeout(time.Minute*10), gserv.ReadTimeout(time.Minute*10), gserv.SetCatchPanics(true)),
		mdb: mbbolt.NewMultiDB(dbPath, ".db", dbOpts),
		j:   NewJournal(dbPath, "logs/2006/01/02", true),

		MaxUnusedLock: time.Minute,
	}
	return srv.init()
}

func (s *Server) Close() error {
	var el oerrs.ErrorList
	el.PushIf(s.s.Close())
	s.s.Close()
	if s.j != nil {
		el.PushIf(s.j.Close())
	}
	el.PushIf(s.mdb.Close())
	return el.Err()
}

type stats struct {
	ActiveLocks genh.AtomicInt64 `json:"activeLocks,omitempty"`
	Locks       genh.AtomicInt64 `json:"locks,omitempty"`
	Timeouts    genh.AtomicInt64 `json:"timeouts,omitempty"`
}

type serverTx struct {
	sync.Mutex
	last atomic.Int64
	*mbbolt.Tx
}

type (
	Server struct {
		s   *gserv.Server
		mdb *mbbolt.MultiDB
		j   *Journal

		mux   sync.Mutex
		lock  genh.LMap[string, *serverTx]
		stats stats

		MaxUnusedLock time.Duration
		AuthKey       string
	}
)

func (s *Server) init() *Server {
	s.s.Use(func(ctx *gserv.Context) gserv.Response {
		if s.AuthKey != "" && ctx.Req.Header.Get("Authorization") != s.AuthKey {
			ctx.EncodeCodec(gserv.MsgpCodec{}, http.StatusUnauthorized, "Unauthorized")
			return nil
		}
		clearHeaders(ctx)
		return nil
	})

	gserv.MsgpGet(s.s, "/stats", s.getStats, false)
	gserv.JSONGet(s.s, "/stats.json", s.getStats, false)
	gserv.MsgpPost(s.s, "/beginTx/:db", s.txBegin, false)
	gserv.MsgpDelete(s.s, "/commitTx/:db", s.txCommit, false)
	gserv.MsgpDelete(s.s, "/rollbackTx/:db", s.txRollback, false)

	// gserv.MsgPackPut(s.s, "/update", s.handleLock)
	gserv.MsgpPost(s.s, "/tx/:db/seq/:bucket", s.txNextSeq, false)
	s.s.GET("/tx/:db/:bucket", s.txForEach)
	gserv.MsgpGet(s.s, "/tx/:db/:bucket/:key", s.txGet, false)
	gserv.MsgpPut(s.s, "/tx/:db/:bucket/:key", s.txPut, false)
	gserv.MsgpDelete(s.s, "/tx/:db/:bucket/:key", s.txDel, false)

	gserv.MsgpPost(s.s, "/r/:db/seq/:bucket", s.nextSeq, false)
	s.s.GET("/r/:db/:bucket", s.forEach)
	gserv.MsgpGet(s.s, "/r/:db/:bucket/:key", s.get, false)
	gserv.MsgpPut(s.s, "/r/:db/:bucket/:key", s.put, false)
	gserv.MsgpDelete(s.s, "/r/:db/:bucket/:key", s.del, false)

	return s
}

func (s *Server) Run(ctx context.Context, addr string) error {
	return s.s.Run(ctx, addr)
}

func (s *Server) getStats(ctx *gserv.Context) (*stats, error) {
	return &s.stats, nil
}

func (s *Server) txBegin(ctx *gserv.Context, req any) (string, error) {
	dbName := ctx.Param("db")

	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}
	s.j.Write(&JournalEntry{Op: "txBegin", DB: dbName}, err)

	tts := &serverTx{Tx: tx}
	tts.last.Store(time.Now().UnixNano())
	s.lock.Set(dbName, tts)
	s.stats.Locks.Add(1)
	s.stats.ActiveLocks.Add(1)
	go s.checkLock(dbName)
	return "OK", nil
}

func (s *Server) txCommit(ctx *gserv.Context) (string, error) {
	return s.unlock(ctx.Param("db"), true)
}

func (s *Server) txRollback(ctx *gserv.Context) (string, error) {
	return s.unlock(ctx.Param("db"), false)
}

func (s *Server) unlock(dbName string, commit bool) (string, error) {
	err := s.withTx(dbName, true, func(tx *mbbolt.Tx) error {
		if commit {
			return tx.Commit()
		}
		return tx.Rollback()
	})
	je := &JournalEntry{DB: dbName}
	if commit {
		je.Op = "txCommit"
	} else {
		je.Op = "txRollback"
	}
	s.j.Write(je, err)
	if err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}

	return "OK", nil
}

func (s *Server) checkLock(dbName string) {
	for tx := s.lock.Get(dbName); tx != nil; tx = s.lock.Get(dbName) {
		if time.Duration(time.Now().UnixNano()-tx.last.Load()) > s.MaxUnusedLock {
			tx.Lock()
			lg.Printf("deleted stale lock: %s", dbName)
			tx.Rollback()
			s.lock.Delete(dbName)
			s.stats.Timeouts.Add(1)
			tx.Unlock()
			break
		}
		time.Sleep(time.Second)
	}
	s.stats.ActiveLocks.Add(-1)
}

func (s *Server) withTx(dbName string, rm bool, fn func(tx *mbbolt.Tx) error) error {
	tx := s.lock.Get(dbName)
	if tx == nil {
		return gserv.ErrNotFound
	}
	tx.Lock()
	defer tx.Unlock()
	if rm {
		s.lock.Delete(dbName)
	}

	tx.last.Store(time.Now().UnixNano())
	return fn(tx.Tx)
}

func (s *Server) txNextSeq(ctx *gserv.Context, seq uint64) (uint64, error) {
	dbName, bucket := ctx.Param("db"), ctx.Param("bucket")
	err := s.withTx(dbName, false, func(tx *mbbolt.Tx) (err error) {
		if seq > 0 {
			err = tx.SetNextIndex(bucket, seq)
		} else {
			seq, err = tx.NextIndex(bucket)
		}
		return
	})
	je := &JournalEntry{Op: "txNextSeq", DB: dbName, Bucket: bucket, Value: seq}
	s.j.Write(je, err)
	if err != nil {
		return 0, gserv.NewError(http.StatusInternalServerError, err)
	}
	return seq, nil
}

func (s *Server) txGet(ctx *gserv.Context) (out []byte, err error) {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	s.withTx(dbName, false, func(tx *mbbolt.Tx) error {
		out = tx.GetBytes(bucket, key, true)
		return nil
	})

	if len(out) == 0 {
		out, err = nil, gserv.ErrNotFound
	}

	je := &JournalEntry{Op: "txGet", DB: dbName, Bucket: bucket, Key: key, Value: out}
	s.j.Write(je, err)

	return
}

func (s *Server) txForEach(ctx *gserv.Context) gserv.Response {
	dbName, bucket := ctx.Param("db"), ctx.Param("bucket")
	enc := genh.NewMsgpackEncoder(ctx)

	err := s.withTx(dbName, false, func(tx *mbbolt.Tx) error {
		return tx.ForEachBytes(bucket, func(key, val []byte) error {
			err := enc.Encode([2][]byte{key, val})
			ctx.Flush()
			return err
		})
	})

	je := &JournalEntry{Op: "txForEach", DB: dbName, Bucket: bucket}
	s.j.Write(je, err)

	return nil
}

func (s *Server) txPut(ctx *gserv.Context, v []byte) (_ string, err error) {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	je := &JournalEntry{Op: "txPut", DB: dbName, Bucket: bucket, Key: key, Value: v}
	defer s.j.Write(je, err)

	if err := s.withTx(dbName, false, func(tx *mbbolt.Tx) error {
		return tx.PutBytes(bucket, key, v)
	}); err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}
	return "OK", nil
}

func (s *Server) txDel(ctx *gserv.Context) (_ string, err error) {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	je := &JournalEntry{Op: "txDelete", DB: dbName, Bucket: bucket, Key: key}
	defer s.j.Write(je, err)

	if err := s.withTx(dbName, false, func(tx *mbbolt.Tx) error {
		return tx.Delete(bucket, key)
	}); err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}

	return "OK", nil
}

func (s *Server) nextSeq(ctx *gserv.Context, seq uint64) (_ uint64, err error) {
	dbName, bucket := ctx.Param("db"), ctx.Param("bucket")
	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return 0, gserv.NewError(http.StatusInternalServerError, err)
	}

	err = db.Update(func(tx *mbbolt.Tx) (err error) {
		if seq > 0 {
			return tx.SetNextIndex(bucket, seq)
		}
		seq, err = tx.NextIndex(bucket)
		return
	})
	if err != nil {
		seq, err = 0, gserv.NewError(http.StatusInternalServerError, err)
	}
	je := &JournalEntry{Op: "nextSeq", DB: dbName, Bucket: bucket, Value: seq}
	s.j.Write(je, err)
	return
}

func (s *Server) get(ctx *gserv.Context) ([]byte, error) {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return nil, gserv.NewError(http.StatusInternalServerError, err)
	}

	out, _ := db.GetBytes(bucket, key)
	if len(out) == 0 {
		out, err = nil, gserv.ErrNotFound
	}

	je := &JournalEntry{Op: "get", DB: dbName, Bucket: bucket, Key: key, Value: out}
	s.j.Write(je, err)

	return out, err
}

func (s *Server) forEach(ctx *gserv.Context) gserv.Response {
	dbName, bucket := ctx.Param("db"), ctx.Param("bucket")
	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		ctx.EncodeCodec(gserv.MsgpCodec{}, http.StatusInternalServerError, gserv.NewError(http.StatusInternalServerError, err))
		return nil
	}

	enc := genh.NewMsgpackEncoder(ctx)
	err = db.View(func(tx *mbbolt.Tx) error {
		return tx.ForEachBytes(bucket, func(key, val []byte) error {
			err := enc.Encode([2][]byte{key, val})
			ctx.Flush()
			return err
		})
	})

	je := &JournalEntry{Op: "forEach", DB: dbName, Bucket: bucket}
	s.j.Write(je, err)

	return nil
}

func (s *Server) put(ctx *gserv.Context, v []byte) (_ string, err error) {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	je := &JournalEntry{Op: "put", DB: dbName, Bucket: bucket, Key: key, Value: v}
	defer s.j.Write(je, err)

	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}

	if err = db.PutBytes(bucket, key, v); err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}
	return "OK", nil
}

func (s *Server) del(ctx *gserv.Context) (_ string, err error) {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	je := &JournalEntry{Op: "delete", DB: dbName, Bucket: bucket, Key: key}
	defer s.j.Write(je, err)

	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}

	if err = db.Delete(bucket, key); err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}
	return "OK", nil
}

func splitPath(p string) (out []string) {
	p = strings.TrimPrefix(strings.TrimSuffix(p, "/"), "/")
	return strings.Split(p, "/")
}

func clearHeaders(ctx *gserv.Context) {
	h := ctx.Header()
	h["Date"] = nil
	h["Content-Type"] = nil
}
