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
)

var (
	RespOK       = gserv.NewMsgpResponse(nil).Cached()
	RespNotFound = gserv.NewError(http.StatusNotFound, "Not Found")

	endOfList = [2][]byte{nil, nil}
	errorKey  = []byte("___error")

	lg = log.New(log.Default().Writer(), "", log.Lshortfile)
)

const Version = 202203022

func NewServer(dbPath string, dbOpts *mbbolt.Options) *Server {
	srv := &Server{
		s:   gserv.New(gserv.WriteTimeout(time.Minute*10), gserv.ReadTimeout(time.Minute*10), gserv.SetCatchPanics(true)),
		mdb: mbbolt.NewMultiDB(dbPath, ".mdb", dbOpts),

		MaxUnusedLock: time.Minute,
	}
	return srv.init()
}

func (s *Server) Close() error {
	s.s.Shutdown(time.Second)
	return s.mdb.Close()
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

func (s *Server) getStats(ctx *gserv.Context) (stats, error) {
	return s.stats, nil
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
		return nil, gserv.ErrNotFound
	}
	return
}

func (s *Server) txForEach(ctx *gserv.Context) gserv.Response {
	dbName, bucket := ctx.Param("db"), ctx.Param("bucket")
	enc := genh.NewMsgpackEncoder(ctx)

	if err := s.withTx(dbName, false, func(tx *mbbolt.Tx) error {
		return tx.ForEachBytes(bucket, func(key, val []byte) error {
			err := enc.Encode([2][]byte{key, val})
			ctx.Flush()
			return err
		})
	}); err != nil {
		enc.Encode([2][]byte{errorKey, []byte(err.Error())})
	}

	enc.Encode(endOfList)

	return nil
}

func (s *Server) txPut(ctx *gserv.Context, v []byte) (string, error) {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	if err := s.withTx(dbName, false, func(tx *mbbolt.Tx) error {
		return tx.PutBytes(bucket, key, v)
	}); err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}
	return "", nil
}

func (s *Server) txDel(ctx *gserv.Context) (string, error) {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	if err := s.withTx(dbName, false, func(tx *mbbolt.Tx) error {
		return tx.Delete(bucket, key)
	}); err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}

	return "", nil
}

func (s *Server) nextSeq(ctx *gserv.Context, seq uint64) (uint64, error) {
	dbName, bucket := ctx.Param("db"), ctx.Param("bucket")
	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return 0, gserv.NewError(http.StatusInternalServerError, err)
	}

	if err := db.Update(func(tx *mbbolt.Tx) (err error) {
		if seq > 0 {
			return tx.SetNextIndex(bucket, seq)
		}
		seq, err = tx.NextIndex(bucket)
		return
	}); err != nil {
		return 0, gserv.NewError(http.StatusInternalServerError, err)
	}
	return seq, nil
}

func (s *Server) get(ctx *gserv.Context) ([]byte, error) {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return nil, gserv.NewError(http.StatusInternalServerError, err)
	}

	b, _ := db.GetBytes(bucket, key)
	if b == nil {
		return nil, gserv.ErrNotFound
	}

	return b, nil
}

func (s *Server) forEach(ctx *gserv.Context) gserv.Response {
	dbName, bucket := ctx.Param("db"), ctx.Param("bucket")
	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		ctx.EncodeCodec(gserv.MsgpCodec{}, http.StatusInternalServerError, gserv.NewError(http.StatusInternalServerError, err))
		return nil
	}

	enc := genh.NewMsgpackEncoder(ctx)
	if db.View(func(tx *mbbolt.Tx) error {
		return tx.ForEachBytes(bucket, func(key, val []byte) error {
			return enc.Encode([2][]byte{key, val})
		})
	}) != nil {
		enc.Encode([2][]byte{errorKey, []byte(err.Error())})
		ctx.Flush()
	}
	enc.Encode(endOfList)
	return nil
}

func (s *Server) put(ctx *gserv.Context, val []byte) (string, error) {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}

	if err = db.PutBytes(bucket, key, val); err != nil {
		return "", gserv.NewError(http.StatusInternalServerError, err)
	}
	return "OK", nil
}

func (s *Server) del(ctx *gserv.Context) (string, error) {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
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
