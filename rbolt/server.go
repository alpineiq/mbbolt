package rbolt

import (
	"context"
	"io"
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
	RespNotFound = gserv.NewMsgpErrorResponse(http.StatusNotFound, "Not Found").Cached()

	endOfList = [2][]byte{nil, nil}
	errorKey  = []byte("___error")

	lg = log.New(log.Default().Writer(), "", log.Lshortfile)
)

const Version = 202203022

func NewServer(dbPath string, dbOpts *mbbolt.Options) *Server {
	srv := &Server{
		s:   gserv.New(gserv.WriteTimeout(time.Minute*10), gserv.ReadTimeout(time.Minute*10), gserv.SetCatchPanics(false)),
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
	ActiveLocks uint32
	Locks       uint32
}

type serverTx struct {
	sync.Mutex
	*mbbolt.Tx
	last atomic.Int64
}

type (
	Server struct {
		s   *gserv.Server
		mdb *mbbolt.MultiDB

		mux  sync.Mutex
		lock genh.LMap[string, *serverTx]
		stats

		MaxUnusedLock time.Duration
	}
)

func (s *Server) init() *Server {
	s.s.Use(func(ctx *gserv.Context) gserv.Response {
		clearHeaders(ctx)
		return nil
	})

	s.s.POST("/beginTx/:db", s.txBegin)
	s.s.DELETE("/commitTx/:db", s.txCommit)
	s.s.DELETE("/rollbackTx/:db", s.txRollback)

	// gserv.MsgPackPut(s.s, "/update", s.handleLock)
	s.s.POST("/tx/:db/nextSeq/:bucket", s.txNextSeq)
	s.s.GET("/tx/:db/:bucket", s.txForEach)
	s.s.GET("/tx/:db/:bucket/:key", s.txGet)
	s.s.PUT("/tx/:db/:bucket/:key", s.txPut)
	s.s.DELETE("/tx/:db/:bucket/:key", s.txDel)

	s.s.POST("/r/:db/nextSeq/:bucket", s.nextSeq)
	s.s.GET("/r/:db/:bucket", s.forEach)
	s.s.GET("/r/:db/:bucket/:key", s.get)
	s.s.PUT("/r/:db/:bucket/:key", s.put)
	s.s.DELETE("/r/:db/:bucket/:key", s.del)

	return s
}

func (s *Server) Run(ctx context.Context, addr string) error {
	return s.s.Run(ctx, addr)
}

func (s *Server) txBegin(ctx *gserv.Context) gserv.Response {
	dbName := ctx.Param("db")

	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err.Error())
	}
	tx, err := db.Begin(true)
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err.Error())
	}

	tts := &serverTx{Tx: tx}
	tts.last.Store(time.Now().UnixNano())
	s.lock.Set(dbName, tts)
	go s.checkLock(dbName)
	return nil
}

func (s *Server) txCommit(ctx *gserv.Context) gserv.Response {
	return s.unlock(ctx.Param("db"), true)
}

func (s *Server) txRollback(ctx *gserv.Context) gserv.Response {
	return s.unlock(ctx.Param("db"), false)
}

func (s *Server) checkLock(dbName string) {
	for tx := s.lock.Get(dbName); tx != nil; tx = s.lock.Get(dbName) {
		if time.Duration(time.Now().UnixNano()-tx.last.Load()) > s.MaxUnusedLock {
			lg.Printf("deleted lock after timeout: %s", dbName)
			tx.Rollback()
			s.lock.Delete(dbName)
		}
	}
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

func (s *Server) unlock(dbName string, commit bool) gserv.Response {
	err := s.withTx(dbName, true, func(tx *mbbolt.Tx) error {
		if commit {
			return tx.Commit()
		}
		return tx.Rollback()
	})
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}

	return nil
}

func (s *Server) txNextSeq(ctx *gserv.Context) gserv.Response {
	dbName, bucket := ctx.Param("db"), ctx.Param("bucket")
	var seq uint64
	var err error
	genh.DecodeMsgpack(ctx, &seq)
	err = s.withTx(dbName, false, func(tx *mbbolt.Tx) error {
		if seq > 0 {
			err = tx.SetNextIndex(bucket, seq)
		} else {
			seq, err = tx.NextIndex(bucket)
		}
		return err
	})
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}
	genh.EncodeMsgpack(ctx, seq)
	return nil
}

func (s *Server) txGet(ctx *gserv.Context) (resp gserv.Response) {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	if err := s.withTx(dbName, false, func(tx *mbbolt.Tx) error {
		if b := tx.GetBytes(bucket, key, true); b != nil {
			_, err := ctx.Write(b)
			return err
		}
		return mbbolt.ErrBucketNotFound
	}); err != nil {
		return RespNotFound
	}
	return nil
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

func (s *Server) txPut(ctx *gserv.Context) gserv.Response {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	val, err := io.ReadAll(ctx.Req.Body)
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}

	if err := s.withTx(dbName, false, func(tx *mbbolt.Tx) error {
		return tx.PutBytes(bucket, key, val)
	}); err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}
	return nil
}

func (s *Server) txDel(ctx *gserv.Context) gserv.Response {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	if err := s.withTx(dbName, false, func(tx *mbbolt.Tx) error {
		return tx.Delete(bucket, key)
	}); err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}

	return nil
}

func (s *Server) nextSeq(ctx *gserv.Context) gserv.Response {
	dbName, bucket := ctx.Param("db"), ctx.Param("bucket")
	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}

	var seq uint64
	genh.DecodeMsgpack(ctx, &seq)
	if err := db.Update(func(tx *mbbolt.Tx) (err error) {
		if seq > 0 {
			return tx.SetNextIndex(bucket, seq)
		}
		seq, err = tx.NextIndex(bucket)
		return
	}); err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}
	genh.EncodeMsgpack(ctx, seq)
	return nil
}

func (s *Server) get(ctx *gserv.Context) gserv.Response {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}

	val, err := db.GetBytes(bucket, key)
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}
	if val != nil {
		ctx.Write(val)
	} else {
		return RespNotFound
	}
	return nil
}

func (s *Server) forEach(ctx *gserv.Context) gserv.Response {
	dbName, bucket := ctx.Param("db"), ctx.Param("bucket")
	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
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

func (s *Server) put(ctx *gserv.Context) gserv.Response {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}

	val, err := io.ReadAll(ctx.Req.Body)
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}

	if err = db.PutBytes(bucket, key, val); err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}
	return nil
}

func (s *Server) del(ctx *gserv.Context) gserv.Response {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}

	if err = db.Delete(bucket, key); err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}
	return nil
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
