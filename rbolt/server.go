package rbolt

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
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
)

const Version = 202203022

func NewServer(dbPath string, dbOpts *mbbolt.Options) *Server {
	srv := &Server{
		s:   gserv.New(gserv.WriteTimeout(time.Minute*10), gserv.ReadTimeout(time.Minute*10), gserv.SetCatchPanics(false)),
		mdb: mbbolt.NewMultiDB(dbPath, ".mdb", dbOpts),
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

type (
	locksMap = map[string]*mbbolt.Tx

	Server struct {
		s   *gserv.Server
		mdb *mbbolt.MultiDB

		ActiveConnectionSem chan struct{}

		mux  sync.Mutex
		lock genh.LMap[string, *mbbolt.Tx]

		stats
	}
)

func (s *Server) init() *Server {
	s.s.Use(func(ctx *gserv.Context) gserv.Response {
		clearHeaders(ctx)
		return nil
	})

	s.s.POST("/tx/begin/:db", s.txBegin)
	s.s.DELETE("/tx/commit/:db", s.txCommit)
	s.s.DELETE("/tx/rollback/:db", s.txRollback)

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

	s.lock.Set(dbName, tx)
	return nil
}

func (s *Server) txCommit(ctx *gserv.Context) gserv.Response {
	return s.unlock(ctx.Param("db"), true)
}

func (s *Server) txRollback(ctx *gserv.Context) gserv.Response {
	return s.unlock(ctx.Param("db"), false)
}

func (s *Server) unlock(dbName string, commit bool) gserv.Response {
	tx := s.lock.Swap(dbName, nil)
	if tx == nil {
		return RespNotFound
	}

	var err error
	if commit {
		err = tx.Commit()
	} else {
		err = tx.Rollback()
	}

	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}

	return nil
}

func (s *Server) txNextSeq(ctx *gserv.Context) gserv.Response {
	dbName, bucket := ctx.Param("db"), ctx.Param("bucket")
	tx := s.lock.Get(dbName)
	if tx == nil {
		return RespNotFound
	}

	var seq uint64
	var err error
	genh.DecodeMsgpack(ctx, &seq)
	if seq > 0 {
		err = tx.SetNextIndex(bucket, seq)
	} else {
		seq, err = tx.NextIndex(bucket)
	}
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}
	genh.EncodeMsgpack(ctx, seq)
	return nil
}

func (s *Server) txGet(ctx *gserv.Context) gserv.Response {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	tx := s.lock.Get(dbName)
	if tx == nil {
		return RespNotFound
	}
	if b := tx.GetBytes(bucket, key, true); b != nil {
		ctx.Write(b)
	} else {
		return RespNotFound
	}
	return nil
}

func (s *Server) txForEach(ctx *gserv.Context) gserv.Response {
	dbName, bucket := ctx.Param("db"), ctx.Param("bucket")
	tx := s.lock.Get(dbName)
	if tx == nil {
		return RespNotFound
	}

	enc := genh.NewMsgpackEncoder(ctx)

	if err := tx.ForEachBytes(bucket, func(key, val []byte) error {
		return enc.Encode([2][]byte{key, val})
	}); err != nil {
		enc.Encode([2][]byte{errorKey, []byte(err.Error())})
	}

	enc.Encode(endOfList)

	return nil
}

func (s *Server) txPut(ctx *gserv.Context) gserv.Response {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	tx := s.lock.Get(dbName)
	if tx == nil {
		return RespNotFound
	}

	val, err := io.ReadAll(ctx.Req.Body)
	if err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}

	if err := tx.PutBytes(bucket, key, val); err != nil {
		return gserv.NewMsgpErrorResponse(http.StatusInternalServerError, err)
	}
	return nil
}

func (s *Server) txDel(ctx *gserv.Context) gserv.Response {
	dbName, bucket, key := ctx.Param("db"), ctx.Param("bucket"), ctx.Param("key")
	tx := s.lock.Get(dbName)
	if tx == nil {
		return RespNotFound
	}

	if err := tx.Delete(bucket, key); err != nil {
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
