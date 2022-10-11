package rbolt

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.oneofone.dev/genh"
)

type JournalEntry struct {
	TS     int64  `json:"ts,omitempty"`
	Op     string `json:"op,omitempty"`
	DB     string `json:"db,omitempty"`
	Bucket string `json:"bucket,omitempty"`
	Key    string `json:"key,omitempty"`
	Error  string `json:"error,omitempty"`
	Value  any    `json:"value,omitempty"`
}

type Journal struct {
	base    string
	fileFmt string
	useJSON bool

	mux sync.Mutex
	fn  string
	f   *os.File
	enc interface {
		Encode(v any) error
	}
}

func NewJournal(base, fileFmt string, useJSON bool) *Journal {
	return &Journal{
		base:    base,
		fileFmt: fileFmt,
		useJSON: useJSON,
	}
}

func (j *Journal) writer() (_ io.Writer, err error) {
	nfn := time.Now().Format(j.fileFmt)
	if j.useJSON {
		nfn += ".json"
	} else {
		nfn += ".msgp"
	}

	if j.fn == nfn {
		return j.f, nil
	}

	if j.f != nil {
		if err = j.f.Close(); err != nil {
			log.Printf("error closing journal %q: %v", j.f.Name(), err)
		}
	}

	j.fn = nfn
	fp := filepath.Join(j.base, j.fn)
	os.MkdirAll(filepath.Dir(fp), 0o755)

	if j.f, err = os.OpenFile(fp, os.O_CREATE|os.O_RDWR, 0o644); j.f != nil {
		_, err = j.f.Seek(0, io.SeekEnd)
	}

	if err != nil {
		return nil, err
	}

	if j.useJSON {
		j.enc = json.NewEncoder(j.f)
	} else {
		j.enc = genh.NewMsgpackEncoder(j.f)
	}
	return j.f, err
}

func (j *Journal) Write(v *JournalEntry, err error) error {
	v.TS = time.Now().Unix()
	if err != nil {
		v.Error = err.Error()
	}
	j.mux.Lock()
	defer j.mux.Unlock()
	_, err2 := j.writer()
	if err2 != nil {
		return err2
	}
	// js, _ := json.Marshal(v)
	// log.Println(string(js))
	return j.enc.Encode(v)
}

func (j *Journal) Close() error {
	j.mux.Lock()
	defer j.mux.Unlock()
	if f := j.f; f != nil {
		j.f, j.enc = nil, nil
		return f.Close()
	}
	return nil
}
