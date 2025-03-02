//go:build linux
// +build linux

package rados

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/ceph/go-ceph/rados"
	datastore "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

type Datastore struct {
	conn     *rados.Conn
	confPath string
	pool     string
}

func NewDatastore(confPath string, pool string) (*Datastore, error) {
	var err error
	ds := &Datastore{confPath: confPath, pool: pool}
	ds.conn, err = rados.NewConn()
	if err != nil {
		return nil, err
	}

	if confPath == "" {
		err = ds.conn.ReadDefaultConfigFile()
	} else {
		err = ds.conn.ReadConfigFile(confPath)
	}
	if err != nil {
		return nil, err
	}
	err = ds.conn.Connect()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to rados\n")
		return nil, err
	}
	return ds, nil
}

func (ds *Datastore) Shutdown() {
	ds.conn.Shutdown()
}

func (ds *Datastore) Put(_ context.Context, key datastore.Key, value []byte) error {
	ioctx, err := ds.conn.OpenIOContext(ds.pool)
	if err != nil {
		return err
	}
	defer ioctx.Destroy()
	err = ioctx.WriteFull(key.String(), value)
	return err
}

func (ds *Datastore) Get(_ context.Context, key datastore.Key) (value []byte, err error) {
	var ioctx *rados.IOContext
	ioctx, err = ds.conn.OpenIOContext(ds.pool)
	if err != nil {
		return
	}
	defer ioctx.Destroy()
	var result bytes.Buffer
	var buf []byte = make([]byte, 1024)
	var offset uint64
	for {
		var count int
		count, err = ioctx.Read(key.String(), buf, offset)
		if err != nil {
			if err == rados.RadosErrorNotFound {
				err = datastore.ErrNotFound
				return
			}
			return
		}
		if count < len(buf) {
			result.Write(buf[:count])
			break
		}
		offset += uint64(count)
		result.Write(buf)
	}
	value = result.Bytes()
	return
}

func (ds *Datastore) Delete(_ context.Context, key datastore.Key) error {
	ioctx, err := ds.conn.OpenIOContext(ds.pool)
	if err != nil {
		return err
	}
	defer ioctx.Destroy()
	err = ioctx.Delete(key.String())
	return err
}

func (ds *Datastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	ioctx, err := ds.conn.OpenIOContext(ds.pool)
	if err != nil {
		return nil, err
	}

	reschan := make(chan dsq.Result, dsq.KeysOnlyBufSize)
	go func() {
		defer close(reschan)
		defer ioctx.Destroy()
		iter, err := ioctx.Iter()
		defer iter.Close()
		if err != nil {
			reschan <- dsq.Result{Error: errors.New("Failed to fetch rados iterator")}
			return
		}
		for iter.Next() {
			if q.Prefix != "" && !strings.HasPrefix(iter.Value(), q.Prefix) {
				continue
			}
			if q.KeysOnly {
				reschan <- dsq.Result{Entry: dsq.Entry{Key: iter.Value()}}
			} else {
				v, err := ds.Get(ctx, datastore.NewKey(iter.Value()))
				if err != nil {
					err = fmt.Errorf("Failed to fetch value for key '%s': %w", iter.Value(), err)
					return
				}
				reschan <- dsq.Result{Entry: dsq.Entry{Key: iter.Value(), Value: v}}
			}
		}
	}()
	qr := dsq.ResultsWithChan(q, reschan)

	for _, f := range q.Filters {
		qr = dsq.NaiveFilter(qr, f)
	}
	for _, o := range q.Orders {
		qr = dsq.NaiveOrder(qr, o)
	}
	qr = dsq.NaiveOffset(qr, q.Offset)
	if q.Limit > 0 {
		qr = dsq.NaiveLimit(qr, q.Limit)
	}
	return qr, nil
}

func (ds *Datastore) Has(_ context.Context, key datastore.Key) (exists bool, err error) {
	ioctx, err := ds.conn.OpenIOContext(ds.pool)
	if err != nil {
		return
	}
	defer ioctx.Destroy()
	_, err = ioctx.Stat(key.String())
	if err != nil {
		if err == rados.RadosErrorNotFound {
			err = nil
			return
		}
		return
	} else {
		exists = true
	}
	return
}

func (ds *Datastore) GetSize(_ context.Context, key datastore.Key) (size int, err error) {
	ioctx, err := ds.conn.OpenIOContext(ds.pool)
	if err != nil {
		size = -1
		return
	}
	defer ioctx.Destroy()
	var stat rados.ObjectStat
	stat, err = ioctx.Stat(key.String())
	if err != nil {
		size = -1
		if err == rados.RadosErrorNotFound {
			err = datastore.ErrNotFound
			return
		}
		return
	} else {
		size = int(stat.Size)
	}
	return

}

func (ds *Datastore) Sync(_ context.Context, prefix datastore.Key) (err error) {
	return nil
}

func (ds *Datastore) Batch(_ context.Context) (datastore.Batch, error) {
	return datastore.NewBasicBatch(ds), nil
}

func (ds *Datastore) Close() error {
	ds.Shutdown()
	return nil
}
