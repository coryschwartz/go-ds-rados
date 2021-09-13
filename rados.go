//go:build linux
// +build linux

package rados

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/ceph/go-ceph/rados"
	datastore "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

type Datastore struct {
	conn  *rados.Conn
	ioctx *rados.IOContext
}

func NewDatastore(pool string, namespace string) (*Datastore, error) {
	var err error
	ds := &Datastore{}
	ds.conn, err = rados.NewConn()
	if err != nil {
		return nil, fmt.Errorf("failed to create ceph connection: %w", err)
	}
	err = ds.conn.ReadDefaultConfigFile()
	if err != nil {
		return nil, fmt.Errorf("failed to read default ceph config file: %w", err)
	}
	return setupRados(ds, pool, namespace)
}

func NewDatastoreWithConfig(pool string, namespace string, confPath string) (*Datastore, error) {
	var err error
	ds := &Datastore{}
	ds.conn, err = rados.NewConn()
	if err != nil {
		return nil, fmt.Errorf("failed to create ceph connection: %w", err)
	}
	err = ds.conn.ReadConfigFile(confPath)
	if err != nil {
		return nil, fmt.Errorf("failed read ceph config file: %w", err)
	}
	return setupRados(ds, pool, namespace)
}

func setupRados(ds *Datastore, pool string, namespace string) (*Datastore, error) {
	err := ds.conn.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ceph: %w", err)
	}
	ds.ioctx, err = ds.conn.OpenIOContext(pool)
	if err != nil {
		return nil, fmt.Errorf("failed to open ceph io context: %w", err)
	}
	ds.ioctx.SetNamespace(namespace)
	return ds, nil
}

func (ds *Datastore) Shutdown() {
	ds.conn.Shutdown()
}

func (ds *Datastore) Put(key datastore.Key, value []byte) error {
	return ds.ioctx.WriteFull(key.String(), value)
}

func (ds *Datastore) Get(key datastore.Key) (value []byte, err error) {
	var result bytes.Buffer
	var buf []byte = make([]byte, 1024)
	var offset uint64
	for {
		var count int
		count, err = ds.ioctx.Read(key.String(), buf, offset)
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

func (ds *Datastore) Delete(key datastore.Key) error {
	return ds.ioctx.Delete(key.String())
}

func (ds *Datastore) Query(q dsq.Query) (dsq.Results, error) {
	reschan := make(chan dsq.Result, dsq.KeysOnlyBufSize)
	go func() {
		defer close(reschan)
		iter, err := ds.ioctx.Iter()
		defer iter.Close()
		if err != nil {
			reschan <- dsq.Result{Error: fmt.Errorf("failed to fetch rados iterator: %w", err)}
			return
		}
		for iter.Next() {
			if q.Prefix != "" && !strings.HasPrefix(iter.Value(), q.Prefix) {
				continue
			}
			if q.KeysOnly {
				reschan <- dsq.Result{Entry: dsq.Entry{Key: iter.Value()}}
			} else {
				v, err := ds.Get(datastore.NewKey(iter.Value()))
				if err != nil {
					err = fmt.Errorf("failed to fetch value for key '%s': %w", iter.Value(), err)
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

func (ds *Datastore) Has(key datastore.Key) (exists bool, err error) {
	_, err = ds.ioctx.Stat(key.String())
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

func (ds *Datastore) GetSize(key datastore.Key) (size int, err error) {
	var stat rados.ObjectStat
	stat, err = ds.ioctx.Stat(key.String())
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

func (ds *Datastore) Sync(prefix datastore.Key) (err error) {
	return nil
}

func (ds *Datastore) Batch() (datastore.Batch, error) {
	return datastore.NewBasicBatch(ds), nil
}

func (ds *Datastore) Close() error {
	ds.Shutdown()
	return nil
}
