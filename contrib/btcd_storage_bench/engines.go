package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/dgraph-io/badger/v4"
	badgerOptions "github.com/dgraph-io/badger/v4/options"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"go.etcd.io/bbolt"
)

type kvSnapshot interface {
	has(key []byte) (bool, error)
	close() error
}

type kvBatch interface {
	put(key, value []byte) error
	delete(key []byte) error
	commit() error
	close() error
}

type kvEngine interface {
	newSnapshot() (kvSnapshot, error)
	newSeedBatch() (kvBatch, error)
	newBatch() (kvBatch, error)
	compact() error
	close() error
}

func openEngine(name, root string) (kvEngine, error) {
	switch name {
	case "leveldb":
		return openLevelDB(filepath.Join(root, "db"))
	case "pebble":
		return openPebble(filepath.Join(root, "db"))
	case "badger":
		return openBadger(filepath.Join(root, "db"))
	case "bbolt":
		return openBBolt(filepath.Join(root, "chainstate.db"))
	default:
		return nil, fmt.Errorf("unsupported engine %q", name)
	}
}

type levelDBEngine struct {
	db *leveldb.DB
}

func openLevelDB(path string) (*levelDBEngine, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{
		Compression: opt.NoCompression,
		Filter:      filter.NewBloomFilter(10),
	})
	if err != nil {
		return nil, err
	}
	return &levelDBEngine{db: db}, nil
}

type levelDBSnapshot struct {
	snapshot *leveldb.Snapshot
}

func (s *levelDBSnapshot) has(key []byte) (bool, error) {
	value, err := s.snapshot.Get(key, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(value) > 0 {
		_ = value[len(value)-1]
	}
	return true, nil
}

func (s *levelDBSnapshot) close() error {
	s.snapshot.Release()
	return nil
}

type levelDBBatch struct {
	db    *leveldb.DB
	batch leveldb.Batch
}

func (b *levelDBBatch) put(key, value []byte) error {
	b.batch.Put(key, value)
	return nil
}

func (b *levelDBBatch) delete(key []byte) error {
	b.batch.Delete(key)
	return nil
}

func (b *levelDBBatch) commit() error {
	return b.db.Write(&b.batch, &opt.WriteOptions{Sync: true})
}

func (b *levelDBBatch) close() error { return nil }

func (e *levelDBEngine) newSnapshot() (kvSnapshot, error) {
	snapshot, err := e.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &levelDBSnapshot{snapshot: snapshot}, nil
}

func (e *levelDBEngine) newBatch() (kvBatch, error) {
	return &levelDBBatch{db: e.db}, nil
}

func (e *levelDBEngine) newSeedBatch() (kvBatch, error) { return e.newBatch() }

func (e *levelDBEngine) compact() error {
	return e.db.CompactRange(util.Range{})
}

func (e *levelDBEngine) close() error { return e.db.Close() }

type pebbleEngine struct {
	db *pebble.DB
}

type quietPebbleLogger struct{}

func (quietPebbleLogger) Infof(string, ...interface{}) {}

func (quietPebbleLogger) Errorf(format string, arguments ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", arguments...)
}

func (quietPebbleLogger) Fatalf(format string, arguments ...interface{}) {
	panic(fmt.Sprintf(format, arguments...))
}

func pebbleOptions() *pebble.Options {
	options := &pebble.Options{}
	options.EnsureDefaults()
	options.Logger = quietPebbleLogger{}
	policy := bloom.FilterPolicy(10)
	for index := range options.Levels {
		options.Levels[index].Compression = func() *sstable.CompressionProfile { return sstable.NoCompression }
		options.Levels[index].FilterPolicy = policy
	}
	return options
}

func openPebble(path string) (*pebbleEngine, error) {
	db, err := pebble.Open(path, pebbleOptions())
	if err != nil {
		return nil, err
	}
	return &pebbleEngine{db: db}, nil
}

type pebbleSnapshot struct {
	snapshot *pebble.Snapshot
}

func (s *pebbleSnapshot) has(key []byte) (bool, error) {
	value, closer, err := s.snapshot.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(value) > 0 {
		_ = value[len(value)-1]
	}
	return true, closer.Close()
}

func (s *pebbleSnapshot) close() error { return s.snapshot.Close() }

type pebbleBatch struct {
	batch *pebble.Batch
}

func (b *pebbleBatch) put(key, value []byte) error {
	return b.batch.Set(key, value, nil)
}

func (b *pebbleBatch) delete(key []byte) error {
	return b.batch.Delete(key, nil)
}

func (b *pebbleBatch) commit() error { return b.batch.Commit(pebble.Sync) }

func (b *pebbleBatch) close() error { return b.batch.Close() }

func (e *pebbleEngine) newSnapshot() (kvSnapshot, error) {
	return &pebbleSnapshot{snapshot: e.db.NewSnapshot()}, nil
}

func (e *pebbleEngine) newBatch() (kvBatch, error) {
	return &pebbleBatch{batch: e.db.NewBatch()}, nil
}

func (e *pebbleEngine) newSeedBatch() (kvBatch, error) { return e.newBatch() }

func (e *pebbleEngine) compact() error {
	if err := e.db.Flush(); err != nil {
		return err
	}
	return e.db.Compact(context.Background(), []byte{0x00}, []byte{0xff}, true)
}

func (e *pebbleEngine) close() error { return e.db.Close() }

type badgerEngine struct {
	db *badger.DB
}

func openBadger(path string) (*badgerEngine, error) {
	options := badger.DefaultOptions(path).
		WithLogger(nil).
		WithCompression(badgerOptions.None).
		WithSyncWrites(true)
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	return &badgerEngine{db: db}, nil
}

type badgerSnapshot struct {
	txn *badger.Txn
}

func (s *badgerSnapshot) has(key []byte) (bool, error) {
	item, err := s.txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, err = item.ValueCopy(nil)
	return err == nil, err
}

func (s *badgerSnapshot) close() error {
	s.txn.Discard()
	return nil
}

type badgerBatch struct {
	batch *badger.WriteBatch
}

func (b *badgerBatch) put(key, value []byte) error { return b.batch.Set(key, value) }

func (b *badgerBatch) delete(key []byte) error { return b.batch.Delete(key) }

func (b *badgerBatch) commit() error { return b.batch.Flush() }

func (b *badgerBatch) close() error {
	b.batch.Cancel()
	return nil
}

type badgerTxnBatch struct {
	txn       *badger.Txn
	committed bool
}

func (b *badgerTxnBatch) put(key, value []byte) error { return b.txn.Set(key, value) }

func (b *badgerTxnBatch) delete(key []byte) error { return b.txn.Delete(key) }

func (b *badgerTxnBatch) commit() error {
	if err := b.txn.Commit(); err != nil {
		return err
	}
	b.committed = true
	return nil
}

func (b *badgerTxnBatch) close() error {
	if !b.committed {
		b.txn.Discard()
	}
	return nil
}

func (e *badgerEngine) newSnapshot() (kvSnapshot, error) {
	return &badgerSnapshot{txn: e.db.NewTransaction(false)}, nil
}

func (e *badgerEngine) newBatch() (kvBatch, error) {
	return &badgerTxnBatch{txn: e.db.NewTransaction(true)}, nil
}

func (e *badgerEngine) newSeedBatch() (kvBatch, error) {
	return &badgerBatch{batch: e.db.NewWriteBatch()}, nil
}

func (e *badgerEngine) compact() error {
	if err := e.db.Flatten(1); err != nil {
		return err
	}
	for {
		err := e.db.RunValueLogGC(0.5)
		if errors.Is(err, badger.ErrNoRewrite) {
			break
		}
		if err != nil {
			return err
		}
	}
	return e.db.Sync()
}

func (e *badgerEngine) close() error { return e.db.Close() }

var bboltBucket = []byte("chainstate")

type bboltEngine struct {
	db   *bbolt.DB
	path string
}

func openBBolt(path string) (*bboltEngine, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{FreelistType: bbolt.FreelistMapType})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bboltBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &bboltEngine{db: db, path: path}, nil
}

type bboltSnapshot struct {
	tx *bbolt.Tx
}

func (s *bboltSnapshot) has(key []byte) (bool, error) {
	value := s.tx.Bucket(bboltBucket).Get(key)
	if len(value) > 0 {
		_ = value[len(value)-1]
	}
	return value != nil, nil
}

func (s *bboltSnapshot) close() error { return s.tx.Rollback() }

type bboltBatch struct {
	tx        *bbolt.Tx
	bucket    *bbolt.Bucket
	committed bool
}

func (b *bboltBatch) put(key, value []byte) error { return b.bucket.Put(key, value) }

func (b *bboltBatch) delete(key []byte) error { return b.bucket.Delete(key) }

func (b *bboltBatch) commit() error {
	if err := b.tx.Commit(); err != nil {
		return err
	}
	b.committed = true
	return nil
}

func (b *bboltBatch) close() error {
	if b.committed {
		return nil
	}
	return b.tx.Rollback()
}

func (e *bboltEngine) newSnapshot() (kvSnapshot, error) {
	tx, err := e.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &bboltSnapshot{tx: tx}, nil
}

func (e *bboltEngine) newBatch() (kvBatch, error) {
	tx, err := e.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &bboltBatch{tx: tx, bucket: tx.Bucket(bboltBucket)}, nil
}

func (e *bboltEngine) newSeedBatch() (kvBatch, error) { return e.newBatch() }

func (e *bboltEngine) compact() error {
	destinationPath := e.path + ".compact"
	if err := os.Remove(destinationPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	destination, err := bbolt.Open(destinationPath, 0o600, nil)
	if err != nil {
		return err
	}
	if err := bbolt.Compact(destination, e.db, 64*1024*1024); err != nil {
		_ = destination.Close()
		return err
	}
	if err := destination.Close(); err != nil {
		return err
	}
	if err := e.db.Close(); err != nil {
		return err
	}
	e.db = nil
	oldPath := e.path + ".precompact"
	if err := os.Remove(oldPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(e.path, oldPath); err != nil {
		return err
	}
	if err := os.Rename(destinationPath, e.path); err != nil {
		_ = os.Rename(oldPath, e.path)
		return err
	}
	if err := os.Remove(oldPath); err != nil {
		return err
	}
	reopened, err := bbolt.Open(e.path, 0o600, &bbolt.Options{FreelistType: bbolt.FreelistMapType})
	if err != nil {
		return err
	}
	e.db = reopened
	return nil
}

func (e *bboltEngine) close() error {
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

var _ io.Closer = (*pebble.Snapshot)(nil)
