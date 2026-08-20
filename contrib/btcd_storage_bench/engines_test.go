package main

import (
	"bytes"
	"testing"
	"time"
)

func TestSelectedEnginesShareAtomicBatchSemantics(t *testing.T) {
	for _, name := range []string{"leveldb", "pebble", "badger", "bbolt"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			engine, err := openEngine(name, root)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := engine.newBatch()
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.put([]byte("old"), []byte("coin")); err != nil {
				t.Fatal(err)
			}
			if err := batch.put([]byte("tip"), []byte{1}); err != nil {
				t.Fatal(err)
			}
			if err := batch.commit(); err != nil {
				t.Fatal(err)
			}
			if err := batch.close(); err != nil {
				t.Fatal(err)
			}

			batch, err = engine.newBatch()
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.delete([]byte("old")); err != nil {
				t.Fatal(err)
			}
			if err := batch.put([]byte("new"), []byte("coin")); err != nil {
				t.Fatal(err)
			}
			if err := batch.put([]byte("tip"), []byte{2}); err != nil {
				t.Fatal(err)
			}
			if err := batch.commit(); err != nil {
				t.Fatal(err)
			}
			if err := batch.close(); err != nil {
				t.Fatal(err)
			}

			snapshot, err := engine.newSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			old, err := snapshot.has([]byte("old"))
			if err != nil {
				t.Fatal(err)
			}
			created, err := snapshot.has([]byte("new"))
			if err != nil {
				t.Fatal(err)
			}
			if old || !created {
				t.Fatalf("unexpected post-commit view: old=%v new=%v", old, created)
			}
			if err := snapshot.close(); err != nil {
				t.Fatal(err)
			}
			if err := engine.compact(); err != nil {
				t.Fatal(err)
			}
			if err := engine.close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := openEngine(name, root)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.close()
			snapshot, err = reopened.newSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.close()
			created, err = snapshot.has([]byte("new"))
			if err != nil || !created {
				t.Fatalf("compacted value missing: found=%v err=%v", created, err)
			}
		})
	}
}

func TestBadgerUsesAtomicTransactionsOnlyForMeasuredMutation(t *testing.T) {
	engine, err := openBadger(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.close()
	measured, err := engine.newBatch()
	if err != nil {
		t.Fatal(err)
	}
	defer measured.close()
	if _, atomic := measured.(*badgerTxnBatch); !atomic {
		t.Fatal("measured Badger mutation must be one transaction")
	}
	prefill, err := engine.newSeedBatch()
	if err != nil {
		t.Fatal(err)
	}
	defer prefill.close()
	if _, bulk := prefill.(*badgerBatch); !bulk {
		t.Fatal("Badger bulk loader must remain confined to unmeasured prefill")
	}
}

func TestSafeRunClosesEachEngineExactlyOnce(t *testing.T) {
	w := workload{UTXOs: 1_000, Blocks: 2, UpdatesPerBlock: 10, Lookups: 100}
	for _, name := range []string{"leveldb", "pebble", "badger", "bbolt"} {
		t.Run(name, func(t *testing.T) {
			got := safeRun(w, scenario{Name: "serving", BlocksPerCommit: 1}, "btcd-vlq", name, time.Second)
			if got.Error != "" {
				t.Fatalf("engine run failed during cleanup: %s", got.Error)
			}
			if !got.ReachedTarget || got.CompletedBlocks != w.Blocks {
				t.Fatalf("unexpected completion: %+v", got)
			}
		})
	}
}

func TestBtcdCodecVectorsRemainPinnedAcrossEngines(t *testing.T) {
	if got := putVLQ(113_931 << 1); !bytes.Equal(got, []byte{0x8c, 0xf3, 0x16}) {
		t.Fatalf("VLQ drift: %x", got)
	}
	script := []byte{0x76, 0xa9, 0x14, 0x10, 0x18, 0x85, 0x36, 0x70, 0xf9, 0xf3, 0xb0, 0x58, 0x2c, 0x5b, 0x9e, 0xe8, 0xce, 0x93, 0x76, 0x4a, 0xc3, 0x2b, 0x93, 0x88, 0xac}
	want := []byte{0x00, 0xa5, 0x2f, 0x00, 0x10, 0x18, 0x85, 0x36, 0x70, 0xf9, 0xf3, 0xb0, 0x58, 0x2c, 0x5b, 0x9e, 0xe8, 0xce, 0x93, 0x76, 0x4a, 0xc3, 0x2b, 0x93}
	if got := encodeCoin(coin{value: 546, script: script}); !bytes.Equal(got, want) {
		t.Fatalf("coin codec drift: %x", got)
	}
}
