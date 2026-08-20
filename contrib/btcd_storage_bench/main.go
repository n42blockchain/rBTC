package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	defaultUTXOs   = 200_000
	defaultBlocks  = 256
	defaultUpdates = 1_000
	defaultLookups = 100_000
	lookupBatch    = 4_096
)

type workload struct {
	UTXOs           uint32 `json:"utxos"`
	Blocks          uint32 `json:"blocks"`
	UpdatesPerBlock uint32 `json:"updates_per_block"`
	Lookups         uint32 `json:"lookups"`
}

type scenario struct {
	Name            string
	BlocksPerCommit uint32
}

type result struct {
	Backend                        string  `json:"backend"`
	Scenario                       string  `json:"scenario"`
	BlocksPerCommit                uint32  `json:"blocks_per_commit"`
	SeedNS                         int64   `json:"seed_ns"`
	MutationNS                     int64   `json:"mutation_ns"`
	BlocksPerSecond                float64 `json:"blocks_per_second"`
	UTXOChangesPerSecond           float64 `json:"utxo_changes_per_second"`
	LookupNS                       int64   `json:"lookup_ns"`
	LookupsPerSecond               float64 `json:"lookups_per_second"`
	LogicalBytesBeforeCompaction   uint64  `json:"logical_bytes_before_compaction"`
	AllocatedBytesBeforeCompaction uint64  `json:"allocated_bytes_before_compaction"`
	LogicalBytesAfterCompaction    uint64  `json:"logical_bytes_after_compaction"`
	AllocatedBytesAfterCompaction  uint64  `json:"allocated_bytes_after_compaction"`
	CompactionNS                   int64   `json:"compaction_ns"`
}

type report struct {
	SchemaVersion uint32   `json:"schema_version"`
	BtcdReference string   `json:"btcd_reference"`
	Host          string   `json:"host"`
	Boundary      string   `json:"comparison_boundary"`
	Durability    string   `json:"durability"`
	LookupView    string   `json:"lookup_view"`
	KeyFormat     string   `json:"key_format"`
	Workload      workload `json:"workload"`
	Results       []result `json:"results"`
}

type coin struct {
	value  uint64
	height uint32
	script []byte
}

type liveSet struct {
	keys    [][]byte
	coins   []coin
	cursor  int
	updates int
	keyKind string
}

type transition struct {
	height  uint32
	spent   [][]byte
	created []createdCoin
	undos   []undo
}

type createdCoin struct {
	key  []byte
	coin coin
}

type undo struct {
	spent   []createdCoin
	created [][]byte
}

func envUint32(name string, fallback uint32) uint32 {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		panic(fmt.Sprintf("%s must be uint32: %v", name, err))
	}
	return uint32(parsed)
}

func loadWorkload() workload {
	w := workload{
		UTXOs:           envUint32("RBTC_ENGINE_BENCH_UTXOS", defaultUTXOs),
		Blocks:          envUint32("RBTC_ENGINE_BENCH_BLOCKS", defaultBlocks),
		UpdatesPerBlock: envUint32("RBTC_ENGINE_BENCH_UPDATES", defaultUpdates),
		Lookups:         envUint32("RBTC_ENGINE_BENCH_LOOKUPS", defaultLookups),
	}
	if w.UTXOs == 0 || w.UTXOs > 10_000_000 || w.Blocks == 0 || w.Blocks > 10_000 ||
		w.UpdatesPerBlock == 0 || w.UpdatesPerBlock > w.UTXOs ||
		w.Lookups == 0 || w.Lookups > 10_000_000 || w.Blocks%256 != 0 {
		panic("invalid or non-256-aligned workload")
	}
	return w
}

func loadKeyFormat() string {
	format := os.Getenv("RBTC_BTCD_KEY_FORMAT")
	if format == "" {
		return "btcd-vlq"
	}
	if format != "btcd-vlq" && format != "fixed36-be" && format != "ordered-varint" {
		panic("RBTC_BTCD_KEY_FORMAT must be btcd-vlq, ordered-varint, or fixed36-be")
	}
	return format
}

// putVLQ is btcd's canonical MSB VLQ with the continuation offset.
func putVLQ(value uint64) []byte {
	var scratch [10]byte
	offset := len(scratch) - 1
	for {
		scratch[offset] = byte(value & 0x7f)
		if offset != len(scratch)-1 {
			scratch[offset] |= 0x80
		}
		if value <= 0x7f {
			break
		}
		value = (value >> 7) - 1
		offset--
	}
	return append([]byte(nil), scratch[offset:]...)
}

func compressAmount(amount uint64) uint64 {
	if amount == 0 {
		return 0
	}
	exponent := uint64(0)
	for amount%10 == 0 && exponent < 9 {
		amount /= 10
		exponent++
	}
	if exponent < 9 {
		digit := amount % 10
		amount /= 10
		return 1 + 10*(9*amount+digit-1) + exponent
	}
	return 10 + 10*(amount-1)
}

func encodeCoin(c coin) []byte {
	encoded := putVLQ(uint64(c.height) << 1)
	encoded = append(encoded, putVLQ(compressAmount(c.value))...)
	encoded = append(encoded, encodeScript(c.script)...)
	return encoded
}

func encodeScript(script []byte) []byte {
	// These tags and payloads mirror btcd's compressedScriptSize and
	// putCompressedScript. The workload uses P2PKH because it is both a common
	// historical UTXO shape and a pinned upstream btcd test vector.
	if len(script) == 25 && script[0] == 0x76 && script[1] == 0xa9 &&
		script[2] == 0x14 && script[23] == 0x88 && script[24] == 0xac {
		return append([]byte{0x00}, script[3:23]...)
	}
	if len(script) == 23 && script[0] == 0xa9 && script[1] == 0x14 && script[22] == 0x87 {
		return append([]byte{0x01}, script[2:22]...)
	}
	encoded := putVLQ(uint64(len(script) + 6))
	return append(encoded, script...)
}

func txid(generation, index uint32) [32]byte {
	var id [32]byte
	binary.BigEndian.PutUint32(id[0:4], generation)
	binary.BigEndian.PutUint32(id[4:8], index)
	binary.BigEndian.PutUint32(id[8:12], generation*0x9e3779b9)
	return id
}

// outpointKey mirrors btcd chainio.go: wire-order txid plus MSB-VLQ vout.
func outpointKey(generation, index uint32) []byte {
	id := txid(generation, index)
	return outpointKeyFromID(id, index%4)
}

func outpointKeyFromID(id [32]byte, vout uint32) []byte {
	key := append([]byte(nil), id[:]...)
	return append(key, putVLQ(uint64(vout))...)
}

func benchmarkOutpointKey(generation, index uint32, keyKind string) []byte {
	id := txid(generation, index)
	return encodeBenchmarkOutpointKey(id, index%4, keyKind)
}

func encodeBenchmarkOutpointKey(id [32]byte, vout uint32, keyKind string) []byte {
	if keyKind == "btcd-vlq" {
		return outpointKeyFromID(id, vout)
	}
	if keyKind == "ordered-varint" {
		width := 1
		for remaining := vout >> 8; remaining != 0; remaining >>= 8 {
			width++
		}
		key := make([]byte, 33+width)
		copy(key, id[:])
		key[32] = byte(width - 1)
		encoded := vout
		for offset := len(key) - 1; offset >= 33; offset-- {
			key[offset] = byte(encoded)
			encoded >>= 8
		}
		return key
	}
	key := make([]byte, 36)
	copy(key, id[:])
	binary.BigEndian.PutUint32(key[32:], vout)
	return key
}

func utxoDBKey(outpoint []byte) []byte {
	return append([]byte{0x01}, outpoint...)
}

func undoDBKey(height uint32) []byte {
	key := make([]byte, 5)
	key[0] = 0x02
	binary.BigEndian.PutUint32(key[1:], height)
	return key
}

func newCoin(height, index uint32) coin {
	script := make([]byte, 25)
	script[0] = 0x76
	script[1] = 0xa9
	script[2] = 0x14
	for i := 3; i < 23; i++ {
		script[i] = byte(index)
	}
	script[23] = 0x88
	script[24] = 0xac
	return coin{value: 50_000 + uint64(index%10_000), height: height, script: script}
}

func newLiveSet(w workload, keyKind string) *liveSet {
	keys := make([][]byte, w.UTXOs)
	coins := make([]coin, w.UTXOs)
	for i := uint32(0); i < w.UTXOs; i++ {
		keys[i] = benchmarkOutpointKey(0, i, keyKind)
		coins[i] = newCoin(0, i)
	}
	return &liveSet{keys: keys, coins: coins, updates: int(w.UpdatesPerBlock), keyKind: keyKind}
}

func (l *liveSet) transition(height uint32) transition {
	t := transition{height: height, spent: make([][]byte, 0, l.updates), created: make([]createdCoin, 0, l.updates)}
	for pairStart := 0; pairStart < l.updates; pairStart += 2 {
		u := undo{spent: make([]createdCoin, 0, 2), created: make([][]byte, 0, 2)}
		for offset := 0; offset < 2 && pairStart+offset < l.updates; offset++ {
			index := (l.cursor + pairStart + offset) % len(l.keys)
			oldKey := append([]byte(nil), l.keys[index]...)
			oldCoin := l.coins[index]
			newKey := benchmarkOutpointKey(height, uint32(index), l.keyKind)
			newValue := newCoin(height, uint32(index))
			t.spent = append(t.spent, oldKey)
			t.created = append(t.created, createdCoin{key: newKey, coin: newValue})
			u.spent = append(u.spent, createdCoin{key: oldKey, coin: oldCoin})
			u.created = append(u.created, append([]byte(nil), newKey...))
			l.keys[index] = newKey
			l.coins[index] = newValue
		}
		t.undos = append(t.undos, u)
	}
	l.cursor = (l.cursor + l.updates) % len(l.keys)
	return t
}

func appendU32(target []byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return append(target, encoded[:]...)
}

func encodeUndo(undos []undo) []byte {
	encoded := appendU32(nil, 1)
	encoded = appendU32(encoded, uint32(len(undos)))
	for _, u := range undos {
		encoded = appendU32(encoded, uint32(len(u.spent)))
		for _, spent := range u.spent {
			coinBytes := encodeCoin(spent.coin)
			encoded = appendU32(encoded, uint32(len(spent.key)))
			encoded = append(encoded, spent.key...)
			encoded = appendU32(encoded, uint32(len(coinBytes)))
			encoded = append(encoded, coinBytes...)
		}
		encoded = appendU32(encoded, uint32(len(u.created)))
		for _, key := range u.created {
			encoded = appendU32(encoded, uint32(len(key)))
			encoded = append(encoded, key...)
		}
	}
	return encoded
}

func seed(db *leveldb.DB, live *liveSet) time.Duration {
	started := time.Now()
	batch := new(leveldb.Batch)
	for i, key := range live.keys {
		batch.Put(utxoDBKey(key), encodeCoin(live.coins[i]))
	}
	if err := db.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
		panic(err)
	}
	return time.Since(started)
}

func commit(db *leveldb.DB, transitions []transition) {
	created := make(map[string]createdCoin)
	spent := make(map[string][]byte)
	for _, transition := range transitions {
		for _, key := range transition.spent {
			encoded := string(key)
			if _, ok := created[encoded]; ok {
				delete(created, encoded)
			} else {
				spent[encoded] = key
			}
		}
		for _, output := range transition.created {
			created[string(output.key)] = output
		}
	}
	snapshot, err := db.GetSnapshot()
	if err != nil {
		panic(err)
	}
	spentKeys := make([]string, 0, len(spent))
	for key := range spent {
		spentKeys = append(spentKeys, key)
	}
	createdKeys := make([]string, 0, len(created))
	for key := range created {
		createdKeys = append(createdKeys, key)
	}
	sort.Strings(spentKeys)
	sort.Strings(createdKeys)

	for _, encoded := range spentKeys {
		key := spent[encoded]
		if _, err := snapshot.Get(utxoDBKey(key), nil); err != nil {
			panic(fmt.Sprintf("read spent UTXO: %v", err))
		}
	}
	snapshot.Release()

	batch := new(leveldb.Batch)
	for _, encoded := range spentKeys {
		key := spent[encoded]
		batch.Delete(utxoDBKey(key))
	}
	for _, encoded := range createdKeys {
		output := created[encoded]
		batch.Put(utxoDBKey(output.key), encodeCoin(output.coin))
	}
	for _, transition := range transitions {
		batch.Put(undoDBKey(transition.height), encodeUndo(transition.undos))
	}
	var tip [4]byte
	binary.BigEndian.PutUint32(tip[:], transitions[len(transitions)-1].height)
	batch.Put([]byte{0x03}, tip[:])
	if err := db.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
		panic(err)
	}
}

func mutate(db *leveldb.DB, w workload, s scenario, live *liveSet) time.Duration {
	started := time.Now()
	for height := uint32(1); height <= w.Blocks; {
		end := height + s.BlocksPerCommit - 1
		if end > w.Blocks {
			end = w.Blocks
		}
		transitions := make([]transition, 0, end-height+1)
		for next := height; next <= end; next++ {
			transitions = append(transitions, live.transition(next))
		}
		commit(db, transitions)
		height = end + 1
	}
	return time.Since(started)
}

func lookup(db *leveldb.DB, w workload, live *liveSet) time.Duration {
	started := time.Now()
	hits := uint32(0)
	for base := uint32(0); base < w.Lookups; base += lookupBatch {
		end := base + lookupBatch
		if end > w.Lookups {
			end = w.Lookups
		}
		snapshot, err := db.GetSnapshot()
		if err != nil {
			panic(err)
		}
		for ordinal := base; ordinal < end; ordinal++ {
			var key []byte
			if ordinal%4 == 0 {
				key = benchmarkOutpointKey(^uint32(0), ordinal%w.UTXOs, live.keyKind)
			} else {
				key = live.keys[ordinal%w.UTXOs]
			}
			_, err := snapshot.Get(utxoDBKey(key), nil)
			if err == nil {
				hits++
			} else if err != leveldb.ErrNotFound {
				panic(err)
			}
		}
		snapshot.Release()
	}
	expected := w.Lookups - (w.Lookups+3)/4
	if hits != expected {
		panic(fmt.Sprintf("lookup hits %d, expected %d", hits, expected))
	}
	return time.Since(started)
}

func dirSizes(path string) (uint64, uint64) {
	var logical, allocated uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			logical += uint64(info.Size())
			if stat, ok := info.Sys().(*syscall.Stat_t); ok {
				allocated += uint64(stat.Blocks) * 512
			} else {
				allocated += uint64(info.Size())
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return logical, allocated
}

func run(w workload, s scenario, keyKind string) result {
	dir, err := os.MkdirTemp("", "btcd-leveldb-bench-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	db, err := leveldb.OpenFile(dir, &opt.Options{
		Compression: opt.NoCompression,
		Filter:      filter.NewBloomFilter(10),
	})
	if err != nil {
		panic(err)
	}
	live := newLiveSet(w, keyKind)
	seedElapsed := seed(db, live)
	mutationElapsed := mutate(db, w, s, live)
	lookupElapsed := lookup(db, w, live)
	if err := db.Close(); err != nil {
		panic(err)
	}
	beforeLogical, beforeAllocated := dirSizes(dir)
	db, err = leveldb.OpenFile(dir, &opt.Options{
		Compression: opt.NoCompression,
		Filter:      filter.NewBloomFilter(10),
	})
	if err != nil {
		panic(err)
	}
	compactStarted := time.Now()
	if err := db.CompactRange(util.Range{}); err != nil {
		panic(err)
	}
	compactElapsed := time.Since(compactStarted)
	if err := db.Close(); err != nil {
		panic(err)
	}
	afterLogical, afterAllocated := dirSizes(dir)
	seconds := mutationElapsed.Seconds()
	return result{
		Backend:                        "btcd-codec-goleveldb-" + keyKind + "-matched-chainstate",
		Scenario:                       s.Name,
		BlocksPerCommit:                s.BlocksPerCommit,
		SeedNS:                         seedElapsed.Nanoseconds(),
		MutationNS:                     mutationElapsed.Nanoseconds(),
		BlocksPerSecond:                float64(w.Blocks) / seconds,
		UTXOChangesPerSecond:           float64(w.Blocks*w.UpdatesPerBlock*2) / seconds,
		LookupNS:                       lookupElapsed.Nanoseconds(),
		LookupsPerSecond:               float64(w.Lookups) / lookupElapsed.Seconds(),
		LogicalBytesBeforeCompaction:   beforeLogical,
		AllocatedBytesBeforeCompaction: beforeAllocated,
		LogicalBytesAfterCompaction:    afterLogical,
		AllocatedBytesAfterCompaction:  afterAllocated,
		CompactionNS:                   compactElapsed.Nanoseconds(),
	}
}

func main() {
	w := loadWorkload()
	keyFormat := loadKeyFormat()
	report := report{
		SchemaVersion: 1,
		BtcdReference: "btcd v0.26.2 / 05585e037ba0690572208dbc46d121a49cc0c4c9; chainio codec mirrored",
		Host:          runtime.GOOS + "/" + runtime.GOARCH + " " + runtime.Version(),
		Boundary:      "storage-only: btcd key/coin codec plus its pinned Go LevelDB; not block or script validation and not btcd UTXO cache",
		Durability:    "LevelDB Sync=true; UTXO+per-block undo+tip in one WriteBatch",
		LookupView:    "one LevelDB snapshot per 4096 caller-ordered lookups",
		KeyFormat:     keyFormat,
		Workload:      w,
		Results: []result{
			run(w, scenario{Name: "serving", BlocksPerCommit: 1}, keyFormat),
			run(w, scenario{Name: "ibd-256", BlocksPerCommit: 256}, keyFormat),
		},
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(encoded))
	if path := os.Getenv("RBTC_ENGINE_BENCH_REPORT"); path != "" {
		if err := os.WriteFile(path, append(encoded, '\n'), 0o600); err != nil {
			panic(err)
		}
	}

	// Pin both the VLQ and the complete coin encoding against upstream btcd
	// test vectors, so this lane fails loudly if its codec drifts.
	if !bytes.Equal(putVLQ(113_931<<1), []byte{0x8c, 0xf3, 0x16}) {
		panic("btcd VLQ implementation drift")
	}
	dustScript := []byte{0x76, 0xa9, 0x14, 0x10, 0x18, 0x85, 0x36, 0x70, 0xf9, 0xf3, 0xb0, 0x58, 0x2c, 0x5b, 0x9e, 0xe8, 0xce, 0x93, 0x76, 0x4a, 0xc3, 0x2b, 0x93, 0x88, 0xac}
	wantDustCoin := []byte{0x00, 0xa5, 0x2f, 0x00, 0x10, 0x18, 0x85, 0x36, 0x70, 0xf9, 0xf3, 0xb0, 0x58, 0x2c, 0x5b, 0x9e, 0xe8, 0xce, 0x93, 0x76, 0x4a, 0xc3, 0x2b, 0x93}
	if got := encodeCoin(coin{value: 546, height: 0, script: dustScript}); !bytes.Equal(got, wantDustCoin) {
		panic(fmt.Sprintf("btcd P2PKH coin codec drift: got %x", got))
	}
}
