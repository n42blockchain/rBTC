package main

import (
	"bytes"
	"testing"
)

func TestBtcdCodecVectors(t *testing.T) {
	t.Parallel()
	if got, want := putVLQ(113_931<<1), []byte{0x8c, 0xf3, 0x16}; !bytes.Equal(got, want) {
		t.Fatalf("header VLQ = %x, want %x", got, want)
	}
	dustScript := []byte{0x76, 0xa9, 0x14, 0x10, 0x18, 0x85, 0x36, 0x70, 0xf9, 0xf3, 0xb0, 0x58, 0x2c, 0x5b, 0x9e, 0xe8, 0xce, 0x93, 0x76, 0x4a, 0xc3, 0x2b, 0x93, 0x88, 0xac}
	want := []byte{0x00, 0xa5, 0x2f, 0x00, 0x10, 0x18, 0x85, 0x36, 0x70, 0xf9, 0xf3, 0xb0, 0x58, 0x2c, 0x5b, 0x9e, 0xe8, 0xce, 0x93, 0x76, 0x4a, 0xc3, 0x2b, 0x93}
	if got := encodeCoin(coin{value: 546, height: 0, script: dustScript}); !bytes.Equal(got, want) {
		t.Fatalf("P2PKH coin = %x, want %x", got, want)
	}
}

func TestBtcdOutpointKeyUsesCanonicalVoutVLQ(t *testing.T) {
	t.Parallel()
	id := txid(7, 11)
	tests := []struct {
		vout uint32
		want []byte
	}{
		{127, []byte{0x7f}},
		{128, []byte{0x80, 0x00}},
		{16_384, []byte{0xff, 0x00}},
		{16_512, []byte{0x80, 0x80, 0x00}},
		{^uint32(0), []byte{0x8e, 0xfe, 0xfe, 0xfe, 0x7f}},
	}
	for _, test := range tests {
		key := outpointKeyFromID(id, test.vout)
		if got := key[32:]; !bytes.Equal(got, test.want) {
			t.Fatalf("vout %d = %x, want %x", test.vout, got, test.want)
		}
	}
}

func TestOrderedVarintIsCompactAndNumericallyOrdered(t *testing.T) {
	t.Parallel()
	values := []uint32{0, 1, 127, 128, 255, 256, 16_384, 65_535, 65_536, ^uint32(0)}
	id := txid(9, 7)
	var previous []byte
	for _, value := range values {
		key := encodeBenchmarkOutpointKey(id, value, "ordered-varint")
		if len(key) < 34 || len(key) > 37 {
			t.Fatalf("vout %d produced %d-byte key", value, len(key))
		}
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			t.Fatalf("vout %d key %x does not sort after %x", value, key, previous)
		}
		previous = key
	}
}
