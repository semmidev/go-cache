package cache

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
)

// Konfigurasi parameter Count-Min Sketch untuk TinyLFU.
const (
	// sketchRows adalah jumlah baris tabel hash (depth) pada Count-Min Sketch.
	// Menggunakan 4 baris independen untuk meminimalkan probabilitas collision.
	sketchRows = 4

	// sketchWidth adalah lebar tiap baris tabel hash (number of buckets).
	// Harus bernilai pangkat 2 agar operasi modulo dapat dioptimalkan menjadi bitwise AND.
	sketchWidth = 1024

	// sketchMask digunakan untuk bitwise AND hash dengan (sketchWidth - 1).
	sketchMask = sketchWidth - 1

	// maxCounter adalah nilai maksimum counter 4-bit (15).
	// Pembatasan 4-bit memadai untuk membedakan item populer tanpa memboroskan memori.
	maxCounter = 15
)

// tinyLFU mengimplementasikan Count-Min Sketch sebagai filter frekuensi probabilistik (TinyLFU).
// Berdasarkan spesifikasi TinyUFO, struktur ini memperkirakan frekuensi
// akses sebuah key dengan penggunaan memori yang sangat efisien.
type tinyLFU struct {
	mu        sync.RWMutex
	counters  [sketchRows][sketchWidth]uint8
	threshold uint64
	total     atomic.Uint64
}

// newTinyLFU membuat instance baru dari tinyLFU dengan threshold reset default.
func newTinyLFU() *tinyLFU {
	return &tinyLFU{threshold: sketchWidth * 10}
}

// Increment menambah estimasi frekuensi untuk sebuah hash key di seluruh baris Count-Min Sketch.
// Menggunakan konstanta perkalian rasio emas (Golden Ratio multiplier: 0x9e3779b97f4a7c15)
// untuk menghasilkan index baris independen (Knuth Multiplicative Hashing).
//
// Mekanisme Aging (Halving):
// Jika akumulasi increment melampaui `threshold`, seluruh counter dalam sketch dibagi 2 (halved).
// Hal ini mencegah "stale frequency problem" sehingga item yang dulu populer namun sekarang jarang
// diakses tidak mendominasi cache selamanya.
func (t *tinyLFU) Increment(hash uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Cek aging threshold: Lakukan pembagian 2 (decaying/halving) seluruh counter
	if t.total.Load() > t.threshold {
		for r := range sketchRows {
			for c := range sketchWidth {
				t.counters[r][c] /= 2
			}
		}
		t.total.Store(0)
	}

	// Update counter untuk tiap baris dengan hash indepeden
	for r := range sketchRows {
		idx := (hash + uint64(r)*0x9e3779b97f4a7c15) & sketchMask
		if t.counters[r][idx] < maxCounter {
			t.counters[r][idx]++
		}
	}
	t.total.Add(1)
}

// Estimate mengembalikan estimasi frekuensi akses terendah (minimum value across rows)
// dari sebuah hash key. Mengambil nilai minimum mengurangi kesalahan overestimate akibat hash collision.
func (t *tinyLFU) Estimate(hash uint64) int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := int(maxCounter)
	for r := range sketchRows {
		idx := (hash + uint64(r)*0x9e3779b97f4a7c15) & sketchMask
		result = min(result, int(t.counters[r][idx]))
	}
	return result
}

// Reset mengosongkan seluruh counter dalam sketch dan mereset statistik total increment.
func (t *tinyLFU) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for r := range sketchRows {
		clear(t.counters[r][:])
	}
	t.total.Store(0)
}

// globalSeed digunakan sebagai perancak awal fungsi hash maphash secara global.
var globalSeed = maphash.MakeSeed()

