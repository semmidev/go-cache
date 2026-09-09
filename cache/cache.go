package cache

import (
	"hash/maphash"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// defaultShards mendefinisikan jumlah shard internal pada MemoryCache (64 shards).
	// Menggunakan 64 shards berbasis power-of-two untuk penguncian terpisah (Lock Striping)
	// dan komputasi modulo cepat via bitwise AND (`hash & shardMask`).
	defaultShards = 64

	// smallRatio adalah rasio ukuran Small Queue pada algoritma S3-FIFO (10% dari total kapasitas shard).
	// Small Queue berfungsi menampung item baru untuk memfilter "one-hit wonders" (item yang hanya diakses sekali).
	smallRatio = 0.1

	// defaultCap adalah kapasitas total default untuk MemoryCache seluruh shard (1000 item).
	defaultCap = 1000
)

// hashKey menghitung nilai hash uint64 dari key ber-type generic K.
// Menggunakan fast path untuk tipe data standar (string, int, int64, uint64, int32, uint32)
// dan maphash fallback berbasis byte representation untuk tipe data comparable lainnya.
func hashKey[K comparable](k K) uint64 {
	switch v := any(k).(type) {
	case string:
		return maphash.String(globalSeed, v)
	case int:
		return uint64(v)
	case int64:
		return uint64(v)
	case uint64:
		return v
	case int32:
		return uint64(v)
	case uint32:
		return uint64(v)
	default:
		var h maphash.Hash
		h.SetSeed(globalSeed)
		ptr := unsafe.Pointer(&k)
		size := unsafe.Sizeof(k)
		h.Write((*[1 << 30]byte)(ptr)[:size:size])
		return h.Sum64()
	}
}

// entry merepresentasikan elemen data tunggal yang disimpan di dalam cache shard.
type entry[K comparable, V any] struct {
	key      K      // Key penyimpan
	value    V      // Nilai payload
	hash     uint64 // Nilai hash uint64 dari key
	freq     int8   // Frekuensi akses internal (digunakan untuk promosi & second-chance FIFO)
	expireAt int64  // Timestamp kadaluarsa dalam nanosekond (0 berarti tanpa TTL)
}

// expired menguji apakah item sudah melampaui masa berlaku (TTL).
func (e *entry[K, V]) expired(now int64) bool {
	if e.expireAt == 0 {
		return false
	}
	return now > e.expireAt
}

// shard mengimplementasikan sub-cache mandiri dengan penguncian sync.RWMutex tersendiri.
// Struktur ini menyimpan tabel hash utama, dua antrean FIFO (smallQueue & mainQueue) untuk S3-FIFO,
// serta Count-Min Sketch (tinyLFU) untuk admission control.
type shard[K comparable, V any] struct {
	mu         sync.RWMutex
	table      map[K]*entry[K, V]
	smallQueue []*entry[K, V]
	mainQueue  []*entry[K, V]
	smallCap   int
	mainCap    int
	cap        int
	sketch     *tinyLFU
}

// newShard mengalokasikan dan menginisialisasi shard baru dengan kapasitas tertentu.
func newShard[K comparable, V any](cap int) *shard[K, V] {
	smallCap := max(int(float64(cap)*smallRatio), 1)
	mainCap := max(cap-smallCap, 1)
	return &shard[K, V]{
		table:      make(map[K]*entry[K, V], cap),
		smallQueue: make([]*entry[K, V], 0, smallCap+1),
		mainQueue:  make([]*entry[K, V], 0, mainCap+1),
		smallCap:   smallCap,
		mainCap:    mainCap,
		cap:        cap,
		sketch:     newTinyLFU(),
	}
}

// MemoryCache adalah struktur data in-memory cache utama yang mengimplementasikan
// arsitektur TinyUFO + S3-FIFO untuk performa tinggi.
//
// Keunggulan Arsitektur:
// 1. **Lock Striping**: Terbagi menjadi 64 shard independen dengan sync.RWMutex untuk throughput tinggi.
// 2. **S3-FIFO Eviction**: Kombinasi Small Queue (10%) & Main Queue (90%) dengan frekuensi promosi.
// 3. **TinyLFU Admission**: Count-Min Sketch probabilistik untuk menyaring item tidak populer saat cache penuh.
// 4. **Atomic Metrics**: Pelacakan statistik hit & miss berkinerja tinggi tanpa lock global.
type MemoryCache[K comparable, V any] struct {
	shards    []*shard[K, V]
	shardMask uint64
	cap       int
	hits      atomic.Uint64
	misses    atomic.Uint64
}

// cacheConfig menyimpan konfigurasi pembuatan instance MemoryCache.
type cacheConfig struct{ cap int }

// Option didefinisikan sebagai fungsi konfigurasi functional options pattern.
type Option func(*cacheConfig)

// WithCapacity mengatur kapasitas maksimum total elemen yang disimpan di dalam MemoryCache.
func WithCapacity(c int) Option {
	return func(cfg *cacheConfig) { cfg.cap = c }
}

// New membuat dan menginisialisasi instance MemoryCache baru dengan opsi konfigurasi yang diberikan.
func New[K comparable, V any](opts ...Option) *MemoryCache[K, V] {
	cfg := cacheConfig{cap: defaultCap}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.cap < defaultShards {
		cfg.cap = defaultShards
	}
	perShard := cfg.cap / defaultShards
	if perShard < 4 {
		perShard = 4
	}
	shards := make([]*shard[K, V], defaultShards)
	for i := range defaultShards {
		shards[i] = newShard[K, V](perShard)
	}
	return &MemoryCache[K, V]{
		shards:    shards,
		shardMask: defaultShards - 1,
		cap:       cfg.cap,
	}
}

// getShard mengembalikan pointer ke shard yang bertanggung jawab atas hash key tertentu.
func (c *MemoryCache[K, V]) getShard(hash uint64) *shard[K, V] {
	return c.shards[hash&c.shardMask]
}

// Get mengambil nilai yang terhubung dengan key dari cache.
// Mengembalikan (value, true) jika ditemukan dan belum kadaluarsa, atau (zero, false) jika tidak ada/expired.
//
// Operasi Get akan:
// 1. Memperbarui statistik hit/miss secara atomik.
// 2. Melakukan registrasi frekuensi ke TinyLFU Sketch.
// 3. Mempromosikan item ke Main Queue jika frekuensi akses >= 2 (S3-FIFO promotion).
func (c *MemoryCache[K, V]) Get(key K) (V, bool) {
	h := hashKey(key)
	s := c.getShard(h)
	now := time.Now().UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.table[key]
	if !ok {
		c.misses.Add(1)
		var zero V
		return zero, false
	}
	if e.expired(now) {
		delete(s.table, key)
		s.removeFromQueues(key)
		c.misses.Add(1)
		var zero V
		return zero, false
	}

	if e.freq < 10 {
		e.freq++
	}
	s.sketch.Increment(h)
	c.hits.Add(1)

	// S3-FIFO: Promosikan dari Small Queue ke Main Queue jika item sering diakses (freq >= 2)
	if e.freq >= 2 {
		s.promoteToMain(e)
	}
	return e.value, true
}

// Contains memeriksa apakah key ada di dalam cache dan belum kadaluarsa tanpa mengubah counter statistik.
func (c *MemoryCache[K, V]) Contains(key K) bool {
	_, ok := c.Get(key)
	return ok
}

// Put menyimpan pasangan key-value ke dalam cache dengan opsional durasi TTL (Time-To-Live).
// Jika cache sudah penuh, Put akan berkonsultasi dengan TinyLFU Admission Policy (shouldAdmit).
// Jika kandidat baru memiliki estimasi frekuensi lebih rendah daripada item korban di antrean,
// item baru akan ditolak (dibuang) demi menjaga efisiensi cache hit ratio.
func (c *MemoryCache[K, V]) Put(key K, value V, ttl ...time.Duration) {
	var exp int64
	if len(ttl) > 0 && ttl[0] > 0 {
		exp = time.Now().Add(ttl[0]).UnixNano()
	}

	h := hashKey(key)
	s := c.getShard(h)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Update jika key sudah ada
	if e, ok := s.table[key]; ok {
		e.value = value
		e.expireAt = exp
		if e.freq < 10 {
			e.freq++
		}
		s.sketch.Increment(h)
		return
	}

	// TinyLFU Admission Check jika shard sudah penuh
	if len(s.table) >= s.cap {
		if !s.shouldAdmit(h) {
			return
		}
		s.evict()
	}

	e := &entry[K, V]{key: key, value: value, hash: h, freq: 1, expireAt: exp}
	s.table[key] = e
	s.smallQueue = append(s.smallQueue, e)
	s.sketch.Increment(h)

	if len(s.smallQueue) > s.smallCap {
		s.evictSmall()
	}
}

// ForcePut memasukkan key-value secara paksa tanpa melalui TinyLFU Admission Policy check.
// Fungsi ini biasa digunakan oleh Read-Through Cache (RTCache) untuk menjamin bahwa data
// hasil lookup dari database utama selalu disimpan ke dalam cache.
func (c *MemoryCache[K, V]) ForcePut(key K, value V, ttl ...time.Duration) {
	var exp int64
	if len(ttl) > 0 && ttl[0] > 0 {
		exp = time.Now().Add(ttl[0]).UnixNano()
	}

	h := hashKey(key)
	s := c.getShard(h)

	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.table[key]; ok {
		e.value = value
		e.expireAt = exp
		if e.freq < 10 {
			e.freq++
		}
		return
	}

	if len(s.table) >= s.cap {
		s.evict()
	}

	e := &entry[K, V]{key: key, value: value, hash: h, freq: 1, expireAt: exp}
	s.table[key] = e
	s.smallQueue = append(s.smallQueue, e)
}

// shouldAdmit membandingkan estimasi frekuensi kandidat baru vs victim di antrean Small Queue via TinyLFU.
func (s *shard[K, V]) shouldAdmit(hash uint64) bool {
	if len(s.table) < s.cap {
		return true
	}
	if len(s.smallQueue) == 0 {
		return true
	}
	victim := s.smallQueue[0]
	return s.sketch.Estimate(hash) >= s.sketch.Estimate(victim.hash)
}

// evict melakukan pembukaan ruang dengan mengevakuasi item dari Small Queue atau Main Queue.
func (s *shard[K, V]) evict() {
	if len(s.smallQueue) > 0 {
		s.evictSmall()
	} else {
		s.evictMain()
	}
}

// evictSmall memproses item terdepan di Small Queue.
// Jika item pernah diakses (freq > 1), item dipromosikan ke Main Queue bukannya dihapus.
func (s *shard[K, V]) evictSmall() {
	if len(s.smallQueue) == 0 {
		return
	}
	victim := s.smallQueue[0]
	s.smallQueue = s.smallQueue[1:]

	if victim.freq > 1 {
		s.mainQueue = append(s.mainQueue, victim)
		if len(s.mainQueue) > s.mainCap {
			s.evictMain()
		}
		return
	}
	delete(s.table, victim.key)
}

// evictMain mengeksekusi algoritma Second-Chance FIFO pada Main Queue.
// Jika item terdepan memiliki freq > 1, frekuensinya dikurangi dan dimasukkan kembali ke buntut antrean.
// Jika freq <= 1, item tersebut dihapus dari tabel cache.
func (s *shard[K, V]) evictMain() {
	if len(s.mainQueue) == 0 {
		s.evictSmall()
		return
	}
	victim := s.mainQueue[0]
	s.mainQueue = s.mainQueue[1:]

	if victim.freq > 1 {
		victim.freq--
		s.mainQueue = append(s.mainQueue, victim)
		return
	}
	delete(s.table, victim.key)
}

// promoteToMain memindahkan entry dari Small Queue ke Main Queue.
func (s *shard[K, V]) promoteToMain(e *entry[K, V]) {
	if idx := slices.IndexFunc(s.smallQueue, func(en *entry[K, V]) bool {
		return en.key == e.key
	}); idx >= 0 {
		s.smallQueue = slices.Delete(s.smallQueue, idx, idx+1)
	}
	if slices.IndexFunc(s.mainQueue, func(en *entry[K, V]) bool {
		return en.key == e.key
	}) >= 0 {
		return
	}
	s.mainQueue = append(s.mainQueue, e)
	if len(s.mainQueue) > s.mainCap {
		s.evictMain()
	}
}

// removeFromQueues menghapus item dari Small Queue maupun Main Queue berdasarkan key.
func (s *shard[K, V]) removeFromQueues(key K) {
	if idx := slices.IndexFunc(s.smallQueue, func(e *entry[K, V]) bool {
		return e.key == key
	}); idx >= 0 {
		s.smallQueue = slices.Delete(s.smallQueue, idx, idx+1)
	}
	if idx := slices.IndexFunc(s.mainQueue, func(e *entry[K, V]) bool {
		return e.key == key
	}); idx >= 0 {
		s.mainQueue = slices.Delete(s.mainQueue, idx, idx+1)
	}
}

// Remove menghapus entri dengan key tertentu dari cache jika ada.
func (c *MemoryCache[K, V]) Remove(key K) {
	h := hashKey(key)
	s := c.getShard(h)
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.table, key)
	s.removeFromQueues(key)
}

// Clear mengosongkan seluruh entri dari semua shard dan mereset statistik hits/misses.
func (c *MemoryCache[K, V]) Clear() {
	for _, s := range c.shards {
		s.mu.Lock()
		s.table = make(map[K]*entry[K, V], s.cap)
		s.smallQueue = s.smallQueue[:0]
		s.mainQueue = s.mainQueue[:0]
		s.sketch.Reset()
		s.mu.Unlock()
	}
	c.hits.Store(0)
	c.misses.Store(0)
}

// Len mengembalikan jumlah total elemen aktif (termasuk yang mungkin sudah expired tetapi belum dibersihkan) di dalam seluruh shard.
func (c *MemoryCache[K, V]) Len() int {
	total := 0
	for _, s := range c.shards {
		s.mu.RLock()
		total += len(s.table)
		s.mu.RUnlock()
	}
	return total
}

// Cap mengembalikan total kapasitas maksimum penyimpanan MemoryCache.
func (c *MemoryCache[K, V]) Cap() int { return c.cap }

// Stats mengembalikan statistik (hits, misses, current_size) dari cache saat ini.
func (c *MemoryCache[K, V]) Stats() (hits, misses uint64, size int) {
	return c.hits.Load(), c.misses.Load(), c.Len()
}

// Keys mengembalikan slice berisi seluruh key aktif yang ada di dalam cache.
func (c *MemoryCache[K, V]) Keys() []K {
	keys := make([]K, 0, c.Len())
	for _, s := range c.shards {
		s.mu.RLock()
		for k := range s.table {
			keys = append(keys, k)
		}
		s.mu.RUnlock()
	}
	return keys
}
