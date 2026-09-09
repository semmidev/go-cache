package cache

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	// defaultLRUShards mendefinisikan jumlah shard default untuk LRUCache (64 shards).
	defaultLRUShards = 64
	// defaultLRUCap mendefinisikan kapasitas item default (1000 item).
	defaultLRUCap = 1000
)

// lruShard mengimplementasikan sub-cache LRU mandiri dengan penguncian sync.RWMutex tersendiri.
type lruShard[K comparable, V any] struct {
	mu     sync.RWMutex
	items  map[K]*lruNode[K, V]
	list   *lruList[K, V]
	weight int64
}

// newLRUShard mengalokasikan dan menginisialisasi shard LRU baru.
func newLRUShard[K comparable, V any](cap int) *lruShard[K, V] {
	return &lruShard[K, V]{
		items: make(map[K]*lruNode[K, V], cap),
		list:  newLRUList[K, V](),
	}
}

// LRUCache merepresentasikan struktur data In-Memory LRU Cache berkinerja tinggi,
// terinspirasi oleh arsitektur Cloudflare Pingora (pingora-lru).
//
// Fitur Utama:
// 1. **Lock-Striped Sharding**: Terbagi menjadi N shard (default 64) untuk throughput tinggi tanpa global lock.
// 2. **Weighted LRU**: Setiap entri mendukung bobot (misal ukuran byte), dikendalikan oleh total weight limit.
// 3. **Power of Two Choices (P2C) Eviction**: Eviction global memilih 2 shard secara acak dan mengevakuasi dari shard yang memiliki beban lebih tinggi.
// 4. **PromoteTopN Optimization**: Pengecekan cepat dengan Read Lock (RLock) untuk memastikan item di N posisi teratas sebelum mengambil Write Lock.
// 5. **Atomic Metrics & Bookkeeping**: Pelacakan hit, miss, total len, dan weight tanpa lock global.
type LRUCache[K comparable, V any] struct {
	shards        []*lruShard[K, V]
	shardLens     []atomic.Int64
	shardMask     uint64
	numShards     int
	cap           int
	weightLimit   int64
	watermark     int
	len           atomic.Int64
	weight        atomic.Int64
	hits          atomic.Uint64
	misses        atomic.Uint64
	evictedLen    atomic.Uint64
	evictedWeight atomic.Uint64
	seed          uint64
}

// lruConfig menyimpan parameter konfigurasi pembuatan instance LRUCache.
type lruConfig struct {
	cap         int
	weightLimit int64
	watermark   int
	shards      int
}

// LRUOption didefinisikan sebagai fungsi konfigurasi functional options pattern untuk LRUCache.
type LRUOption func(*lruConfig)

// WithLRUCapacity mengatur kapasitas maksimum total elemen yang disimpan di dalam LRUCache.
func WithLRUCapacity(c int) LRUOption {
	return func(cfg *lruConfig) { cfg.cap = c }
}

// WithWeightLimit mengatur batas bobot kumulatif maksimum (misal total memori dalam byte) untuk LRUCache.
func WithWeightLimit(limit int64) LRUOption {
	return func(cfg *lruConfig) { cfg.weightLimit = limit }
}

// WithWatermark mengatur ambang batas item (watermark) yang memicu pencopotan (eviction).
func WithWatermark(watermark int) LRUOption {
	return func(cfg *lruConfig) { cfg.watermark = watermark }
}

// WithLRUShards mengatur jumlah shard independen pada LRUCache (harus power of two, misal 16, 32, 64, 128).
func WithLRUShards(shards int) LRUOption {
	return func(cfg *lruConfig) { cfg.shards = shards }
}

// NewLRU membuat dan menginisialisasi instance LRUCache baru berdasarkan opsi yang diberikan.
func NewLRU[K comparable, V any](opts ...LRUOption) *LRUCache[K, V] {
	cfg := lruConfig{
		cap:    defaultLRUCap,
		shards: defaultLRUShards,
	}
	for _, o := range opts {
		o(&cfg)
	}

	// Normalisasi jumlah shard agar power-of-two
	shards := cfg.shards
	if shards <= 0 || (shards&(shards-1)) != 0 {
		shards = defaultLRUShards
	}

	if cfg.cap < shards {
		cfg.cap = shards
	}
	perShardCap := max(cfg.cap/shards, 4)

	shardList := make([]*lruShard[K, V], shards)
	shardLens := make([]atomic.Int64, shards)
	for i := range shards {
		shardList[i] = newLRUShard[K, V](perShardCap)
	}

	watermark := cfg.watermark
	if watermark <= 0 {
		watermark = cfg.cap
	}

	return &LRUCache[K, V]{
		shards:      shardList,
		shardLens:   shardLens,
		shardMask:   uint64(shards - 1),
		numShards:   shards,
		cap:         cfg.cap,
		weightLimit: cfg.weightLimit,
		watermark:   watermark,
		seed:        uint64(time.Now().UnixNano()),
	}
}

// getShard mengembalikan shard yang bertanggung jawab untuk hash key tertentu.
func (c *LRUCache[K, V]) getShard(hash uint64) (int, *lruShard[K, V]) {
	idx := int(hash & c.shardMask)
	return idx, c.shards[idx]
}

// Get mengambil nilai dari LRUCache berdasarkan key.
// Memperbarui posisi item ke paling depan (MRU) jika ditemukan dan belum kadaluarsa.
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	h := hashKey(key)
	_, s := c.getShard(h)
	now := time.Now().UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.items[key]
	if !ok {
		c.misses.Add(1)
		var zero V
		return zero, false
	}

	if node.expired(now) {
		s.list.remove(node)
		delete(s.items, key)
		s.weight -= node.weight
		c.weight.Add(-node.weight)
		c.len.Add(-1)
		c.shardLens[int(h&c.shardMask)].Add(-1)
		c.misses.Add(1)
		var zero V
		return zero, false
	}

	s.list.moveToFront(node)
	c.hits.Add(1)
	return node.value, true
}

// Contains memeriksa ketersediaan key tanpa mengubah posisi LRU order maupun statistik hit/miss.
func (c *LRUCache[K, V]) Contains(key K) bool {
	h := hashKey(key)
	_, s := c.getShard(h)
	now := time.Now().UnixNano()

	s.mu.RLock()
	defer s.mu.RUnlock()

	node, ok := s.items[key]
	if !ok {
		return false
	}
	return !node.expired(now)
}

// Put menyimpan pasangan key-value ke dalam LRUCache dengan bobot default 1 dan opsi durasi TTL.
func (c *LRUCache[K, V]) Put(key K, value V, ttl ...time.Duration) {
	c.PutWithWeight(key, value, 1, ttl...)
}

// PutWithWeight menyimpan pasangan key-value ke dalam LRUCache dengan bobot spesifik (misal byte size).
// Jika batas kapasitas (watermark) atau batas bobot (weightLimit) terlampaui,
// evict_to_limit akan dipanggil secara otomatis via Power of Two Choices (P2C).
func (c *LRUCache[K, V]) PutWithWeight(key K, value V, weight int64, ttl ...time.Duration) {
	if weight < 1 {
		weight = 1
	}

	var exp int64
	if len(ttl) > 0 && ttl[0] > 0 {
		exp = time.Now().Add(ttl[0]).UnixNano()
	}

	h := hashKey(key)
	shardIdx, s := c.getShard(h)

	s.mu.Lock()

	// Update jika key sudah ada
	if existing, ok := s.items[key]; ok {
		existing.value = value
		existing.expireAt = exp
		weightDiff := weight - existing.weight
		existing.weight = weight
		s.weight += weightDiff
		s.list.moveToFront(existing)
		s.mu.Unlock()

		c.weight.Add(weightDiff)
		c.evictToLimitIfNeeded()
		return
	}

	// Tambah item baru
	node := &lruNode[K, V]{
		key:      key,
		value:    value,
		hash:     h,
		weight:   weight,
		expireAt: exp,
	}

	s.items[key] = node
	s.list.pushFront(node)
	s.weight += weight
	s.mu.Unlock()

	c.len.Add(1)
	c.weight.Add(weight)
	c.shardLens[shardIdx].Add(1)

	c.evictToLimitIfNeeded()
}

// Promote memindahkan item ke posisi terdepan (MRU). Mengembalikan true jika key ditemukan.
func (c *LRUCache[K, V]) Promote(key K) bool {
	h := hashKey(key)
	_, s := c.getShard(h)

	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.items[key]
	if !ok {
		return false
	}
	s.list.moveToFront(node)
	return true
}

// PromoteTopN mempromosikan item ke MRU hanya jika item tersebut TIDAK berada dalam topN posisi teratas.
//
// Fitur Optimalisasi Pingora:
// Memakai Read-Lock (RLock) terlebih dahulu untuk mengecek `isInTopN`. Jika sudah berada di topN,
// fungsi langsung mengembalikan true TANPA perlu mengambil Write-Lock (WLock). Ini sangat mengurangi
// lock contention pada beban kerja high-concurrency read.
func (c *LRUCache[K, V]) PromoteTopN(key K, topN int) bool {
	h := hashKey(key)
	_, s := c.getShard(h)

	// Step 1: Read-Lock fast path
	s.mu.RLock()
	node, ok := s.items[key]
	if !ok {
		s.mu.RUnlock()
		return false
	}
	if s.list.isInTopN(node, topN) {
		s.mu.RUnlock()
		return true
	}
	s.mu.RUnlock()

	// Step 2: Write-Lock slow path jika perlu di-promote
	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-check di bawah lock write
	node, ok = s.items[key]
	if !ok {
		return false
	}
	s.list.moveToFront(node)
	return true
}

// Remove menghapus entri dari LRUCache jika ditemukan.
func (c *LRUCache[K, V]) Remove(key K) bool {
	h := hashKey(key)
	shardIdx, s := c.getShard(h)

	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.items[key]
	if !ok {
		return false
	}

	s.list.remove(node)
	delete(s.items, key)
	s.weight -= node.weight

	c.len.Add(-1)
	c.weight.Add(-node.weight)
	c.shardLens[shardIdx].Add(-1)
	return true
}

// EvictShard mengevakuasi 1 item paling lama tidak digunakan (LRU tail) dari shard tertentu.
func (c *LRUCache[K, V]) EvictShard(shardIdx int) (K, V, int64, bool) {
	if shardIdx < 0 || shardIdx >= c.numShards {
		var zk K
		var zv V
		return zk, zv, 0, false
	}
	s := c.shards[shardIdx]

	s.mu.Lock()
	victim := s.list.popTail()
	if victim == nil {
		s.mu.Unlock()
		var zk K
		var zv V
		return zk, zv, 0, false
	}

	delete(s.items, victim.key)
	s.weight -= victim.weight
	s.mu.Unlock()

	c.len.Add(-1)
	c.weight.Add(-victim.weight)
	c.shardLens[shardIdx].Add(-1)
	c.evictedLen.Add(1)
	c.evictedWeight.Add(uint64(victim.weight))

	return victim.key, victim.value, victim.weight, true
}

// evictToLimitIfNeeded mengeksekusi eviction jika kapasitas item atau batas bobot terlampaui.
func (c *LRUCache[K, V]) evictToLimitIfNeeded() {
	for (c.watermark > 0 && c.len.Load() > int64(c.watermark)) ||
		(c.weightLimit > 0 && c.weight.Load() > c.weightLimit) {
		if !c.evictOneP2C() {
			break
		}
	}
}

// evictOneP2C memilih 2 shard secara acak (Power of Two Choices) dan mengevakuasi item dari shard dengan beban lebih besar.
func (c *LRUCache[K, V]) evictOneP2C() bool {
	if c.len.Load() <= 0 {
		return false
	}

	// Power of Two Choices (P2C): pilih 2 shard acak
	now := uint64(time.Now().UnixNano())
	s1 := int(now % uint64(c.numShards))
	s2 := int((now >> 16) % uint64(c.numShards))
	if s1 == s2 {
		s2 = (s1 + 1) % c.numShards
	}

	// Pilih shard yang memiliki jumlah item shadow (`shardLens`) lebih banyak
	targetShard := s1
	if c.shardLens[s2].Load() > c.shardLens[s1].Load() {
		targetShard = s2
	}

	_, _, _, ok := c.EvictShard(targetShard)
	if !ok {
		// Fallback: coba shard s1 atau s2 yang satu lagi
		otherShard := s1
		if targetShard == s1 {
			otherShard = s2
		}
		_, _, _, ok = c.EvictShard(otherShard)
	}
	return ok
}

// Len mengembalikan jumlah total elemen aktif di dalam LRUCache.
func (c *LRUCache[K, V]) Len() int {
	return int(c.len.Load())
}

// Weight mengembalikan total bobot kumulatif dari seluruh item aktif di dalam LRUCache.
func (c *LRUCache[K, V]) Weight() int64 {
	return c.weight.Load()
}

// Cap mengembalikan total kapasitas maksimum penyimpanan LRUCache.
func (c *LRUCache[K, V]) Cap() int {
	return c.cap
}

// WeightLimit mengembalikan batas bobot maksimum dari LRUCache.
func (c *LRUCache[K, V]) WeightLimit() int64 {
	return c.weightLimit
}

// Stats mengembalikan statistik eksekusi (hits, misses, evictedLen, evictedWeight, size, weight).
func (c *LRUCache[K, V]) Stats() (hits, misses, evictedLen, evictedWeight uint64, size int, weight int64) {
	return c.hits.Load(), c.misses.Load(), c.evictedLen.Load(), c.evictedWeight.Load(), c.Len(), c.Weight()
}

// Clear mengosongkan seluruh entri dari semua shard dan mereset counter statistik.
func (c *LRUCache[K, V]) Clear() {
	for i, s := range c.shards {
		s.mu.Lock()
		s.items = make(map[K]*lruNode[K, V], c.cap/c.numShards)
		s.list.clear()
		s.weight = 0
		s.mu.Unlock()
		c.shardLens[i].Store(0)
	}
	c.len.Store(0)
	c.weight.Store(0)
	c.hits.Store(0)
	c.misses.Store(0)
	c.evictedLen.Store(0)
	c.evictedWeight.Store(0)
}

// Keys mengembalikan slice berisi seluruh key aktif yang tersimpan di dalam LRUCache dalam urutan tidak berurutan.
func (c *LRUCache[K, V]) Keys() []K {
	keys := make([]K, 0, c.Len())
	for _, s := range c.shards {
		s.mu.RLock()
		for k := range s.items {
			keys = append(keys, k)
		}
		s.mu.RUnlock()
	}
	return keys
}

// PeekLRU melihat item yang berada di ekor (LRU tail) dari shard tertentu tanpa mengevakuasi atau mengubah urutan LRU.
// Fitur ini setara dengan `peek_lru` pada Cloudflare Pingora untuk menginspeksi eviction frontier.
func (c *LRUCache[K, V]) PeekLRU(shardIdx int) (K, V, int64, bool) {
	if shardIdx < 0 || shardIdx >= c.numShards {
		var zk K
		var zv V
		return zk, zv, 0, false
	}
	s := c.shards[shardIdx]
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.list.tail == nil {
		var zk K
		var zv V
		return zk, zv, 0, false
	}
	return s.list.tail.key, s.list.tail.value, s.list.tail.weight, true
}

// SetWeight memperbarui bobot (weight) entri yang ada tanpa mengubah posisinya dalam antrean LRU.
func (c *LRUCache[K, V]) SetWeight(key K, weight int64) bool {
	if weight < 1 {
		weight = 1
	}
	h := hashKey(key)
	_, s := c.getShard(h)

	s.mu.Lock()
	node, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		return false
	}

	diff := weight - node.weight
	node.weight = weight
	s.weight += diff
	s.mu.Unlock()

	c.weight.Add(diff)
	c.evictToLimitIfNeeded()
	return true
}

// IncrementWeight menambah bobot entri (misal untuk range-request yang ukuran assetnya tumbuh bertahap).
// Jika key belum ada, entri baru akan di-admit dengan nilai default value dari defaultValFn.
func (c *LRUCache[K, V]) IncrementWeight(key K, delta int64, defaultValFn func() V) int64 {
	if delta < 1 {
		delta = 1
	}
	h := hashKey(key)
	shardIdx, s := c.getShard(h)

	s.mu.Lock()
	if existing, ok := s.items[key]; ok {
		existing.weight += delta
		newW := existing.weight
		s.weight += delta
		s.list.moveToFront(existing)
		s.mu.Unlock()

		c.weight.Add(delta)
		c.evictToLimitIfNeeded()
		return newW
	}

	var val V
	if defaultValFn != nil {
		val = defaultValFn()
	}
	node := &lruNode[K, V]{
		key:    key,
		value:  val,
		hash:   h,
		weight: delta,
	}
	s.items[key] = node
	s.list.pushFront(node)
	s.weight += delta
	s.mu.Unlock()

	c.len.Add(1)
	c.weight.Add(delta)
	c.shardLens[shardIdx].Add(1)
	c.evictToLimitIfNeeded()

	return delta
}

// ShardWeight mengembalikan total bobot dari shard spesifik.
func (c *LRUCache[K, V]) ShardWeight(shardIdx int) int64 {
	if shardIdx < 0 || shardIdx >= c.numShards {
		return 0
	}
	s := c.shards[shardIdx]
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.weight
}

// ShardLen mengembalikan jumlah item di dalam shard spesifik via shadow atomic counter (lock-free).
func (c *LRUCache[K, V]) ShardLen(shardIdx int) int {
	if shardIdx < 0 || shardIdx >= c.numShards {
		return 0
	}
	return int(c.shardLens[shardIdx].Load())
}
