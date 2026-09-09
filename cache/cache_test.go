package cache

import (
	"sync"
	"testing"
	"time"
)

// TestMemoryCacheBasic menguji fungsi dasar penyimpan dan penghapusan key-value (Put, Get, Remove).
func TestMemoryCacheBasic(t *testing.T) {
	c := New[string, string](WithCapacity(10))
	c.Put("foo", "bar")
	v, ok := c.Get("foo")
	if !ok || v != "bar" {
		t.Fatalf("expected bar got %v ok %v", v, ok)
	}
	c.Remove("foo")
	if _, ok := c.Get("foo"); ok {
		t.Fatalf("should be removed")
	}
}

// TestMemoryCacheTTL menguji kadaluarsa entri otomatis berdasarkan durasi TTL (Time-To-Live).
func TestMemoryCacheTTL(t *testing.T) {
	c := New[string, int](WithCapacity(10))
	c.Put("a", 1, 50*time.Millisecond)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("should exist")
	}
	time.Sleep(60 * time.Millisecond)
	if _, ok := c.Get("a"); ok {
		t.Fatal("should be expired")
	}
}

// TestMemoryCacheEvictionS3FIFO menguji perilaku evikasi S3-FIFO (Small Queue & Main Queue promotion) pada shard.
func TestMemoryCacheEvictionS3FIFO(t *testing.T) {
	s := newShard[string, int](5)

	// Masukkan 5 item hingga shard penuh
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		e := &entry[string, int]{key: k, value: 1, hash: hashKey(k), freq: 1}
		s.table[k] = e
		s.smallQueue = append(s.smallQueue, e)
	}

	// Akses key "a" untuk menaikkan frekuensi dan mempromosikannya ke Main Queue
	eA := s.table["a"]
	eA.freq = 2
	s.promoteToMain(eA)

	// Pemicu evikasi ketika shard melampaui kapasitas 5
	if len(s.table) >= s.cap {
		s.evict()
	}

	// Key "a" yang dipromosikan ke Main Queue harus dipertahankan
	if _, ok := s.table["a"]; !ok {
		t.Fatal("a should be kept in main queue")
	}
	if len(s.table) > 5 {
		t.Fatalf("shard table len should <= 5 got %d", len(s.table))
	}
}

// TestMemoryCacheConcurrent menguji keandalan thread-safety saat diakses serentak oleh banyak goroutine.
func TestMemoryCacheConcurrent(t *testing.T) {
	c := New[int, int](WithCapacity(1000))
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			for j := range 1000 {
				key := j % 100
				c.Put(key, j)
				c.Get(key)
			}
		})
		_ = i
	}
	wg.Wait()
}

// TestMemoryCacheStats menguji akurasi pelacakan counter statistik Hit dan Miss.
func TestMemoryCacheStats(t *testing.T) {
	c := New[string, int](WithCapacity(10))
	c.Put("a", 1)
	c.Get("a") // Hit
	c.Get("b") // Miss
	h, m, _ := c.Stats()
	if h != 1 || m != 1 {
		t.Fatalf("expected h=1 m=1 got h=%d m=%d", h, m)
	}
}

// TestMemoryCacheForcePut menguji fungsi ForcePut yang melewati pemeriksaan TinyLFU admission.
func TestMemoryCacheForcePut(t *testing.T) {
	c := New[int, int](WithCapacity(2))
	c.Put(1, 1)
	c.Put(2, 2)
	c.Get(1)
	c.Get(1)
	c.Put(3, 3)
	c.ForcePut(3, 3)
	if _, ok := c.Get(3); !ok {
		t.Fatal("ForcePut should insert")
	}
}
