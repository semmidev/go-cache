package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLRUBasicOperations(t *testing.T) {
	c := NewLRU[string, int](WithLRUCapacity(100), WithLRUShards(4))

	c.Put("a", 100)
	c.Put("b", 200)

	if val, ok := c.Get("a"); !ok || val != 100 {
		t.Fatalf("expected Get(a) = 100, got %v, ok=%v", val, ok)
	}

	if val, ok := c.Get("b"); !ok || val != 200 {
		t.Fatalf("expected Get(b) = 200, got %v, ok=%v", val, ok)
	}

	if _, ok := c.Get("c"); ok {
		t.Fatalf("expected Get(c) to fail for non-existent key")
	}

	if !c.Contains("a") {
		t.Fatalf("expected Contains(a) = true")
	}

	if c.Len() != 2 {
		t.Fatalf("expected Len() = 2, got %d", c.Len())
	}

	if !c.Remove("a") {
		t.Fatalf("expected Remove(a) = true")
	}

	if c.Contains("a") {
		t.Fatalf("expected Contains(a) = false after removal")
	}

	if c.Len() != 1 {
		t.Fatalf("expected Len() = 1, got %d", c.Len())
	}
}

func TestLRUWeightAndCapacityEviction(t *testing.T) {
	c := NewLRU[int, int](
		WithLRUCapacity(10),
		WithWatermark(10),
		WithWeightLimit(100),
		WithLRUShards(2),
	)

	for i := range 15 {
		c.PutWithWeight(i, i*10, 10)
	}

	if c.Len() > 10 {
		t.Fatalf("expected Len() <= 10 after eviction, got %d", c.Len())
	}

	if c.Weight() > 100 {
		t.Fatalf("expected Weight() <= 100 after eviction, got %d", c.Weight())
	}

	hits, misses, evictedLen, evictedWeight, _, _ := c.Stats()
	if evictedLen == 0 {
		t.Fatalf("expected evictedLen > 0")
	}
	if evictedWeight == 0 {
		t.Fatalf("expected evictedWeight > 0")
	}
	t.Logf("Stats: hits=%d, misses=%d, evictedLen=%d, evictedWeight=%d", hits, misses, evictedLen, evictedWeight)
}

func TestLRUPromoteTopN(t *testing.T) {
	c := NewLRU[string, int](WithLRUCapacity(10), WithLRUShards(1))

	c.Put("k1", 1)
	c.Put("k2", 2)
	c.Put("k3", 3)

	// k3 adalah MRU (posisi 1), k2 posisi 2, k1 posisi 3
	if !c.PromoteTopN("k3", 2) {
		t.Fatalf("expected PromoteTopN(k3) = true")
	}

	if !c.PromoteTopN("k1", 2) {
		t.Fatalf("expected PromoteTopN(k1) = true")
	}

	// k1 sekarang dipromosikan ke MRU
	val, ok := c.Get("k1")
	if !ok || val != 1 {
		t.Fatalf("expected Get(k1) = 1, got %v", val)
	}
}

func TestLRUTTLExpiration(t *testing.T) {
	c := NewLRU[string, string](WithLRUCapacity(10), WithLRUShards(2))

	c.Put("temp", "value", 20*time.Millisecond)
	c.Put("perm", "value")

	if _, ok := c.Get("temp"); !ok {
		t.Fatalf("expected temp to be available immediately")
	}

	time.Sleep(50 * time.Millisecond)

	if _, ok := c.Get("temp"); ok {
		t.Fatalf("expected temp to be expired after TTL")
	}

	if _, ok := c.Get("perm"); !ok {
		t.Fatalf("expected perm to be still active")
	}
}

func TestAsyncLRUCoalescing(t *testing.T) {
	lru := NewLRU[string, int](WithLRUCapacity(100))
	asyncLRU := NewAsyncLRU(lru)

	var calls atomic.Int32
	fetchFn := func(ctx context.Context, key string) (int, error) {
		calls.Add(1)
		time.Sleep(30 * time.Millisecond)
		return 42, nil
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	numGoroutines := 10

	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, err := asyncLRU.GetOrFetch(ctx, "hotkey", 5*time.Minute, fetchFn)
			if err != nil || val != 42 {
				t.Errorf("expected val=42, err=nil, got val=%v, err=%v", val, err)
			}
		}()
	}

	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("expected exactly 1 fetch call due to request coalescing, got %d", calls.Load())
	}
}

func TestLRUPersistenceDumpRestore(t *testing.T) {
	c1 := NewLRU[string, int](WithLRUCapacity(100), WithLRUShards(4))
	c1.PutWithWeight("a", 10, 5)
	c1.PutWithWeight("b", 20, 10)
	c1.PutWithWeight("c", 30, 15)

	var buf bytes.Buffer
	if err := c1.Dump(&buf); err != nil {
		t.Fatalf("Dump failed: %v", err)
	}

	c2 := NewLRU[string, int](WithLRUCapacity(100), WithLRUShards(4))
	if err := c2.Restore(&buf); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	if c2.Len() != 3 {
		t.Fatalf("expected c2.Len() = 3, got %d", c2.Len())
	}

	for _, k := range []string{"a", "b", "c"} {
		if !c2.Contains(k) {
			t.Fatalf("expected restored cache to contain key %s", k)
		}
	}
}

func TestLRUConcurrentAccess(t *testing.T) {
	c := NewLRU[int, int](WithLRUCapacity(1000), WithLRUShards(16))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for g := range 10 {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
					key := (gID * 100) + (i % 50)
					c.Put(key, i)
					c.Get(key)
					if i%5 == 0 {
						c.PromoteTopN(key, 5)
					}
					if i%10 == 0 {
						c.Remove(key)
					}
					i++
				}
			}
		}(g)
	}
	wg.Wait()

	t.Logf("Concurrent test complete. Cache size: %d, Hits: %d, Misses: %d", c.Len(), c.hits.Load(), c.misses.Load())
}

func TestAsyncLRUFetchErrorHandling(t *testing.T) {
	asyncLRU := NewAsyncLRU[string, int](nil)
	expectedErr := errors.New("db error")

	fetchFn := func(ctx context.Context, key string) (int, error) {
		return 0, expectedErr
	}

	_, err := asyncLRU.GetOrFetch(context.Background(), "errKey", time.Minute, fetchFn)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func TestLRUClearAndKeys(t *testing.T) {
	c := NewLRU[int, string](WithLRUCapacity(50), WithLRUShards(4))
	for i := range 10 {
		c.Put(i, fmt.Sprintf("val-%d", i))
	}

	keys := c.Keys()
	if len(keys) != 10 {
		t.Fatalf("expected 10 keys, got %d", len(keys))
	}

	c.Clear()
	if c.Len() != 0 {
		t.Fatalf("expected Len() = 0 after Clear(), got %d", c.Len())
	}
}

func TestLRUPingoraParityFeatures(t *testing.T) {
	c := NewLRU[string, string](WithLRUCapacity(10), WithLRUShards(1))

	c.PutWithWeight("asset1", "data1", 100)
	c.PutWithWeight("asset2", "data2", 200)

	// Test SetWeight
	if !c.SetWeight("asset1", 150) {
		t.Fatalf("expected SetWeight(asset1) = true")
	}
	if c.Weight() != 350 {
		t.Fatalf("expected total weight = 350, got %d", c.Weight())
	}

	// Test IncrementWeight
	newW := c.IncrementWeight("asset1", 50, nil)
	if newW != 200 {
		t.Fatalf("expected asset1 weight = 200 after increment, got %d", newW)
	}

	// Test IncrementWeight missing key
	newW2 := c.IncrementWeight("asset3", 500, func() string { return "data3" })
	if newW2 != 500 {
		t.Fatalf("expected asset3 weight = 500, got %d", newW2)
	}

	// Test PeekLRU (tail node)
	k, v, w, ok := c.PeekLRU(0)
	if !ok {
		t.Fatalf("expected PeekLRU = true")
	}
	t.Logf("PeekLRU tail item: key=%s, val=%s, weight=%d", k, v, w)

	// Test ShardWeight & ShardLen
	if c.ShardWeight(0) != c.Weight() {
		t.Fatalf("expected ShardWeight = %d, got %d", c.Weight(), c.ShardWeight(0))
	}
	if c.ShardLen(0) != c.Len() {
		t.Fatalf("expected ShardLen = %d, got %d", c.Len(), c.ShardLen(0))
	}
}
