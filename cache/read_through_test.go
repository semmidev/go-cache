package cache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRTCacheHitMiss menguji alur transisi CacheMiss (memanggil lookup) lalu CacheHit pada pemanggilan kedua.
func TestRTCacheHitMiss(t *testing.T) {
	rt := NewRTCache[string, string, string](WithRTCapacity(10))
	lookup := func(ctx context.Context, k string, extra *string) (string, *time.Duration, error) {
		ttl := 1 * time.Minute
		return "val-" + k, &ttl, nil
	}
	ctx := t.Context()
	v, status, _ := rt.Get(ctx, "a", nil, nil, lookup)
	if status != CacheMiss || v != "val-a" {
		t.Fatalf("expected MISS val-a got %s %s", status, v)
	}
	v2, status2, _ := rt.Get(ctx, "a", nil, nil, lookup)
	if status2 != CacheHit {
		t.Fatalf("expected HIT got %s", status2)
	}
	if v2 != "val-a" {
		t.Fatalf("unexpected %s", v2)
	}
}

// TestRTCacheStampede menguji pencegahan Cache Stampede (50 goroutine serentak hanya memicu 1 kali DB lookup).
func TestRTCacheStampede(t *testing.T) {
	rt := NewRTCache[string, string, string](WithRTCapacity(100), WithLockTimeout(5*time.Second))
	var count atomic.Int32
	lookup := func(ctx context.Context, k string, extra *string) (string, *time.Duration, error) {
		count.Add(1)
		time.Sleep(100 * time.Millisecond)
		ttl := 1 * time.Minute
		return "data", &ttl, nil
	}
	ctx := t.Context()
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			rt.Get(ctx, "hot-key", nil, nil, lookup)
		})
	}
	wg.Wait()
	if count.Load() != 1 {
		t.Fatalf("stampede not prevented count=%d", count.Load())
	}
}

// TestRTCacheCancellationFix menguji perbaikan Bug #931 (penanganan aman saat context dibatalkan agar tidak terjadi lock leak).
func TestRTCacheCancellationFix(t *testing.T) {
	rt := NewRTCache[int, int, string](WithRTCapacity(10))
	started := make(chan struct{})
	lookup := func(ctx context.Context, k int, extra *string) (int, *time.Duration, error) {
		close(started)
		<-ctx.Done()
		return 0, nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		rt.Get(ctx, 1, nil, nil, lookup)
	}()
	<-started
	time.Sleep(10 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)

	ctx2 := context.Background()
	lookup2 := func(ctx context.Context, k int, extra *string) (int, *time.Duration, error) {
		ttl := 1 * time.Minute
		return 42, &ttl, nil
	}
	done := make(chan struct{})
	go func() {
		v, _, _ := rt.Get(ctx2, 1, nil, nil, lookup2)
		if v != 42 {
			t.Errorf("expected 42 got %d", v)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stale lock leaked - bug #931")
	}
}

// TestRTCacheMultiGet menguji fungsi batching MultiGet.
func TestRTCacheMultiGet(t *testing.T) {
	rt := NewRTCache[string, string, string](WithRTCapacity(10))
	ctx := t.Context()
	lookupSingle := func(ctx context.Context, k string, extra *string) (string, *time.Duration, error) {
		ttl := 1 * time.Minute
		return "val-" + k, &ttl, nil
	}
	rt.Get(ctx, "a", nil, nil, lookupSingle)
	multiLookup := func(ctx context.Context, keys []string, extra *string) (map[string]string, map[string]time.Duration, error) {
		res := make(map[string]string)
		ttls := make(map[string]time.Duration)
		for _, k := range keys {
			res[k] = "val-" + k
			ttls[k] = time.Minute
		}
		return res, ttls, nil
	}
	keys := []string{"a", "b", "c"}
	results, statuses, _ := rt.MultiGet(ctx, keys, nil, multiLookup)
	if statuses["a"] != CacheHit {
		t.Fatalf("a should HIT")
	}
	if len(results) != 3 {
		t.Fatalf("expected 3")
	}
	fmt.Printf("MultiGet ok %v\n", results)
}
