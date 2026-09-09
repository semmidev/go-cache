package cache

import (
	"context"
	"testing"
	"time"
)

// BenchmarkMemoryCacheGetHit menguji kecepatan baca (Get) paralel saat seluruh data berada di cache (100% HIT).
func BenchmarkMemoryCacheGetHit(b *testing.B) {
	c := New[int, int](WithCapacity(10000))
	for i := range 10000 {
		c.Put(i, i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get(42)
		}
	})
}

// BenchmarkMemoryCachePut menguji performa alokasi dan penulisan (Put) berturut-turut pada MemoryCache.
func BenchmarkMemoryCachePut(b *testing.B) {
	c := New[int, int](WithCapacity(100000))
	i := 0
	for b.Loop() {
		c.Put(i, i)
		i++
	}
}

// BenchmarkRTCacheHit menguji performa RTCache.Get paralel ketika data sudah tersedia di cache (HIT fast-path).
func BenchmarkRTCacheHit(b *testing.B) {
	rt := NewRTCache[int, int, string](WithRTCapacity(10000))
	ctx := context.Background()
	lookup := func(ctx context.Context, k int, extra *string) (int, *time.Duration, error) {
		ttl := 5 * time.Minute
		return k * 2, &ttl, nil
	}
	rt.Get(ctx, 42, nil, nil, lookup)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rt.Get(ctx, 42, nil, nil, lookup)
		}
	})
}

// BenchmarkRTCacheStampede menguji efisiensi Request Coalescing RTCache saat terjadi lonjakan akses serentak (Cache Stampede).
func BenchmarkRTCacheStampede(b *testing.B) {
	rt := NewRTCache[string, string, string](WithRTCapacity(1000), WithLockTimeout(5*time.Second))
	ctx := context.Background()
	lookup := func(ctx context.Context, k string, extra *string) (string, *time.Duration, error) {
		ttl := 5 * time.Minute
		return "value", &ttl, nil
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rt.Get(ctx, "hot-key", nil, nil, lookup)
		}
	})
}

