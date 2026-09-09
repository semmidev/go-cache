package cache

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"
)

// BenchmarkMemoryCache_GetHit menguji kecepatan Get paralel pada MemoryCache (S3-FIFO) saat 100% HIT.
func BenchmarkMemoryCache_GetHit(b *testing.B) {
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

// BenchmarkLRU_GetHit menguji kecepatan Get paralel pada LRUCache (Pingora Sharded LRU) saat 100% HIT.
func BenchmarkLRU_GetHit(b *testing.B) {
	c := NewLRU[int, int](WithLRUCapacity(10000), WithLRUShards(64))
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

// BenchmarkMemoryCache_Put menguji penulisan beruntun (Put) pada MemoryCache (S3-FIFO).
func BenchmarkMemoryCache_Put(b *testing.B) {
	c := New[int, int](WithCapacity(100000))
	i := 0
	for b.Loop() {
		c.Put(i, i)
		i++
	}
}

// BenchmarkLRU_Put menguji penulisan beruntun (Put) pada LRUCache (Pingora LRU).
func BenchmarkLRU_Put(b *testing.B) {
	c := NewLRU[int, int](WithLRUCapacity(100000), WithLRUShards(64))
	i := 0
	for b.Loop() {
		c.Put(i, i)
		i++
	}
}

// BenchmarkMemoryCache_ParallelPut menguji penulisan paralel tinggi pada MemoryCache.
func BenchmarkMemoryCache_ParallelPut(b *testing.B) {
	c := New[int, int](WithCapacity(100000))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.IntN(100000)
		for pb.Next() {
			c.Put(i, i)
			i++
		}
	})
}

// BenchmarkLRU_ParallelPut menguji penulisan paralel tinggi pada LRUCache.
func BenchmarkLRU_ParallelPut(b *testing.B) {
	c := NewLRU[int, int](WithLRUCapacity(100000), WithLRUShards(64))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.IntN(100000)
		for pb.Next() {
			c.Put(i, i)
			i++
		}
	})
}

// BenchmarkMemoryCache_Mixed80Read20Write menguji skenario umum web app (80% Read, 20% Write) pada MemoryCache.
func BenchmarkMemoryCache_Mixed80Read20Write(b *testing.B) {
	c := New[int, int](WithCapacity(10000))
	for i := range 10000 {
		c.Put(i, i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%5 == 0 {
				c.Put(i%10000, i)
			} else {
				c.Get(i % 10000)
			}
			i++
		}
	})
}

// BenchmarkLRU_Mixed80Read20Write menguji skenario umum web app (80% Read, 20% Write) pada LRUCache.
func BenchmarkLRU_Mixed80Read20Write(b *testing.B) {
	c := NewLRU[int, int](WithLRUCapacity(10000), WithLRUShards(64))
	for i := range 10000 {
		c.Put(i, i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%5 == 0 {
				c.Put(i%10000, i)
			} else {
				c.Get(i % 10000)
			}
			i++
		}
	})
}

// BenchmarkMemoryCache_EvictionHeavy menguji performa saat cache penuh (100% Eviction throughput).
func BenchmarkMemoryCache_EvictionHeavy(b *testing.B) {
	c := New[int, int](WithCapacity(1000))
	for i := range 1000 {
		c.Put(i, i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 1000
		for pb.Next() {
			c.Put(i, i)
			i++
		}
	})
}

// BenchmarkLRU_EvictionHeavy menguji performa saat cache penuh (100% Eviction throughput via P2C).
func BenchmarkLRU_EvictionHeavy(b *testing.B) {
	c := NewLRU[int, int](WithLRUCapacity(1000), WithWatermark(1000), WithLRUShards(64))
	for i := range 1000 {
		c.Put(i, i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 1000
		for pb.Next() {
			c.Put(i, i)
			i++
		}
	})
}

// BenchmarkLRU_PromoteTopN menguji kecepatan optimalisasi PromoteTopN (Fast-Path RLock) pada LRUCache.
func BenchmarkLRU_PromoteTopN(b *testing.B) {
	c := NewLRU[int, int](WithLRUCapacity(1000), WithLRUShards(16))
	for i := range 1000 {
		c.Put(i, i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.PromoteTopN(42, 10)
		}
	})
}

// BenchmarkAsyncLRU_CoalescedGet menguji performa AsyncLRUCache Request Coalescing saat lonjakan trafik.
func BenchmarkAsyncLRU_CoalescedGet(b *testing.B) {
	asyncLRU := NewAsyncLRU(NewLRU[int, int](WithLRUCapacity(10000)))
	ctx := context.Background()
	fetchFn := func(ctx context.Context, k int) (int, error) {
		return k * 2, nil
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			asyncLRU.GetOrFetch(ctx, 42, 5*time.Minute, fetchFn)
		}
	})
}
