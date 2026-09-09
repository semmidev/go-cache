package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrFetchFailed mengindikasikan kegagalan pada fungsi loader/fetch.
	ErrFetchFailed = errors.New("async_lru: fetch function failed")
)

// asyncCall merepresentasikan pemanggilan asynchronous yang sedang berjalan untuk key tertentu.
type asyncCall[V any] struct {
	wg  sync.WaitGroup
	val V
	err error
}

// AsyncLRUCache merepresentasikan pembungkus asynchronous (Async LRU Cache) pada LRUCache,
// terinspirasi oleh `async_lru.rs` dari Cloudflare Pingora.
//
// AsyncLRUCache menyediakan fitur Request Coalescing (Singleflight), sehingga lonjakan
// permintaan serentak untuk key yang sama saat terjadi Cache Miss hanya akan mengeksekusi
// 1 pemanggilan loader/fetch ke database utama. Hasilnya kemudian didistribusikan ke seluruh caller.
type AsyncLRUCache[K comparable, V any] struct {
	cache *LRUCache[K, V]
	mu    sync.Mutex
	calls map[K]*asyncCall[V]
}

// NewAsyncLRU membuat dan menginisialisasi instance AsyncLRUCache baru dari LRUCache yang ada atau opsi baru.
func NewAsyncLRU[K comparable, V any](lru *LRUCache[K, V]) *AsyncLRUCache[K, V] {
	if lru == nil {
		lru = NewLRU[K, V]()
	}
	return &AsyncLRUCache[K, V]{
		cache: lru,
		calls: make(map[K]*asyncCall[V]),
	}
}

// Cache mengembalikan pointer ke underlying LRUCache.
func (a *AsyncLRUCache[K, V]) Cache() *LRUCache[K, V] {
	return a.cache
}

// GetOrFetch mengambil item dari LRUCache. Jika item tidak ada (miss),
// GetOrFetch akan memanggil fetchFn secara koalesens (singleflight) dan menyimpan hasilnya ke LRUCache.
func (a *AsyncLRUCache[K, V]) GetOrFetch(
	ctx context.Context,
	key K,
	ttl time.Duration,
	fetchFn func(ctx context.Context, key K) (V, error),
) (V, error) {
	return a.GetOrFetchWithWeight(ctx, key, 1, ttl, fetchFn)
}

// GetOrFetchWithWeight mengambil item dengan penentuan bobot custom saat penyimpan ke LRUCache.
func (a *AsyncLRUCache[K, V]) GetOrFetchWithWeight(
	ctx context.Context,
	key K,
	weight int64,
	ttl time.Duration,
	fetchFn func(ctx context.Context, key K) (V, error),
) (V, error) {
	// Fast path: Cek cache terlebih dahulu
	if val, ok := a.cache.Get(key); ok {
		return val, nil
	}

	// Request Coalescing (Singleflight)
	a.mu.Lock()
	if c, ok := a.calls[key]; ok {
		a.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}

	call := new(asyncCall[V])
	call.wg.Add(1)
	a.calls[key] = call
	a.mu.Unlock()

	// Eksekusi fetchFn untuk caller pertama
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				call.err = fmt.Errorf("panic during async fetch: %v", r)
			}
			call.wg.Done()
			close(done)
		}()
		call.val, call.err = fetchFn(ctx, key)
		if call.err == nil {
			a.cache.PutWithWeight(key, call.val, weight, ttl)
		}
	}()

	select {
	case <-ctx.Done():
		return call.val, ctx.Err()
	case <-done:
		a.mu.Lock()
		delete(a.calls, key)
		a.mu.Unlock()
		return call.val, call.err
	}
}
