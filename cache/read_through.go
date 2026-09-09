package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// CacheStatus merepresentasikan status hasil pencarian pada RTCache (HIT, MISS, atau STALE).
type CacheStatus int

const (
	// CacheHit menandakan data ditemukan langsung di dalam memory cache.
	CacheHit CacheStatus = iota

	// CacheMiss menandakan data tidak ada di cache dan berhasil didapatkan melalui LookupFunc ke database/origin.
	CacheMiss

	// CacheStale menandakan data dikembalikan karena timeout pada penguncian (lockTimeout) atau fallback.
	CacheStale
)

// String mengembalikan representasi string teks dari CacheStatus ("HIT", "MISS", "STALE", atau "UNKNOWN").
func (s CacheStatus) String() string {
	switch s {
	case CacheHit:
		return "HIT"
	case CacheMiss:
		return "MISS"
	case CacheStale:
		return "STALE"
	default:
		return "UNKNOWN"
	}
}

// LookupFunc adalah tipe fungsi callback penarik data utama dari database/origin untuk key tunggal.
// Mengembalikan (value, newTTL, error). Jika newTTL non-nil, nilainya akan menggantikan TTL default.
type LookupFunc[K comparable, V any, S any] func(ctx context.Context, key K, extra *S) (V, *time.Duration, error)

// MultiLookupFunc adalah tipe fungsi callback penarik data secara batch untuk banyak key sekaligus.
// Mengembalikan (map[K]V, map[K]time.Duration, error).
type MultiLookupFunc[K comparable, V any, S any] func(ctx context.Context, keys []K, extra *S) (map[K]V, map[K]time.Duration, error)

// cacheLock merepresentasikan struktur kunci koordinasi per-key untuk Request Coalescing.
// Menggunakan channel `done` sebagai sinyal broadcast penutupan lock ke semua goroutine penunggu.
type cacheLock struct {
	start time.Time
	done  chan struct{}
}

// newCacheLock membuat instance cacheLock baru dengan pencatatan waktu mulai.
func newCacheLock() *cacheLock {
	return &cacheLock{start: time.Now(), done: make(chan struct{})}
}

// TooOld memeriksa apakah lock sudah melebihi durasi `maxAge` yang diizinkan (stale lock).
func (l *cacheLock) TooOld(maxAge *time.Duration) bool {
	if maxAge == nil {
		return false
	}
	return time.Since(l.start) > *maxAge
}

// RTCache (Read-Through Cache) merepresentasikan layer abstraksi cache transparan dengan
// perlindungan anti-Cache Stampede (Thundering Herd) performa tinggi.
//
// Karakteristik Utama:
// 1. **Request Coalescing**: Menggabungkan puluhan/ratusan request bersamaan untuk key yang sama menjadi 1 panggilan lookup saja ke database.
// 2. **Singleflight & Sync Map Lockers**: Mencegah redundansi eksekusi IO serentak.
// 3. **Penanganan Context Cancellation Leak (Issue #931)**: Mengamankan dari kebocoran lock (lock leak) saat context caller dibatalkan (`ctx.Done()`).
// 4. **Lock Age & Timeout Options**: Membatasi durasi tunggu goroutine penunggu agar tidak memicu bottleneck.
type RTCache[K comparable, V any, S any] struct {
	inner       *MemoryCache[K, V]
	lockers     sync.Map
	lockAge     *time.Duration
	lockTimeout *time.Duration
	sf          singleflight.Group
}

// rtConfig menyimpan opsi konfigurasi pembuatan RTCache.
type rtConfig struct {
	cap         int
	lockAge     *time.Duration
	lockTimeout *time.Duration
}

// RTOption didefinisikan sebagai fungsi konfigurasi functional options pattern untuk RTCache.
type RTOption func(*rtConfig)

// WithRTCapacity mengatur kapasitas maksimum penyimpanan MemoryCache internal di RTCache.
func WithRTCapacity(cap int) RTOption {
	return func(c *rtConfig) { c.cap = cap }
}

// WithLockAge mengatur durasi maksimum sebuah lock dianggap valid sebelum dianggap hang/stale.
func WithLockAge(d time.Duration) RTOption {
	return func(c *rtConfig) { c.lockAge = &d }
}

// WithLockTimeout mengatur batas waktu maksimum goroutine penunggu menunggu hasil dari goroutine utama.
func WithLockTimeout(d time.Duration) RTOption {
	return func(c *rtConfig) { c.lockTimeout = &d }
}

// NewRTCache membuat dan menginisialisasi instance RTCache baru dengan opsi konfigurasi yang diberikan.
func NewRTCache[K comparable, V any, S any](opts ...RTOption) *RTCache[K, V, S] {
	cfg := rtConfig{cap: 1000}
	for _, o := range opts {
		o(&cfg)
	}
	return &RTCache[K, V, S]{
		inner:       New[K, V](WithCapacity(cfg.cap)),
		lockAge:     cfg.lockAge,
		lockTimeout: cfg.lockTimeout,
	}
}

// Get mengambil nilai dari cache jika ada (HIT).
// Jika terjadi Cache Miss:
//  1. Goroutine pertama akan mengambil lock internal, mengeksekusi LookupFunc ke database/origin,
//     lalu menyimpan hasilnya ke cache (ForcePut) dan membuka lock.
//  2. Goroutine lain yang meminta key sama pada saat bersamaan tidak akan memanggil database,
//     melainkan menunggu sinyal selesai dari goroutine pertama (Request Coalescing).
//  3. Menangani kasus pemicu pembatalan context secara aman (Issue #931 fix) tanpa meninggalkan stale lock.
func (c *RTCache[K, V, S]) Get(ctx context.Context, key K, ttl *time.Duration, extra *S, lookup LookupFunc[K, V, S]) (V, CacheStatus, error) {
	// Fast-path: Cek cache internal terlebih dahulu
	if v, ok := c.inner.Get(key); ok {
		return v, CacheHit, nil
	}

	// Cek apakah sudah ada goroutine lain yang sedang melakukan lookup untuk key ini
	if val, ok := c.lockers.Load(key); ok {
		lock := val.(*cacheLock)
		if !lock.TooOld(c.lockAge) {
			return c.waitForLock(ctx, key, lock, extra, lookup)
		}
	}

	newLock := newCacheLock()
	actual, loaded := c.lockers.LoadOrStore(key, newLock)
	if loaded {
		lock := actual.(*cacheLock)
		return c.waitForLock(ctx, key, lock, extra, lookup)
	}

	var zero V

	// Pembersihan aman (Issue #931 Fix): Memastikan channel done ditutup dan lock dibersihkan
	// bahkan jika goroutine mengalami panic atau dibatalkan.
	defer func() {
		select {
		case <-newLock.done:
		default:
			close(newLock.done)
		}
		c.lockers.Delete(key)
	}()

	sfKey := fmt.Sprintf("%v-%d", key, hashKey(key))
	v, err, _ := c.sf.Do(sfKey, func() (interface{}, error) {
		val, newTTL, err := lookup(ctx, key, extra)
		if err != nil {
			return nil, err
		}
		finalTTL := ttl
		if newTTL != nil {
			finalTTL = newTTL
		}
		if finalTTL != nil {
			c.inner.ForcePut(key, val, *finalTTL)
		} else {
			c.inner.ForcePut(key, val)
		}
		return val, nil
	})

	if err != nil {
		return zero, CacheMiss, err
	}
	return v.(V), CacheMiss, nil
}

// waitForLock menangani penantian goroutine caller terhadap lock yang dipegang oleh goroutine pemicu lookup utama.
func (c *RTCache[K, V, S]) waitForLock(ctx context.Context, key K, lock *cacheLock, extra *S, lookup LookupFunc[K, V, S]) (V, CacheStatus, error) {
	var zero V
	if c.lockTimeout != nil {
		select {
		case <-lock.done:
			if v, ok := c.inner.Get(key); ok {
				return v, CacheHit, nil
			}
			val, _, err := lookup(ctx, key, extra)
			if err != nil {
				return zero, CacheMiss, err
			}
			return val, CacheStale, nil
		case <-time.After(*c.lockTimeout):
			val, _, err := lookup(ctx, key, extra)
			if err != nil {
				return zero, CacheMiss, err
			}
			return val, CacheStale, nil
		case <-ctx.Done():
			return zero, CacheMiss, ctx.Err()
		}
	} else {
		select {
		case <-lock.done:
			if v, ok := c.inner.Get(key); ok {
				return v, CacheHit, nil
			}
			val, newTTL, err := lookup(ctx, key, extra)
			if err != nil {
				return zero, CacheMiss, err
			}
			if newTTL != nil {
				c.inner.ForcePut(key, val, *newTTL)
			} else {
				c.inner.ForcePut(key, val)
			}
			return val, CacheMiss, nil
		case <-ctx.Done():
			return zero, CacheMiss, ctx.Err()
		}
	}
}

// GetOrLoad adalah helper sederhana untuk mengambil data dari cache atau memuat via lookup tanpa parameter ttl/extra tambahan.
func (c *RTCache[K, V, S]) GetOrLoad(ctx context.Context, key K, lookup LookupFunc[K, V, S]) (V, CacheStatus, error) {
	return c.Get(ctx, key, nil, nil, lookup)
}

// MultiGet mengambil sekumpulan key sekaligus secara batch.
// Hanya key yang tidak ada di cache (missing keys) yang akan dikirimkan ke `MultiLookupFunc`.
// Hasil dari database akan disimpan ke cache secara otomatis (ForcePut).
func (c *RTCache[K, V, S]) MultiGet(ctx context.Context, keys []K, extra *S, lookup MultiLookupFunc[K, V, S]) (map[K]V, map[K]CacheStatus, error) {
	results := make(map[K]V, len(keys))
	statuses := make(map[K]CacheStatus, len(keys))
	missing := make([]K, 0)

	// Filter mana key yang HIT dan mana yang MISS
	for _, k := range keys {
		if v, ok := c.inner.Get(k); ok {
			results[k] = v
			statuses[k] = CacheHit
		} else {
			missing = append(missing, k)
		}
	}

	// Jika semua key HIT, langsung kembalikan hasil
	if len(missing) == 0 {
		return results, statuses, nil
	}

	// Lakukan batch lookup hanya untuk key yang belum ada di cache
	fetched, ttls, err := lookup(ctx, missing, extra)
	if err != nil {
		return results, statuses, err
	}

	for _, k := range missing {
		if v, ok := fetched[k]; ok {
			results[k] = v
			statuses[k] = CacheMiss
			if ttl, ok := ttls[k]; ok {
				c.inner.ForcePut(k, v, ttl)
			} else {
				c.inner.ForcePut(k, v)
			}
		}
	}
	return results, statuses, nil
}

// Stats mengembalikan statistik (hits, misses, current_size) dari memory cache internal.
func (c *RTCache[K, V, S]) Stats() (hits, misses uint64, size int) { return c.inner.Stats() }

// Len mengembalikan jumlah elemen di dalam cache internal.
func (c *RTCache[K, V, S]) Len() int { return c.inner.Len() }

// Clear mengosongkan seluruh isi cache internal.
func (c *RTCache[K, V, S]) Clear() { c.inner.Clear() }
