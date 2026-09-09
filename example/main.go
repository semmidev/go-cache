package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/semmidev/go-cache/cache"
)

func main() {
	fmt.Println("=================================================================")
	fmt.Println("   go-cache - High Performance In-Memory & Read-Through Cache    ")
	fmt.Println("=================================================================")

	// -------------------------------------------------------------------------
	// 1. Demonstrasi MemoryCache (TinyUFO + S3-FIFO)
	// -------------------------------------------------------------------------
	fmt.Println("\n[1] Demonstrasi Dasar MemoryCache:")
	mc := cache.New[string, string](cache.WithCapacity(1000))

	// Simpan data dengan TTL 5 Menit
	mc.Put("user:101", "Sammidev", 5*time.Minute)
	if v, ok := mc.Get("user:101"); ok {
		fmt.Printf("   ✓ GET key='user:101' -> %s (Status: HIT)\n", v)
	}

	// Ambil statistik hit & miss
	hits, misses, size := mc.Stats()
	fmt.Printf("   📊 Stats MemoryCache -> Hits: %d, Misses: %d, Item Count: %d\n", hits, misses, size)

	// -------------------------------------------------------------------------
	// 2. Demonstrasi RTCache & Request Coalescing (Anti-Cache Stampede)
	// -------------------------------------------------------------------------
	fmt.Println("\n[2] Demonstrasi RTCache (Pencegahan Cache Stampede / Thundering Herd):")
	rt := cache.NewRTCache[string, string, string](
		cache.WithRTCapacity(100),
		cache.WithLockTimeout(3*time.Second),
		cache.WithLockAge(10*time.Second),
	)

	var dbQueryCount atomic.Int32
	// Callback LookupFunc yang mensimulasikan query database lambat (100ms)
	lookupDB := func(ctx context.Context, key string, extra *string) (string, *time.Duration, error) {
		count := dbQueryCount.Add(1)
		fmt.Printf("   🗄️  [DATABASE QUERY #%d] Mengambil data mahal untuk key '%s'...\n", count, key)
		time.Sleep(100 * time.Millisecond) // Simulasi I/O delay
		ttl := 1 * time.Minute
		return "Hasil-DB-" + key, &ttl, nil
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	concurrentRequests := 20

	fmt.Printf("   🚀 Meluncurkan %d Goroutine serentak meminta key 'hot-product'...\n", concurrentRequests)
	for i := 1; i <= concurrentRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			val, status, err := rt.Get(ctx, "hot-product", nil, nil, lookupDB)
			if err != nil {
				fmt.Printf("      Goroutine #%d Error: %v\n", id, err)
				return
			}
			if id <= 3 { // Tampilkan sampel 3 goroutine pertama
				fmt.Printf("      Goroutine #%d -> Value: '%s', Cache Status: %s\n", id, val, status)
			}
		}(i)
	}
	wg.Wait()

	fmt.Printf("   ✅ Total Query ke Database: %d kali (Seharusnya 1 kali saja via Request Coalescing!)\n", dbQueryCount.Load())

	// -------------------------------------------------------------------------
	// 3. Demonstrasi MultiGet Batching
	// -------------------------------------------------------------------------
	fmt.Println("\n[3] Demonstrasi Batching MultiGet:")
	multiLookup := func(ctx context.Context, keys []string, extra *string) (map[string]string, map[string]time.Duration, error) {
		fmt.Printf("   🗄️  [BATCH DB QUERY] Mengambil %d missing key(s) dari DB: %v\n", len(keys), keys)
		res := make(map[string]string)
		ttls := make(map[string]time.Duration)
		for _, k := range keys {
			res[k] = "DataBatch-" + k
			ttls[k] = 2 * time.Minute
		}
		return res, ttls, nil
	}

	batchKeys := []string{"item-1", "item-2", "item-3"}
	results, statuses, err := rt.MultiGet(ctx, batchKeys, nil, multiLookup)
	if err != nil {
		fmt.Printf("   Error MultiGet: %v\n", err)
	} else {
		for _, k := range batchKeys {
			fmt.Printf("   Item '%s' -> Value: '%s', Status: %s\n", k, results[k], statuses[k])
		}
	}

	fmt.Println("\n=================================================================")
	fmt.Println("   Selesai. Seluruh fitur berjalan dengan sempurna!              ")
	fmt.Println("=================================================================")
}

