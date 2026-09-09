package main

import (
	"fmt"
	"time"

	"github.com/semmidev/go-cache/cache"
)

// ImageAsset merepresentasikan objek gambar pada In-Memory CDN / Static Asset Cache.
type ImageAsset struct {
	URL        string
	Format     string
	SizeBytes  int64
	DataBuffer []byte
}

func main() {
	fmt.Println("=================================================================")
	fmt.Println("   go-cache - Sharded Weighted LRU Cache (Cloudflare Pingora Style)")
	fmt.Println("=================================================================")

	// -------------------------------------------------------------------------
	// 1. Inisialisasi LRUCache dengan Batas Bobot (Memory Weight Limit)
	// -------------------------------------------------------------------------
	// Batas bobot maksimum diset ke 500 KB (512,000 Byte).
	// Menggunakan 16 shard independen untuk skalabilitas multi-core concurrent access.
	const weightLimitBytes int64 = 500 * 1024 // 500 KB

	fmt.Printf("\n[1] Membuka Sharded Weighted LRU Cache (Batas Memory Limit: %d KB, 16 Shards)...\n", weightLimitBytes/1024)

	lru := cache.NewLRU[string, ImageAsset](
		cache.WithWeightLimit(weightLimitBytes),
		cache.WithLRUShards(16),
		cache.WithLRUCapacity(100),
	)

	// -------------------------------------------------------------------------
	// 2. Simulasi Menyimpan Asset Gambar dengan Ukuran Byte Berbeda (PutWithWeight)
	// -------------------------------------------------------------------------
	fmt.Println("\n[2] Menyimpan beberapa aset gambar ke dalam In-Memory CDN Cache:")

	images := []ImageAsset{
		{URL: "/images/hero-banner.png", Format: "png", SizeBytes: 150 * 1024}, // 150 KB
		{URL: "/images/logo-company.svg", Format: "svg", SizeBytes: 20 * 1024}, // 20 KB
		{URL: "/images/product-1.jpg", Format: "jpg", SizeBytes: 120 * 1024},   // 120 KB
		{URL: "/images/product-2.jpg", Format: "jpg", SizeBytes: 110 * 1024},   // 110 KB
	}

	for _, img := range images {
		// Menggunakan PutWithWeight untuk mencatat bobot riil (bytes) dari item
		lru.PutWithWeight(img.URL, img, img.SizeBytes, 10*time.Minute)
		fmt.Printf("   ✓ Stored: %-25s | Format: %-4s | Size: %3d KB\n", img.URL, img.Format, img.SizeBytes/1024)
	}

	hits, misses, evictedLen, evictedWeight, size, currentWeight := lru.Stats()
	fmt.Printf("\n📊 Stats Cache Saat Ini:\n")
	fmt.Printf("   • Jumlah Item Aktif : %d\n", size)
	fmt.Printf("   • Total Weight Memori: %d KB / %d KB\n", currentWeight/1024, weightLimitBytes/1024)
	fmt.Printf("   • Total Hits/Misses  : %d / %d\n", hits, misses)
	fmt.Printf("   • Evicted Count/Weight: %d item (%d KB)\n", evictedLen, evictedWeight/1024)

	// -------------------------------------------------------------------------
	// 3. Mengakses Asset (Hits) & Menandai Sebagai Hot Data (MRU)
	// -------------------------------------------------------------------------
	fmt.Println("\n[3] Simulating Cache GET Requests (Hits & Misses):")

	reqURLs := []string{
		"/images/hero-banner.png",    // Hit -> Dipromosikan ke Most Recently Used (MRU)
		"/images/logo-company.svg",   // Hit -> Dipromosikan ke MRU
		"/images/unknown-avatar.png", // Miss -> Tidak ada di cache
	}

	for _, url := range reqURLs {
		if asset, ok := lru.Get(url); ok {
			fmt.Printf("   ✅ GET %-27s -> HIT  (Size: %d KB)\n", url, asset.SizeBytes/1024)
		} else {
			fmt.Printf("   ❌ GET %-27s -> MISS\n", url)
		}
	}

	// -------------------------------------------------------------------------
	// 4. Pengujian Auto-Eviction Berbasis Memory Weight Limit (Pingora P2C)
	// -------------------------------------------------------------------------
	fmt.Println("\n[4] Pengujian Eviction Saat Menambahkan Gambar Berukuran Besar (> Weight Limit):")

	// Total bobot saat ini ~400 KB. Kita tambahkan gambar baru berukuran 200 KB (Total akan melebihi 500 KB limit).
	largeImage := ImageAsset{
		URL:       "/images/highres-gallery.webp",
		Format:    "webp",
		SizeBytes: 200 * 1024, // 200 KB
	}
	fmt.Printf("   📥 Menambahkan aset baru: %s (%d KB)...\n", largeImage.URL, largeImage.SizeBytes/1024)
	lru.PutWithWeight(largeImage.URL, largeImage, largeImage.SizeBytes, 10*time.Minute)

	hits, misses, evictedLen, evictedWeight, size, currentWeight = lru.Stats()
	fmt.Printf("\n📊 Stats Cache Setelah Eviction Otomatis:\n")
	fmt.Printf("   • Jumlah Item Aktif : %d\n", size)
	fmt.Printf("   • Total Weight Memori: %d KB (TETAP DI BAWAH %d KB LIMIT!)\n", currentWeight/1024, weightLimitBytes/1024)
	fmt.Printf("   • Total Hits/Misses  : %d / %d\n", hits, misses)
	fmt.Printf("   • Total Evicted Items: %d item\n", evictedLen)
	fmt.Printf("   • Total Evicted Weight: %d KB\n", evictedWeight/1024)

	// Verifikasi ketersediaan aset terlama (LRU)
	fmt.Println("\n   🔍 Verifikasi Status Item Setelah Eviction:")
	checkURLs := []string{
		"/images/hero-banner.png",      // Di-access baru saja -> Seharusnya TETAP ADA
		"/images/logo-company.svg",     // Di-access baru saja -> Seharusnya TETAP ADA
		"/images/product-1.jpg",        // Kurang sering diakses -> Mungkin ter-evict
		"/images/highres-gallery.webp", // Baru dimasukkan -> TETAP ADA
	}

	for _, url := range checkURLs {
		if _, ok := lru.Get(url); ok {
			fmt.Printf("      • %-27s -> 🟢 Status: ACTIVE\n", url)
		} else {
			fmt.Printf("      • %-27s -> 🔴 Status: EVICTED (dikeluarkan demi menghemat memori)\n", url)
		}
	}

	// -------------------------------------------------------------------------
	// 5. Penyesuaian Bobot Dinamis (IncrementWeight / SetWeight)
	// -------------------------------------------------------------------------
	fmt.Println("\n[5] Demonstrasi Penyesuaian Bobot Dinamis (Incremental Range Requests):")
	// Umpamanya ada file chunk yang ukurannya tumbuh secara bertahap saat dimuat
	chunkURL := "/stream/video-chunk-01.mp4"
	lru.PutWithWeight(chunkURL, ImageAsset{URL: chunkURL, Format: "mp4", SizeBytes: 50 * 1024}, 50*1024)
	fmt.Printf("   ✓ Initial Chunk Stored: %s (Weight: 50 KB)\n", chunkURL)

	// Tambah bobot sebesar 30 KB
	newWeight := lru.IncrementWeight(chunkURL, 30*1024, nil)
	fmt.Printf("   ✓ Weight Incremented   : %s -> New Weight: %d KB\n", chunkURL, newWeight/1024)

	fmt.Println("\n=================================================================")
	fmt.Println("   Selesai. Weighted LRU Cache terbukti mampu membatasi memori! ")
	fmt.Println("=================================================================")
}
