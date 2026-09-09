package cache

import (
	"encoding/gob"
	"fmt"
	"io"
	"time"
)

// persistentEntry merepresentasikan format data serialisasi entri LRU untuk disimpan ke storage/stream.
type persistentEntry[K comparable, V any] struct {
	Key      K
	Value    V
	Weight   int64
	ExpireAt int64
}

// Dump menyimpulkan seluruh state entri aktif di dalam LRUCache ke io.Writer menggunakan binary gob encoding,
// terinspirasi oleh fitur persistence (`persistence.rs`) pada Cloudflare Pingora.
func (c *LRUCache[K, V]) Dump(w io.Writer) error {
	enc := gob.NewEncoder(w)

	// Kumpulkan seluruh node dari tiap shard
	var entries []persistentEntry[K, V]
	now := time.Now().UnixNano()

	for _, s := range c.shards {
		s.mu.RLock()
		curr := s.list.head
		for curr != nil {
			if !curr.expired(now) {
				entries = append(entries, persistentEntry[K, V]{
					Key:      curr.key,
					Value:    curr.value,
					Weight:   curr.weight,
					ExpireAt: curr.expireAt,
				})
			}
			curr = curr.next
		}
		s.mu.RUnlock()
	}

	if err := enc.Encode(len(entries)); err != nil {
		return fmt.Errorf("lru_persistence: failed to encode entry count: %w", err)
	}

	for _, entry := range entries {
		if err := enc.Encode(entry); err != nil {
			return fmt.Errorf("lru_persistence: failed to encode entry: %w", err)
		}
	}

	return nil
}

// Restore membaca dan mengembalikan state entri dari io.Reader ke dalam LRUCache.
func (c *LRUCache[K, V]) Restore(r io.Reader) error {
	dec := gob.NewDecoder(r)

	var count int
	if err := dec.Decode(&count); err != nil {
		return fmt.Errorf("lru_persistence: failed to decode entry count: %w", err)
	}

	now := time.Now().UnixNano()
	for range count {
		var entry persistentEntry[K, V]
		if err := dec.Decode(&entry); err != nil {
			return fmt.Errorf("lru_persistence: failed to decode entry: %w", err)
		}

		if entry.ExpireAt > 0 && now > entry.ExpireAt {
			continue
		}

		var ttl time.Duration
		if entry.ExpireAt > 0 {
			ttl = time.Duration(entry.ExpireAt - now)
		}
		c.PutWithWeight(entry.Key, entry.Value, entry.Weight, ttl)
	}

	return nil
}
