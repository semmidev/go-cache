package cache

// lruNode merepresentasikan entri individual dalam doubly-linked list LRU.
type lruNode[K comparable, V any] struct {
	key      K
	value    V
	hash     uint64
	weight   int64
	expireAt int64
	prev     *lruNode[K, V]
	next     *lruNode[K, V]
}

// expired menguji apakah node telah kadaluarsa berdasarkan timestamp nanosecond.
func (n *lruNode[K, V]) expired(now int64) bool {
	if n.expireAt == 0 {
		return false
	}
	return now > n.expireAt
}

// lruList mengimplementasikan doubly-linked list berkinerja tinggi untuk pelacakan urutan akses LRU.
// Head merepresentasikan elemen yang paling baru diakses (MRU),
// dan Tail merepresentasikan elemen yang paling lama tidak diakses (LRU).
type lruList[K comparable, V any] struct {
	head *lruNode[K, V]
	tail *lruNode[K, V]
	len  int
}

// newLRUList membuat dan menginisialisasi lruList kosong.
func newLRUList[K comparable, V any]() *lruList[K, V] {
	return &lruList[K, V]{}
}

// pushFront menyisipkan node di urutan paling depan (MRU).
func (l *lruList[K, V]) pushFront(node *lruNode[K, V]) {
	node.prev = nil
	node.next = l.head

	if l.head != nil {
		l.head.prev = node
	} else {
		l.tail = node
	}
	l.head = node
	l.len++
}

// moveToFront memindahkan node yang sudah ada ke posisi paling depan (MRU).
func (l *lruList[K, V]) moveToFront(node *lruNode[K, V]) {
	if l.head == node {
		return
	}

	// Unlink node dari posisi saat ini
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		// Node sebelumnya adalah tail
		l.tail = node.prev
	}

	// Link node ke paling depan
	node.prev = nil
	node.next = l.head
	if l.head != nil {
		l.head.prev = node
	}
	l.head = node
}

// remove menghapus node dari linked list.
func (l *lruList[K, V]) remove(node *lruNode[K, V]) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}

	node.prev = nil
	node.next = nil
	l.len--
}

// popTail menghapus dan mengembalikan node terbelakang (LRU victim).
func (l *lruList[K, V]) popTail() *lruNode[K, V] {
	if l.tail == nil {
		return nil
	}
	victim := l.tail
	l.remove(victim)
	return victim
}

// isInTopN memeriksa apakah node berada di N posisi terdepan (MRU) dalam linked list.
// Fungsi ini digunakan oleh PromoteTopN untuk menghindari penguncian write jika node sudah di top N.
func (l *lruList[K, V]) isInTopN(node *lruNode[K, V], topN int) bool {
	curr := l.head
	count := 0
	for curr != nil && count < topN {
		if curr == node {
			return true
		}
		curr = curr.next
		count++
	}
	return false
}

// clear mengosongkan linked list.
func (l *lruList[K, V]) clear() {
	l.head = nil
	l.tail = nil
	l.len = 0
}
