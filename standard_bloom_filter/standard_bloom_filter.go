package bloomfilter

import (
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/amazingchow/photon-dance-bigdata-toolkit/hash"
	"github.com/amazingchow/photon-dance-bigdata-toolkit/util"
)

const (
	// use uint32 as store block
	_BitPerWord uint32 = 32
	_Shift      uint32 = 5
	_Mask       uint32 = 0x1f
)

const (
	_ln2_div_3 float64 = 0.231049
)

type BitSet []uint32

// BloomFilter implements the Standard-Bloom-Filter mentioned by
// "Space/Time Trade-Offs in Hash Coding with Allowable Errors".
// More info:
//     1) math  : http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
//     2) usage : https://shuwoom.com/?p=857
type BloomFilter struct {
	mu sync.RWMutex

	bitset   BitSet
	cap      uint32
	cnt      uint64
	readOnly bool

	markBitset  BitSet
	markDeleted bool

	hashCluster []hash.HashFunc
}

func NewBloomFilter(cap uint32, withMarkDeleted bool) *BloomFilter {
	if cap == 0 {
		cap = 1024 * 1024 * 256
	}
	cap = resizeCap(cap)

	bf := &BloomFilter{
		bitset:      make([]uint32, (cap/_BitPerWord)+1),
		cap:         cap,
		cnt:         0,
		readOnly:    false,
		hashCluster: registerHashCluster(),
	}

	if withMarkDeleted {
		bf.markBitset = make([]uint32, (cap/_BitPerWord)+1)
		bf.markDeleted = true
	}

	return bf
}

func resizeCap(cap uint32) uint32 {
	const twoMb uint32 = 1024 * 1024 * 2

	var x uint32 = twoMb
	for cap > x {
		x += twoMb
	}
	return x
}

func registerHashCluster() []hash.HashFunc {
	hashes := make([]hash.HashFunc, 3)
	hashes[0] = hash.DoubleHashing_7
	hashes[1] = hash.DoubleHashing_13
	hashes[2] = hash.DoubleHashing_19
	return hashes
}

// Insert inserts a string item.
func (bf *BloomFilter) Insert(x string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	if bf.readOnly {
		log.Warn().Msgf("BloomFilter has reached the up-limit <cap: %d>", bf.cap)
		log.Info().Msg(util.MemUsage())
		return
	}

	for _, h := range bf.hashCluster {
		bf.bitset.set(h(x) % bf.cap)
	}
	log.Debug().Msgf("%s has been inserted", x)

	bf.cnt++
	if bf.reachTheUpLimit() {
		bf.readOnly = true
	}
}

// Member checks whether the string item existed or not.
// 不存在一定不存在, 存在可能不存在, 存在一定的误判率.
func (bf *BloomFilter) Member(x string) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	for _, h := range bf.hashCluster {
		if bf.bitset.test(h(x)%bf.cap) == 0 {
			log.Debug().Msgf("%s is not the member", x)
			return false
		}
	}

	if bf.markDeleted {
		hasMarked := true
		for _, h := range bf.hashCluster {
			if bf.markBitset.test(h(x)%bf.cap) == 0 {
				hasMarked = false
				break
			}
		}
		if hasMarked {
			return false
		}
	}

	log.Debug().Msgf("%s is the member", x)
	return true
}

func (bf *BloomFilter) member(x string) bool {
	for _, h := range bf.hashCluster {
		if bf.bitset.test(h(x)%bf.cap) == 0 {
			return false
		}
	}
	return true
}

// MarkDelete marks a string item as deleted if it already existed.
func (bf *BloomFilter) MarkDelete(x string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	if !bf.markDeleted || !bf.member(x) {
		return
	}

	for _, h := range bf.hashCluster {
		bf.markBitset.set(h(x) % bf.cap)
	}
	log.Debug().Msgf("%s has been marked deleted", x)
}

func (bs BitSet) set(i uint32) {
	bs[i>>_Shift] |= (1 << (i & _Mask))
}

func (bs BitSet) clear(i uint32) { //nolint
	bs[i>>_Shift] &= util.BitReverseUint32(1 << (i & _Mask))
}

func (bs BitSet) test(i uint32) uint32 {
	return bs[i>>_Shift] & (1 << (i & _Mask))
}

/*
	p ~= (1 - e^(-k*n/m))^k, m = len(bitset), n = cnt, k = num(hash_cluster)
	if we want to make sure the p stay the resonable value, make n < m * ln2 / k
*/
func (bf *BloomFilter) reachTheUpLimit() bool {
	return float64(bf.cnt) >= float64(bf.cap)*_ln2_div_3
}
