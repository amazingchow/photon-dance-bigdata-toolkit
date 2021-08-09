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

	hashCluster []hash.HashFunc
}

func NewBloomFilter(cap uint32) *BloomFilter {
	if cap == 0 {
		cap = 1024 * 1024 * 256
	}
	cap = resizeCap(cap)

	return &BloomFilter{
		bitset:      make([]uint32, (cap/_BitPerWord)+1),
		cap:         cap,
		cnt:         0,
		readOnly:    false,
		hashCluster: registerHashCluster(),
	}
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
		log.Warn().Msgf("you should expand capcity for BloomFilter <current cap: %d>", bf.cap)
		return
	}

	for _, h := range bf.hashCluster {
		bf.bitset.set(h(x) % bf.cap)
	}
	log.Debug().Msgf("%s has been inserted into BloomFilter", x)

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
	log.Debug().Msgf("%s is the member", x)
	return true
}

func (bs BitSet) set(i uint32) {
	bs[i>>_Shift] |= (1 << (i & _Mask))
}

func (bs BitSet) clear(i uint32) {
	bs[i>>_Shift] &= util.BitReverseUint32(1 << (i & _Mask))
}

func (bs BitSet) test(i uint32) uint32 {
	return bs[i>>_Shift] & (1 << (i & _Mask))
}

// TODO: Resize the bitset when BloomFilter reaches the up-limitation

// TODO: Add one more bitset to record what we want them to be deleted

/*
	p ~= (1 - e^(-k*n/m))^k, m = len(bitset), n = cnt, k = num(hash_cluster)
	if we want to make sure the p stay the resonable value, make n < m * ln2 / k
*/
func (bf *BloomFilter) reachTheUpLimit() bool {
	return float64(bf.cnt) >= float64(bf.cap)*_ln2_div_3
}
