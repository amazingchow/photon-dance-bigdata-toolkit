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

// StandardBloomFilter implements the Standard-Bloom-Filter mentioned by
// "Space/Time Trade-Offs in Hash Coding with Allowable Errors".
// More info:
//     1) math  : http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
//     2) usage : https://shuwoom.com/?p=857
type StandardBloomFilter struct {
	mu sync.RWMutex

	bset     []uint32
	cap      uint32
	cnt      uint64
	readOnly bool

	hashCluster []hash.HashFunc
}

func NewStandardBloomFilter(cap uint32) *StandardBloomFilter {
	if cap == 0 {
		cap = 1024 * 1024 * 256
	}
	cap = resizeInputCap(cap)

	return &StandardBloomFilter{
		bset:        make([]uint32, (cap/_BitPerWord)+1),
		cap:         cap,
		cnt:         0,
		readOnly:    false,
		hashCluster: registerHashCluster(),
	}
}

func resizeInputCap(cap uint32) uint32 {
	const twoMb uint32 = 2 * 1024 * 1024

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
func (bf *StandardBloomFilter) Insert(x string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	if bf.readOnly {
		log.Warn().Msgf("you should expand the capcity <cap: %d>", bf.cap)
		return
	}

	for _, h := range bf.hashCluster {
		bf.set(h(x) % bf.cap)
	}
	log.Debug().Msgf("%s has been inserted", x)

	bf.cnt++
	if bf.reachTheUpLimit() {
		bf.readOnly = true
	}
}

// Member checks whether the string item existed or not.
// 不存在一定不存在, 存在可能不存在, 存在一定的误判率.
func (bf *StandardBloomFilter) Member(x string) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	for _, h := range bf.hashCluster {
		if bf.test(h(x)%bf.cap) == 0 {
			log.Debug().Msgf("%s is not the member", x)
			return false
		}
	}
	log.Debug().Msgf("%s is the member", x)
	return true
}

func (bf *StandardBloomFilter) set(i uint32) {
	bf.bset[i>>_Shift] |= (1 << (i & _Mask))
}

// We should not do clear-op in StandardBloomFilter.
func (bf *StandardBloomFilter) clear(i uint32) { // nolint
	bf.bset[i>>_Shift] &= util.BitReverseUint32(1 << (i & _Mask))
}

func (bf *StandardBloomFilter) test(i uint32) uint32 {
	return bf.bset[i>>_Shift] & (1 << (i & _Mask))
}

// TODO: Resize the bset when StandardBloomFilter reaches the up-limitation

// TODO: Add one more bset to record what we want them to be deleted

/*
	p ~= (1 - e^(-k*n/m))^k, m = len(bset), n = cnt, k = num(hash_cluster)
	if we want to make sure the p stay the resonable value, make n < m * ln2 / k
*/
func (bf *StandardBloomFilter) reachTheUpLimit() bool {
	return float64(bf.cnt) >= float64(bf.cap)*_ln2_div_3
}
