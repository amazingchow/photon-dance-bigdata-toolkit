package bloomfilter

import (
	"math"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/amazingchow/photon-dance-bigdata-toolkit/pkg/hash"
	"github.com/amazingchow/photon-dance-bigdata-toolkit/pkg/util"
)

const (
	// use uint32 as store block
	_BitPerWord uint32 = 32
	_Shift      uint32 = 5
	_Mask       uint32 = 0x1f
)

const (
	_ln2    float64 = 0.6931472
	_ln_1_2 float64 = -0.693147
	_p      float64 = 1e-6
	_4g     uint64  = 4294967296
)

// StandardBloomFilter implements the Standard-Bloom-Filter mentioned by
// "Space/Time Trade-Offs in Hash Coding with Allowable Errors".
type StandardBloomFilter struct {
	mu sync.Mutex

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
		log.Warn().Msgf("you should expand the capcity <cap: %d> to maintain false positive probability <p: %f>", bf.cap, _p)
		return
	}

	for _, h := range bf.hashCluster {
		bf.set(h(x) % bf.cap)
	}
	log.Debug().Msgf("%s has been inserted", x)

	bf.cnt++
	if bf.exceedFalsePositiveProb() {
		bf.readOnly = true
	}
}

// Member checks whether the string item existed or not.
func (bf *StandardBloomFilter) Member(x string) bool {
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

func (bf *StandardBloomFilter) clear(i uint32) {
	bf.bset[i>>_Shift] &= util.BitReverseUint32(1 << (i & _Mask))
}

func (bf *StandardBloomFilter) test(i uint32) uint32 {
	return bf.bset[i>>_Shift] & (1 << (i & _Mask))
}

// TODO: Resize

/*
	p ~= (1 - e^(-k*n/m))^k
	k = (m/n) * ln2
    -->
	p ~= 2^(ln(1/2)*m/n), make x = ln(1/2)*m/n
*/
func (bf *StandardBloomFilter) exceedFalsePositiveProb() bool {
	x := _ln_1_2 * float64(bf.cap) / float64(bf.cnt)
	p := math.Pow(2.0, x)
	return (1 / p) > _p
}
