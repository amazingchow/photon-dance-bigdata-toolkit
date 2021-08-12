package cuckoofilter

import (
	"fmt"
	"math/bits"
	"math/rand"
	"sync"
)

const (
	_MaxNumKicks = 500
)

// CuckooFilter implements the Standard-Cuckoo-Filter mentioned by
// "Cuckoo Filter: Practically Better Than Bloom".
type CuckooFilter struct {
	mu sync.RWMutex

	buckets   []Bucket
	bucketPow uint
	count     uint
}

func NewCuckooFilter(cap uint) *CuckooFilter {
	if cap == 0 {
		cap = 1024 * 1024 * 256
	}
	cap = resizeCap(cap) / _BucketSize

	return &CuckooFilter{
		buckets:   make([]Bucket, cap),
		bucketPow: uint(bits.TrailingZeros(cap)),
		count:     0,
	}
}

func resizeCap(cap uint) uint {
	cap--
	cap |= cap >> 1
	cap |= cap >> 2
	cap |= cap >> 4
	cap |= cap >> 8
	cap |= cap >> 16
	cap |= cap >> 32
	cap++
	return cap
}

// Insert inserts a string item.
/*
	f = fingerprint(x);
	i_1 = hash(x);
	i_2 = i_1 ⊕ hash(f);
	if bucket[i_1] or bucket[i_2] has an empty entry then
		add f to that bucket;
	return Done;
	// must relocate existing items;
	i = randomly pick i_1 or i_2;
	for n = 0; n < MaxNumKicks; n++ do
		randomly select an entry e from bucket[i];
		swap f and the fingerprint stored in entry e;
		i = i ⊕ hash(f);
		if bucket[i] has an empty entry then
			add f to bucket[i];
			return Done;
	// Hashtable is considered full;
	return Failure;
*/
func (cf *CuckooFilter) Insert(x string) bool {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	fp := GetFingerprint(x)
	i1 := GetOneIndex(x, cf.bucketPow)
	if cf.insert(i1, fp) {
		return true
	}
	i2 := GetAnotherIndex(i1, fp, cf.bucketPow)
	if cf.insert(i2, fp) {
		return true
	}
	return cf.reinsert(RandomlySelect(i1, i2), fp)
}

func (cf *CuckooFilter) insert(i uint, fp Fingerprint) bool {
	if cf.buckets[i].Insert(fp) {
		cf.count++
		return true
	}
	return false
}

func (cf *CuckooFilter) reinsert(i uint, fp Fingerprint) bool {
	for k := 0; k < _MaxNumKicks; k++ {
		j := rand.Intn(_BucketSize)
		fp, cf.buckets[i][j] = cf.buckets[i][j], fp

		i = GetAnotherIndex(i, fp, cf.bucketPow)
		if cf.insert(i, fp) {
			return true
		}
	}
	return false
}

// Lookup returns true if string item is inside CuckooFilter.
/*
	f = fingerprint(x);
	i_1 = hash(x);
	i_2 = i_1 ⊕ hash(f);
	if bucket[i_1] or bucket[i_2] has f then
		return True;
	return False;
*/
func (cf *CuckooFilter) Lookup(x string) bool {
	cf.mu.RLock()
	defer cf.mu.RUnlock()

	fp := GetFingerprint(x)
	i1 := GetOneIndex(x, cf.bucketPow)
	if cf.buckets[i1].GetFingerprintIndex(fp) != -1 {
		return true
	}
	i2 := GetAnotherIndex(i1, fp, cf.bucketPow)
	return cf.buckets[i2].GetFingerprintIndex(fp) != -1
}

// Delete removes string item from CuckooFilter if exists and return true if deleted or not.
/*
	f = fingerprint(x);
	i_1 = hash(x);
	i_2 = i_1 ⊕ hash(f);
	if bucket[i_1] or bucket[i_2] has f then
		remove a copy of f from this bucket;
		return True;
	return False;
*/
func (cf *CuckooFilter) Delete(x string) bool {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	fp := GetFingerprint(x)
	i1 := GetOneIndex(x, cf.bucketPow)
	if cf.delete(i1, fp) {
		return true
	}
	i2 := GetAnotherIndex(i1, fp, cf.bucketPow)
	return cf.delete(i2, fp)
}

func (cf *CuckooFilter) delete(i uint, fp Fingerprint) bool {
	if cf.buckets[i].Delete(fp) {
		cf.count--
		return true
	}
	return false
}

// Reset removes all items from CuckooFilter.
func (cf *CuckooFilter) Reset() {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	for i := range cf.buckets {
		cf.buckets[i].Reset()
	}
	cf.count = 0
}

// Count returns the number of items inside CuckooFilter.
func (cf *CuckooFilter) Count() uint {
	cf.mu.RLock()
	defer cf.mu.RUnlock()

	return cf.count
}

// Serialize returns a byte slice representing a CuckooFilter.
func Serialize(cf *CuckooFilter) []byte {
	bytes := make([]byte, len(cf.buckets)*_BucketSize)
	for i := range cf.buckets {
		for j, fp := range cf.buckets[i] {
			bytes[i*len(cf.buckets[i])+j] = byte(fp)
		}
	}
	return bytes
}

// Deserialize returns a CuckooFilter from a byte slice.
func Deserialize(bytes []byte) (*CuckooFilter, error) {
	if len(bytes)%_BucketSize != 0 {
		return nil, fmt.Errorf("expected input byte slice to be multiple of %d, got %d", _BucketSize, len(bytes))
	}

	var count uint
	buckets := make([]Bucket, len(bytes)/_BucketSize)
	for i := range buckets {
		for j := range buckets[i] {
			idx := i*len(buckets[i]) + j
			if bytes[idx] != _NullFp {
				buckets[i][j] = Fingerprint(bytes[idx])
				count++
			}
		}
	}
	return &CuckooFilter{
		buckets:   buckets,
		bucketPow: uint(bits.TrailingZeros(uint(len(buckets)))),
		count:     count,
	}, nil
}
