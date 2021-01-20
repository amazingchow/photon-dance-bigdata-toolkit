package hash

import "unsafe"

/*
	Forked from Austin Appleby's cpp version.

	!!!Note - this code makes a few assumptions about how your machine behaves:

	1. we can read a 4-byte value from any address without crashing.
	2. sizeof(int) == 4.

	And it has a few limitations -

	1. it will not work incrementally.
	2. it will not produce the same result on little-endian / big-endian machine.
*/

// More info: https://github.com/aappleby/smhasher/blob/master/src/MurmurHash2.h

func murmur_hash_2(key string) uint32 {
	// 'm' and 'r' are not really magic-number, they just happen to work well here.
	const m uint32 = 0x5bd1e995
	const r uint = 24

	// Initialize the hash to a random value.
	_len := len(key)
	var len uint32 = *(*uint32)(unsafe.Pointer(&_len))
	var seed uint32 = 0xbc9f1d34 // from Jeff Dean's LevelDB
	var h uint32 = seed ^ len

	// Mix 4 bytes at a time into the hash.
	var data []byte = []byte(key)
	idx := 0
	for len >= 4 {
		var k uint32 = *(*uint32)(unsafe.Pointer(&data[idx]))

		k *= m
		k ^= k >> r
		k *= m

		h *= m
		h ^= k

		idx += 4
		len -= 4
	}

	// Handle the last few bytes of the input array.
	switch len {
	case 3:
		x := uint(data[idx+2]) << 16
		h ^= *(*uint32)(unsafe.Pointer(&x))
		fallthrough
	case 2:
		x := uint(data[idx+1]) << 8
		h ^= *(*uint32)(unsafe.Pointer(&x))
		fallthrough
	case 1:
		x := uint(data[idx])
		h ^= *(*uint32)(unsafe.Pointer(&x))
		h *= m
	}

	// Do a few final mixes of the hash to ensure the last few bytes are well-incorporated.
	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h
}
