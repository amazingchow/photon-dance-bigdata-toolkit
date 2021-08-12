package cuckoofilter

import (
	"math/rand"
	"unsafe"

	"github.com/amazingchow/photon-dance-bigdata-toolkit/hash"
)

var (
	_Masks              = [65]uint{}
	_HashForFingerprint = [256]uint{}
)

func init() {
	for i := uint(0); i <= 64; i++ {
		_Masks[i] = (1 << i) - 1
	}
	for i := 0; i < 256; i++ {
		_HashForFingerprint[i] = uint(hash.MURMUR2(Bytes2String([]byte{byte(i)})))
	}
}

func GetFingerprint(x string) Fingerprint {
	// use least significant bits for fingerprint
	return Fingerprint(hash.MURMUR2(x)%255 + 1)
}

func GetOneIndex(x string, bucketPow uint) uint {
	hash := uint(hash.MURMUR2(x))
	// use most significant bits for derived index
	i1 := hash >> 32 & _Masks[bucketPow]
	return i1
}

func GetAnotherIndex(i uint, fp Fingerprint, bucketPow uint) uint {
	mask := _Masks[bucketPow]
	hash := _HashForFingerprint[fp] & mask
	return (i & mask) ^ hash
}

func RandomlySelect(i1, i2 uint) uint {
	if rand.Intn(2) == 0 {
		return i1
	}
	return i2
}

// Bytes2String fast type conversion from byte array to string, both share the same mem pointer.
func Bytes2String(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}
