package hash

// More info: http://www.isthe.com/chongo/tech/comp/fnv/index.html#FNV-source

/*
	hash = offset_basis
	for each octet_of_data to be hashed
		hash = hash * FNV_prime
		hash = hash xor octet_of_data
	return hash
*/
func fnv_1_32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func FNV132(key string) uint32 {
	return fnv_1_32(key)
}

/*
	hash = offset_basis
	for each octet_of_data to be hashed
		hash = hash xor octet_of_data
		hash = hash * FNV_prime
	return hash
*/
func fnv_1a_32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}

func FNV1A32(key string) uint32 {
	return fnv_1a_32(key)
}
