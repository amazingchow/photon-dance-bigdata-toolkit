package hash

// More info: http://www.isthe.com/chongo/tech/comp/fnv/index.html#FNV-source

/*
	hash = offset_basis
	for each octet_of_data to be hashed
		hash = hash * FNV_prime
		hash = hash xor octet_of_data
	return hash
*/
func fnv_1_64(key string) uint64 {
	hash := uint64(14695981039346656037)
	const prime64 = uint64(1099511628211)
	for i := 0; i < len(key); i++ {
		hash *= prime64
		hash ^= uint64(key[i])
	}
	return hash
}

func FNV164(key string) uint64 {
	return fnv_1_64(key)
}

/*
	hash = offset_basis
	for each octet_of_data to be hashed
		hash = hash xor octet_of_data
		hash = hash * FNV_prime
	return hash
*/
func fnv_1a_64(key string) uint64 {
	hash := uint64(14695981039346656037)
	const prime64 = uint64(1099511628211)
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= prime64
	}
	return hash
}

func FNV1A64(key string) uint64 {
	return fnv_1a_64(key)
}
