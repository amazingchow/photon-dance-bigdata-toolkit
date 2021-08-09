package hash

// More info: https://www.programmingalgorithms.com/algorithm/bkdr-hash/cpp/

func bkdr_hash(key string) uint32 {
	hash := uint32(0)
	const seed uint32 = 131
	for i := 0; i < len(key); i++ {
		hash = (hash * seed) + uint32(key[i])
	}
	return (hash & 0x7fffffff)
}
