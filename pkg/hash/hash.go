package hash

type HashFunc func(key string) uint32

// DoubleHashing provides double-hashing technique: hi(x) = h1(x) + f(x) * h2(x), f(x) = i * i
func DoubleHashing(key string, factor uint32) uint32 {
	return murmur_hash_2(key) + (factor*factor)*fnv_1a_32(key)
}

func DoubleHashing_2(key string) uint32 {
	return DoubleHashing(key, 2)
}

func DoubleHashing_3(key string) uint32 {
	return DoubleHashing(key, 3)
}

func DoubleHashing_5(key string) uint32 {
	return DoubleHashing(key, 5)
}

func DoubleHashing_7(key string) uint32 {
	return DoubleHashing(key, 7)
}

func DoubleHashing_11(key string) uint32 {
	return DoubleHashing(key, 11)
}

func DoubleHashing_13(key string) uint32 {
	return DoubleHashing(key, 13)
}

func DoubleHashing_17(key string) uint32 {
	return DoubleHashing(key, 17)
}

func DoubleHashing_19(key string) uint32 {
	return DoubleHashing(key, 19)
}

// TripleHashing provides triple-hashing technique: hi(x) = h1(x) + f(x) * h2(x) + g(x) * h3(x), f(x) = i, g(x) = i * i
func TripleHashing(key string, factor uint32) uint32 {
	return murmur_hash_2(key) + factor*fnv_1a_32(key) + (factor*factor)*bkdr_hash(key)
}

func TripleHashing_2(key string) uint32 {
	return TripleHashing(key, 2)
}

func TripleHashing_3(key string) uint32 {
	return TripleHashing(key, 3)
}

func TripleHashing_5(key string) uint32 {
	return TripleHashing(key, 5)
}

func TripleHashing_7(key string) uint32 {
	return TripleHashing(key, 7)
}

func TripleHashing_11(key string) uint32 {
	return TripleHashing(key, 11)
}

func TripleHashing_13(key string) uint32 {
	return TripleHashing(key, 13)
}

func TripleHashing_17(key string) uint32 {
	return TripleHashing(key, 17)
}

func TripleHashing_19(key string) uint32 {
	return TripleHashing(key, 19)
}
