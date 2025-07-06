package bloom

import (
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bits     []bool
	size     uint
	hashFunc uint
}

func NewBloomFilter(size uint, hashFunc uint) *BloomFilter {
	return &BloomFilter{
		bits:     make([]bool, size),
		size:     size,
		hashFunc: hashFunc,
	}
}

func (bf *BloomFilter) Add(key string) {
	for i := uint(0); i < bf.hashFunc; i++ {
		index := bf.hash(key, i) % bf.size
		bf.bits[index] = true
	}
}

func (bf *BloomFilter) Contains(key string) bool {
	for i := uint(0); i < bf.hashFunc; i++ {
		index := bf.hash(key, i) % bf.size
		if !bf.bits[index] {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Clear() {
	for i := range bf.bits {
		bf.bits[i] = false
	}
}

func (bf *BloomFilter) hash(key string, seed uint) uint {
	h := fnv.New64a()
	h.Write([]byte(key))
	h.Write([]byte{byte(seed)})
	return uint(h.Sum64())
}

func EstimateSize(n uint, p float64) uint {
	if p <= 0 || p >= 1 {
		return 1000
	}
	return uint(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2)))
}

func EstimateHashFunctions(m, n uint) uint {
	return uint(float64(m) / float64(n) * math.Log(2))
}
