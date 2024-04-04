package lsmtree

import (
	"hash"
	"math"
	"math/rand"
	"sync"

	"github.com/twmb/murmur3"
)

const (
	DefaultFilterSize     = 1000000
	DefaultErrorRate      = 0.01
	DefaultFlushThreshold = 0.8
)

var (
	ln2      float64 = math.Log(2)
	ln2Power float64 = ln2 * ln2
)

type FilterParameters struct {
	Capacity    int
	HashFuncs   []hash.Hash64
	BitsPerElem float64
	NumOfBits   int
}

type CustomBloomFilter struct {
	Params     FilterParameters
	HashRWLock sync.RWMutex
	BloomLock  sync.RWMutex
	Bitset     []uint64
}

type CustomBloomFilterOptions struct {
	Capacity  int
	ErrorRate float64
}

func NewCustomBloomFilter(options CustomBloomFilterOptions) *CustomBloomFilter {

	var filterParams FilterParameters
	filterParams.Capacity = options.Capacity

	filterParams.BitsPerElem = -1 * math.Log(options.ErrorRate) / ln2Power

	k := math.Ceil(filterParams.BitsPerElem * ln2)
	filterParams.HashFuncs = make([]hash.Hash64, int(k))

	for i := 0; i < int(k); i++ {
		filterParams.HashFuncs[i] = murmur3.SeedNew64(rand.Uint64())
	}

	bitset := make([]uint64, DefaultFilterSize)
	filterParams.NumOfBits = DefaultFilterSize * 64

	bloomFilter := &CustomBloomFilter{
		Params: filterParams,
		Bitset: bitset,
	}

	return bloomFilter
}

func (cf *CustomBloomFilter) Add(key string) {
	cf.BloomLock.Lock()
	defer cf.BloomLock.Unlock()

	for _, hashFunc := range cf.Params.HashFuncs {
		cf.HashRWLock.Lock()
		hashFunc.Reset()
		hashFunc.Write([]byte(key))
		hashValue := hashFunc.Sum64() % uint64(cf.Params.NumOfBits)
		setBit(cf.Bitset, hashValue)
		cf.HashRWLock.Unlock()
	}
}

func (cf *CustomBloomFilter) Contains(key string) bool {
	cf.BloomLock.RLock()
	defer cf.BloomLock.RUnlock()

	for _, hashFunc := range cf.Params.HashFuncs {
		cf.HashRWLock.Lock()
		hashFunc.Reset()
		hashFunc.Write([]byte(key))
		hashValue := hashFunc.Sum64() % uint64(cf.Params.NumOfBits)
		if !hasBit(cf.Bitset, hashValue) {
			cf.HashRWLock.Unlock()
			return false
		}
		cf.HashRWLock.Unlock()
	}

	return true
}

func hasBit(bitset []uint64, bitIndex uint64) bool {
	return bitset[bitIndex>>6]&(1<<uint(bitIndex%64)) != 0
}

func setBit(bitset []uint64, bitIndex uint64) {
	bitset[bitIndex>>6] |= (1 << uint(bitIndex%64))
}
