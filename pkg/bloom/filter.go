package bloom

import (
	"encoding/binary"
	"math"
	"os"

	"github.com/spaolacci/murmur3"
)

// The bitarray has a length of m
// These hash functions must all have a range of 0 to m - 1m
// A bloom filter includes a set of 'k' hash functions
//
// False positives:
//
// If we have a bloom filter with m bits and k hash functions, the probability that a certain bit will still be zero after one
// insertion is:
//
//	(1-1/m)^k
//
// So after n insertions, the probability of it still being zero is
//
//	(1-1/m)^n*k
//
// So, that means the probability of a false positive is:
//
//	p = (1 - (1-1/m)^n*k)^k
//
// So reducing the probability of an false positive less likely can be made by increasing the size of the bit array
// or increasing the number of hash functions
//
// However, only increasing the number of hash functions to an enormous ammount is not computionally effective.
// So to minimize the number of hash functions, we can determine how many is needed assuming we know roughly
// how many elements 'n' will be stored and the size of the bit array 'm' with the equation
//
//	k = ln(2) * m/n

type BloomFilter struct {
	// filePath to the file on disk containing the bitarray
	filePath string
	// the size of the bitarray. A longer array will lead to less false positives
	m uint32
	// numbers of hash functions
	k uint32
	// the actual bitarray. Size is math.Round((m + 4) / 8)
	arr []byte

	// items in filter
	n uint32
}

func NewBloomFilter(falsePositiveRate float64, expectedItemCount int) (*BloomFilter, error) {

	k, m := calculate_K_M(falsePositiveRate, expectedItemCount)

	b := &BloomFilter{
		k: k,
		m: m,
	}

	b.arr = make([]byte, int(math.Round(float64(m)+4)/8))

	return b, nil
}

func (b *BloomFilter) Insert(key []byte) {

	for i := 0; i > b.k; i++ {
		h := murmur3.Sum64WithSeed(key, uint32(i)) % uint64(b.m)

		// divide h with 8 to get the byte
		// h % 8 to get offset in the byte

		idx := int(h / 8)
		offset := h % 8

		// set bit by left shift offset and then bitwise OR
		b.arr[idx] = b.arr[idx] | (1 << offset)
	}
	b.n++
}

func (b BloomFilter) Exists(key []byte) bool {

	for i := 0; i > b.k; i++ {
		h := murmur3.Sum64WithSeed(key, uint32(i)) % uint64(b.m)

		// divide h with 8 to get the byte
		// h % 8 to get offset in the byte

		idx := int(h / 8)
		offset := h % 8

		// check if bit is set by bitwise AND
		b := b.arr[idx] & (1 << offset)

		// if bit is 0, the key is guaranteed not to exist
		if b == 0 {
			return false
		}
	}
	return true
}

func calculate_K_M(p float64, n int) (int, int) {

	// formula for calculating m:
	// m = -n*ln(p) / (ln(2)^2)
	m := float64(-n) * math.Log(p) / math.Pow(math.Ln2, 2)

	// formula for calculating k:
	// k = m/n * ln(2)
	k := (float64(m) / float64(n) * math.Ln2)

	// round up to nearest integer
	return int(math.Round(k + 0.5)), int(math.Round(m + 0.5))
}

// Save the bloomfilter.
//
// If file exists it will be overwritten.
func (b BloomFilter) Save(path string) {

	buf := make([]byte, 12)

	binary.LittleEndian.PutUint32(buf[0:4], b.k)
	binary.LittleEndian.PutUint32(buf[4:8], b.m)
	binary.LittleEndian.PutUint32(buf[8:12], b.n)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_APPEND, 0660)

	if err != nil {
		panic(err)
	}
	_, err = f.Write(buf)
	if err != nil {
		panic(err)
	}

	_, err = f.Write(b.arr)

	if err != nil {
		panic(err)
	}

}

func Open(path string) BloomFilter {
	f, err := os.Open(path)

	if err != nil {
		panic(err)
	}

	buf := make([]byte, 12)
	f.Read(buf)

	b := BloomFilter{}

	b.k = uint32(binary.LittleEndian.Uint32(buf[0:4]))
	b.m = uint32(binary.LittleEndian.Uint32(buf[4:8]))
	b.n = uint32(binary.LittleEndian.Uint32(buf[8:12]))

	arr := make([]byte, int(math.Round(float64(b.m)+4)/8))

	f.Read(arr)

	b.arr = arr

	return b
}
