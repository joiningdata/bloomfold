package main

import (
	"math"
	"math/bits"
)

type foldedBloomFilter struct {
	parts []uint64
}

func FoldFilter(orig *BloomFilter) *foldedBloomFilter {
	bitsPerItem := float64(orig.size) / float64(orig.advised)
	newsize := uint64(math.Pow(2.0, math.Ceil(math.Log2(bitsPerItem*float64(orig.nadded)))))
	return FoldFilterWithSize(orig, newsize)
}

func FoldFilterWithSize(orig *BloomFilter, newsize uint64) *foldedBloomFilter {
	mod := 1 + (newsize / 64)
	parts := make([]uint64, mod)
	for i, p := range orig.parts {
		parts[uint64(i)%mod] |= p
	}

	return &foldedBloomFilter{parts}
}

// Similarity calculates the overlap of two filters.
func (b *foldedBloomFilter) Similarity(a *foldedBloomFilter) float64 {
	if len(a.parts) != len(b.parts) {
		panic("cannot compare")
	}

	numer, denom := 0, 0
	for i, x := range b.parts {
		numer += bits.OnesCount64(x & a.parts[i])
		denom += bits.OnesCount64(x | a.parts[i])
	}

	return float64(numer) / float64(denom)
}
