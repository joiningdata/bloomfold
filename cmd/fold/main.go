package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	CompThreshold = 0.95
)

type BloomSet struct {
	sz int
	bf map[string]*BloomFilter

	foldbits uint64
	folds    map[string]*foldedBloomFilter
}

func (s *BloomSet) FoldAll(bitsize uint64) {
	s.foldbits = bitsize
	s.folds = make(map[string]*foldedBloomFilter)
	for key, bf := range s.bf {
		fbf := FoldFilterWithSize(bf, bitsize)
		s.folds[key] = fbf
	}
}

func NewBloomSet(size int) *BloomSet {
	return &BloomSet{
		sz: size,
		bf: make(map[string]*BloomFilter),
	}
}

func (s *BloomSet) Add(key string, data []string) {
	bf := &BloomFilter{}
	bf.Advise(s.sz)
	bf.ErrorRate(0.01)

	for _, x := range data {
		bf.Learn(x)
	}
	s.bf[key] = bf
}

func (s *BloomSet) FoldedBestMatches(data []string, topn int) (keys []string, scores []float64, comps int) {
	query := &BloomFilter{}
	query.Advise(s.sz)
	query.ErrorRate(0.01)
	for _, x := range data {
		query.Learn(x)
	}
	foldedQuery := FoldFilterWithSize(query, s.foldbits)

	res := make(map[string]float64)
	for k, other := range s.folds {
		sim := other.Similarity(foldedQuery)
		res[k] = sim
		if sim >= CompThreshold {
			comps++
		}
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return res[keys[i]] > res[keys[j]]
	})
	if len(keys) > topn {
		keys = keys[:topn]
	}
	for _, k := range keys {
		scores = append(scores, res[k])
	}
	return
}

func (s *BloomSet) BestMatches(data []string, topn int) (keys []string, scores []float64, comps int) {
	query := &BloomFilter{}
	query.Advise(s.sz)
	query.ErrorRate(0.01)
	for _, x := range data {
		query.Learn(x)
	}

	res := make(map[string]float64)
	for k, other := range s.bf {
		sim := other.Similarity(query)
		res[k] = sim
		if sim >= CompThreshold {
			comps++
		}
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return res[keys[i]] > res[keys[j]]
	})
	if len(keys) > topn {
		keys = keys[:topn]
	}
	for _, k := range keys {
		scores = append(scores, res[k])
	}
	return
}

func BestMatches(dataset map[string][]string, data []string, topn int) (keys []string, scores []float64, comps int) {

	query := make(map[string]struct{})
	for _, x := range data {
		query[x] = struct{}{}
	}

	res := make(map[string]float64)
	for k, other := range dataset {
		numer, denom := 0.0, float64(len(query))
		for _, x := range other {
			if _, ok := query[x]; ok {
				numer++
			} else {
				denom++
			}
		}

		res[k] = numer / denom
		if denom > 0 && res[k] >= CompThreshold {
			comps++
		}
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return res[keys[i]] > res[keys[j]]
	})
	if len(keys) > topn {
		keys = keys[:topn]
	}
	for _, k := range keys {
		scores = append(scores, res[k])
	}
	return
}

func main() {
	subsetSize := flag.Int("s", 500, "number of subsets to test")
	nitems := flag.Int("n", 0, "`size` of the source population being sampled")
	foldSize := flag.Int("f", 0, "folding size in `bits`")
	flag.Parse()

	if *nitems == 0 {
		log.Println("Need to measure population size...")
		f, err := os.Open(flag.Arg(0))
		if err != nil {
			log.Fatal(err)
		}
		pop := make(map[string]struct{})
		s := bufio.NewScanner(f)
		for s.Scan() {
			row := strings.Split(s.Text(), "\t")
			for _, x := range row[1:] {
				pop[x] = struct{}{}
			}
		}
		f.Close()
		*nitems = len(pop)
		log.Printf("    Use '-n %d' next time to skip this step.", *nitems)
	}

	start := time.Now()
	bs := NewBloomSet(*nitems)
	dataset := make(map[string][]string)
	f, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		row := strings.Split(s.Text(), "\t")
		dataset[row[0]] = row[1:]
		bs.Add(row[0], row[1:])
	}
	f.Close()

	elap := time.Now().Sub(start)
	log.Println("Loading complete in", elap)

	subset := make(map[string]struct{})

	if *subsetSize == 0 {
		*subsetSize = len(dataset)
	}

	////////////////
	// stage 1: calculate true jaccard similarity
	start = time.Now()
	for key, data := range dataset {
		//best, scores, comps := BestMatches(dataset, data, 10)
		//fmt.Println("FULL", key, comps, best, scores)
		_, _, comps := BestMatches(dataset, data, 10)
		fmt.Println("FULL", key, comps)
		subset[key] = struct{}{}
		if len(subset) > *subsetSize {
			break
		}
	}
	elap = time.Now().Sub(start)
	log.Println("Direct search completed in", elap)

	//////////
	// stage 2: calculate jaccard similarity using bloom bits
	start = time.Now()
	for key, data := range dataset {
		if _, ok := subset[key]; !ok {
			continue
		}
		//best, scores, comps := bs.BestMatches(data, 10)
		//fmt.Println("BLOOM", key, comps, best, scores)
		_, _, comps := bs.BestMatches(data, 10)
		fmt.Println("BLOOM", key, comps)
	}
	elap = time.Now().Sub(start)
	log.Println("Bloom search completed in", elap)

	//////////
	// stage 3: calculate jaccard similarity using folded bloom bits
	start = time.Now()
	bs.FoldAll(uint64(*foldSize))
	elap = time.Now().Sub(start)
	log.Println("Folding took", elap)

	start = time.Now()
	for key, data := range dataset {
		if _, ok := subset[key]; !ok {
			continue
		}
		//best, scores, comps := bs.FoldedBestMatches(data, 10)
		//fmt.Println("FOLDED", key, comps, best, scores)
		_, _, comps := bs.FoldedBestMatches(data, 10)
		fmt.Println("FOLDED", key, comps)
	}
	elap = time.Now().Sub(start)
	log.Println("Folded search completed in", elap)
}
