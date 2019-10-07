package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	CompThreshold   = 0.95
	SizeGroupSplits = 3
	SizeGroups      = 50
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

func (s *BloomSet) FoldedBestMatches(data []string, topn int) int {
	query := &BloomFilter{}
	query.Advise(s.sz)
	query.ErrorRate(0.01)
	for _, x := range data {
		query.Learn(x)
	}
	foldedQuery := FoldFilterWithSize(query, s.foldbits)

	comps := 0
	for _, other := range s.folds {
		sim := other.Similarity(foldedQuery)
		if sim >= CompThreshold {
			comps++
		}
	}
	return comps
}

func (s *BloomSet) BestMatches(data []string, topn int) int {
	query := &BloomFilter{}
	query.Advise(s.sz)
	query.ErrorRate(0.01)
	for _, x := range data {
		query.Learn(x)
	}

	comps := 0
	for _, other := range s.bf {
		sim := other.Similarity(query)
		if sim >= CompThreshold {
			comps++
		}
	}
	return comps
}

func BestMatches(dataset map[string][]string, data []string, topn int) int {

	query := make(map[string]struct{})
	for _, x := range data {
		query[x] = struct{}{}
	}

	comps := 0
	for _, other := range dataset {
		numer, denom := 0.0, float64(len(query))
		for _, x := range other {
			if _, ok := query[x]; ok {
				numer++
			} else {
				denom++
			}
		}

		sim := numer / denom
		if denom > 0 && sim >= CompThreshold {
			comps++
		}
	}
	return comps
}

func parallelCollect(eachFunc func(string, []string) int, dataset map[string][]string) (totalTime, totalCount []float64) {
	type rec struct {
		key  string
		data []string

		result int
		elap   float64
	}
	inchan := make(chan rec)
	outchan := make(chan rec)
	go func(nc int) {
		wg := sync.WaitGroup{}
		for r := range inchan {
			wg.Add(1)
			go func(rx rec) {
				ostart := time.Now()
				rx.result = eachFunc(rx.key, rx.data)
				oelap := time.Now().Sub(ostart)
				rx.elap = oelap.Seconds()
				wg.Done()
				outchan <- rx
			}(r)
		}
		wg.Wait()
		close(outchan)
	}(runtime.NumCPU())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		totalTime = make([]float64, SizeGroupSplits+1)
		totalCount = make([]float64, SizeGroupSplits+1)
		for r := range outchan {
			totalCount[0] += float64(r.result)
			totalTime[0] += r.elap

			g := 1 + (len(r.data) / SizeGroups)
			if g > SizeGroupSplits {
				g = SizeGroupSplits
			}
			totalCount[g] += float64(r.result)
			totalTime[g] += r.elap
		}
		wg.Done()
	}()

	for key, data := range dataset {
		inchan <- rec{key: key, data: data}
	}
	close(inchan)

	wg.Wait()
	//results[key][2] = oelap.Seconds()
	//results[key][5] = float64(comps)
	return
}

func main() {
	//subsetSize := flag.Int("s", 500, "number of subsets to test")
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

	////////////////
	// stage 1: calculate true jaccard similarity
	/*
		start = time.Now()
		totalFull, countFull := parallelCollect(func(key string, data []string) int {
			return BestMatches(dataset, data, 10)
		}, dataset)
		elap = time.Now().Sub(start)
		log.Println("Direct search completed in", elap)
	*/

	//////////
	// stage 2: calculate jaccard similarity using bloom bits
	start = time.Now()
	totalBloom, countBloom := parallelCollect(func(key string, data []string) int {
		return bs.BestMatches(data, 10)
	}, dataset)
	elap = time.Now().Sub(start)
	log.Println("Bloom search completed in", elap)

	//////////
	// stage 3: calculate jaccard similarity using folded bloom bits
	start = time.Now()
	bs.FoldAll(uint64(*foldSize))
	elap = time.Now().Sub(start)
	log.Println("Folding took", elap)

	start = time.Now()
	totalFolded, countFolded := parallelCollect(func(key string, data []string) int {
		return bs.FoldedBestMatches(data, 10)
	}, dataset)
	elap = time.Now().Sub(start)
	log.Println("Folded search completed in", elap)

	/*
		groups := []string{"0-24", "25-49", "50-74", "75+", "all"}
		groupstats := make([]float64, 7*5)
		for _, stats := range results {
			grp := int(stats[0] / 25)
			if grp > 3 {
				grp = 3
			}

			for i, v := range stats {
				if i == 0 {
					groupstats[4*7]++
					groupstats[grp*7+i]++
					continue
				}
				groupstats[4*7+i] += v
				groupstats[grp*7+i] += v
			}
		}

		for g := 0; g < 5; g++ {
			speedup := groupstats[g*7+2] / groupstats[g*7+3]
			errdiff := groupstats[g*7+5] / groupstats[g*7+6]
			fmt.Println(groups[g], groupstats[g*7], speedup, errdiff)
		}
	*/

	lastSz := 1
	for i := 0; i <= SizeGroupSplits; i++ {
		curSz := (i * SizeGroups) - 1
		gname := "all"
		if i > 0 {
			if i == SizeGroupSplits {
				gname = fmt.Sprintf(">=%d", lastSz)
			} else {
				gname = fmt.Sprintf("%d-%d", lastSz, curSz)
			}
			lastSz = curSz + 1
		}

		speedup := totalFolded[i] / totalBloom[i]
		errdiff := countFolded[i] / countBloom[i]
		fmt.Println(gname, speedup, errdiff)
	}
}
