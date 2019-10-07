// Command mkset takes paired associations and a closure file, and exports a new
// collection of data with closures applied and aggregated in both directions.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	ofn1 := flag.String("olr", "", "output L-set(R) associations to `filename`")
	ofn2 := flag.String("orl", "", "output R-set(L) associations to `filename`")
	flag.Parse()

	closures := make(map[string][]string)
	f1, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	s := bufio.NewScanner(f1)
	for s.Scan() {
		row := strings.Split(s.Text(), "\t")
		closures[row[0]] = row
	}
	f1.Close()

	// gene -> terms
	associations := make(map[string]map[string]struct{})
	f2, err := os.Open(flag.Arg(1))
	if err != nil {
		log.Fatal(err)
	}
	s = bufio.NewScanner(f2)
	for s.Scan() {
		row := strings.Split(s.Text(), "\t")
		if len(row) != 2 {
			continue
		}
		if _, ok := associations[row[0]]; !ok {
			associations[row[0]] = make(map[string]struct{})
		}
		for _, x := range closures[row[1]] {
			associations[row[0]][x] = struct{}{}
		}
	}
	f2.Close()

	outf, err := os.Create(*ofn1)
	if err != nil {
		log.Println(err)
		outf = nil
	}

	// term -> genes
	rassocs := make(map[string][]string)
	tmp := make([]string, 0, 100)
	for x, assoc := range associations {
		tmp = tmp[:0]
		for y := range assoc {
			rassocs[y] = append(rassocs[y], x)
			tmp = append(tmp, y)
		}
		if outf != nil {
			fmt.Fprint(outf, x, "\t", strings.Join(tmp, "\t"), "\n")
		}
	}
	if outf != nil {
		outf.Close()
	}

	outf, err = os.Create(*ofn2)
	if err != nil {
		log.Println(err)
		outf = nil
	}

	max := 0
	histo := make(map[int]int)
	for x, y := range rassocs {
		if outf != nil {
			fmt.Fprint(outf, x, "\t", strings.Join(y, "\t"), "\n")
		}
		histo[len(y)]++
		if len(y) > max {
			max = len(y)
		}
	}
	if outf != nil {
		outf.Close()
	}

	for i := 0; i < max; i++ {
		if n, ok := histo[i]; ok {
			fmt.Fprintf(os.Stderr, "%5d: %d\n", i, n)
		}
	}
}
