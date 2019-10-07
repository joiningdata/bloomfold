// Command obo loads an OBO file and exports a flattened, tab-delimited closure dataset.
// Each line represents the complete closure of the 1st term on the line.
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
	flag.Parse()

	f, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}

	tree := make(map[string]map[string][]string)
	term := make(map[string][]string)
	term["_"] = []string{"header"}

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if line == "" {
			continue
		}
		if line[:1] == "[" {
			if len(term) > 1 && term["_"][0] == "[Term]" {
				tree[term["id"][0]] = term
			}
			term = make(map[string][]string)
			term["_"] = []string{line}
			continue
		}
		kv := strings.SplitN(line, ": ", 2)
		if len(kv) != 2 {
			continue
		}

		if idx := strings.Index(kv[1], " ! "); idx > 0 {
			kv[1] = kv[1][:idx]
		}
		term[kv[0]] = append(term[kv[0]], kv[1])
	}
	f.Close()
	if len(term) > 0 {
		tree[term["id"][0]] = term
	}

	closure := make(map[string]map[string]struct{})
	//////////
	for _, x := range tree {
		if _, ok := x["is_obsolete"]; ok {
			continue
		}
		me := x["id"][0]
		closure[me] = make(map[string]struct{})
		closure[me][me] = struct{}{}

		for _, y := range x["is_a"] {
			closure[me][y] = struct{}{}
		}
		//for _, y := range x["part_of"] {
		//	closure[me][y] = struct{}{}
		//}
	}

	nchanges := 1
	for nchanges > 0 {
		nchanges = 0
		for x, ancs := range closure {
			for z := range ancs {
				if _, ok := closure[z][x]; !ok {
					closure[z][x] = struct{}{}
					nchanges++
				}
			}
		}
	}

	clo := make([]string, 0, 1000)
	for x, ancs := range closure {
		clo = clo[:0]
		for y := range ancs {
			if x != y {
				clo = append(clo, y)
			}
		}
		fmt.Printf("%s\t%s\n", x, strings.Join(clo, "\t"))
	}
}
