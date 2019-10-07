all: download obo sets

download:
	curl -LO http://current.geneontology.org/ontology/go.obo
	curl -LO http://current.geneontology.org/annotations/goa_human.gpad.gz
	gunzip goa_human.gpad.gz
	cut -f 2,4 goa_human.gpad |sort -u > simplified.txt
	cut -f 1 simplified.txt|uniq -c > terms_per_gene.txt
	sort -k2 simplified.txt |cut -f2 |uniq -c > genes_per_term.txt

obo:
	go build ./cmd/obo
	./obo go.obo >go_closure.txt

sets:
	go build ./cmd/mkset
	./mkset -olr gene2terms.txt -orl term2genes.txt go_closure.txt simplified.txt

