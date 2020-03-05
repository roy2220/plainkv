override svgfiles := $(patsubst %.dot,%.svg,$(wildcard docs/*.dot))

%.svg: %.dot
	dot -Tsvg -o $@ $<
