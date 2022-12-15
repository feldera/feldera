SOURCES=appendix.tex example1.tex example3.tex implementation.tex	\
	main.tex recursion.tex relational.tex conclusions.tex		\
	example2.tex extensions.tex intro2.tex nested.tex related.tex	\
	streams.tex

main.pdf: $(SOURCES) main.bib
	pdflatex main
	bibtex main
	pdflatex main
