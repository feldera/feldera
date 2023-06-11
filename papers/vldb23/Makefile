SOURCES=example1.tex implementation.tex	\
	main.tex recursion.tex relational.tex conclusions.tex		\
	extensions.tex intro2.tex nested.tex related.tex	\
	streams.tex

TARGET = main.pdf # p955-budiu.pdf

$(TARGET): $(SOURCES) main.bib
	pdflatex main
	bibtex main
	pdflatex main
	mv main.pdf $@
