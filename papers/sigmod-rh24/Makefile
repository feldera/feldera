SOURCES=example1.tex implementation.tex	\
	main.tex recursion.tex relational.tex conclusions.tex		\
	extensions.tex intro.tex nested.tex related.tex	\
	streams.tex view.pdf incview.pdf example1.tex

TARGET = main.pdf

$(TARGET): $(SOURCES) main.bib
	pdflatex main
	bibtex main
	pdflatex main
