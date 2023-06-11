SOURCES=circuits.tex extensions.tex intro.tex matrix.tex \
        related.tex streams.tex ddlog.tex implementation.tex \
        main.tex recursive.tex relational.tex ztransform.tex \
	inc-relational.tex nested.tex incremental.tex inc-recursive.tex

main.pdf: $(SOURCES) main.bib
	pdflatex main
	bibtex main
	pdflatex main
