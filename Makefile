# generic-service-adapter - export della documentazione in PDF / Word
# Richiede: docker. (pandoc/extra e' solo amd64: su Apple Silicon gira in
# emulazione, serve Rosetta attivo in Docker Desktop.)

PANDOC      := docker run --rm --platform linux/amd64 -v "$(CURDIR)":/data pandoc/extra:latest
PANDOC_META := pandoc/metadata.yaml
PANDOC_FULL := pandoc/metadata-full.yaml
PREAMBLE    := /data/pandoc/preamble.tex
ANALISI_MD  := docs/analisi/ingestione-anagrafica-e-movimenti-wallet.md
COMBINED_MD := build/combined.md

.DEFAULT_GOAL := help

.PHONY: help
help: ## Elenca i target disponibili
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

dist:
	mkdir -p dist

.PHONY: pdf
pdf: dist ## Analisi funzionale -> dist/analisi-funzionale.pdf (Eisvogel)
	$(PANDOC) $(ANALISI_MD) --metadata-file=$(PANDOC_META) --template eisvogel \
		--pdf-engine=xelatex --toc --toc-depth=3 --syntax-highlighting=tango \
		--include-in-header=$(PREAMBLE) -o dist/analisi-funzionale.pdf
	@echo "OK - dist/analisi-funzionale.pdf"

.PHONY: docx
docx: dist ## Analisi funzionale -> dist/analisi-funzionale.docx (Word)
	$(PANDOC) $(ANALISI_MD) --metadata-file=$(PANDOC_META) \
		--toc --toc-depth=3 --syntax-highlighting=tango \
		-o dist/analisi-funzionale.docx
	@echo "OK - dist/analisi-funzionale.docx"

.PHONY: doc
doc: pdf docx ## Genera sia il PDF sia il DOCX dell'analisi funzionale

# --- Documento unico: analisi + architettura + ADR ---

.PHONY: combined
combined: ## Assembla build/combined.md (Mermaid pre-renderizzato a PNG)
	python3 pandoc/assemble.py $(COMBINED_MD)

.PHONY: pdf-full
pdf-full: dist combined ## Documento tecnico completo -> dist/documentazione.pdf
	$(PANDOC) $(COMBINED_MD) --metadata-file=$(PANDOC_FULL) --template eisvogel \
		--pdf-engine=xelatex --toc --toc-depth=2 --syntax-highlighting=tango \
		--include-in-header=$(PREAMBLE) --resource-path=/data/build -o dist/documentazione.pdf
	@echo "OK - dist/documentazione.pdf"

.PHONY: docx-full
docx-full: dist combined ## Documento tecnico completo -> dist/documentazione.docx
	$(PANDOC) $(COMBINED_MD) --metadata-file=$(PANDOC_FULL) \
		--toc --toc-depth=2 --syntax-highlighting=tango \
		--resource-path=/data/build -o dist/documentazione.docx
	@echo "OK - dist/documentazione.docx"

.PHONY: doc-full
doc-full: pdf-full docx-full ## Genera PDF e DOCX del documento tecnico completo

.PHONY: clean
clean: ## Rimuove dist/ e build/
	rm -rf dist build
