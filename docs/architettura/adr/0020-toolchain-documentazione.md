# 0020. Documentation toolchain: Markdown + Mermaid, PDF/Word export via Pandoc

**Status:** Accepted 2026-09-04 (supersedes the previous decision "MkDocs Material + Structurizr")
**Trace:** —

> ADR about the documents toolchain, not about the service architecture.

## Context

The analysis and architecture documentation is part of a portfolio project. A
**portable and self-contained** delivery format is needed (a file that opens
anywhere, even offline, with no site to publish) and **versioned**, diff-readable
diagrams, not opaque images. The environment has Python 3.9 and Docker; it has
neither Node nor Homebrew.

A first iteration chose an MkDocs Material site with the C4 views modelled in
Structurizr DSL and exported to SVG. It turned out to be over-sized for the goal:
a site to maintain and publish, two diagram languages, committed SVGs to keep
aligned with the model.

## Decision

- **Source**: plain Markdown in `docs/`.
- **Delivery**: PDF and Word generated with **Pandoc** from the Markdown
  (`make pdf` / `make docx` / `make doc`), **Eisvogel** LaTeX template for the
  PDF.
- **All diagrams** (flow, state, entity, C4-like context and container views):
  **Mermaid** in ` ```mermaid ` blocks inside the `.md`, rendered at Pandoc
  export time via `mermaid-cli`.
- **No Structurizr**: no `workspace.dsl`, no PlantUML/SVG export, no
  `make c4-export` / `make c4`. The C4 views are drawn directly in Mermaid
  (`C4Context` / `C4Container`, or `flowchart` where Mermaid's C4 degrades).
- **No site**: no MkDocs, no GitHub Pages publishing.
- Mermaid syntax constraints for compatibility with Pandoc/`mermaid-cli`: in
  `sequenceDiagram` no `<br/>` in a message's text after `:` (allowed only in
  `participant ... as` and in `Note`); in `flowchart` use `<br/>`, not `\n`;
  labels with parentheses or special characters between quotes.

## Alternatives considered

- **MkDocs Material site + Structurizr** (previous decision). Navigation and
  search, but a site to publish and maintain, two diagram toolchains, committed
  SVGs to realign. Disproportionate for a portfolio delivery.
- **Markdown + Mermaid only, rendered by GitHub.** No self-contained downloadable
  artifact; the rendering depends on the platform.
- **AsciiDoc + Asciidoctor PDF.** Good PDF output, but the documents are already
  in Markdown and there is no reason to migrate the source.

## Consequences

- **+** Delivery in a portable file (PDF/Word), openable offline, suitable to be
  attached or shared.
- **+** A single diagram language (Mermaid), a single source format (Markdown).
- **+** No system dependency beyond Docker (Pandoc + `mermaid-cli` + LaTeX via
  images).
- **−** No navigable site with full-text search.
- **−** The C4 views stay Mermaid approximations (`C4Context`/`C4Container` or
  `flowchart`), without the single model from which to derive multiple consistent
  views.
- **−** The Mermaid diagrams must be written respecting the syntax constraints
  above, otherwise the PDF export fails.
