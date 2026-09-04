#!/usr/bin/env python3
"""Assembla analisi + architettura + ADR in un unico Markdown, con i diagrammi
Mermaid pre-renderizzati a PNG, pronto per Pandoc.

Uso:  python3 pandoc/assemble.py [build/combined.md]
Richiede: docker (immagine mermaid-cli).
"""
import re
import shutil
import subprocess
import sys
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
BUILD = ROOT / "build"
ASSETS = BUILD / "assets"
MMDC = "ghcr.io/mermaid-js/mermaid-cli/mermaid-cli:11.4.0"

# Ordine di lettura del documento unico.
ORDER = [
    "docs/analisi/ingestione-anagrafica-e-movimenti-wallet.md",
    "docs/architettura/panoramica.md",
    "docs/architettura/componenti.md",
    "docs/architettura/topologia-kafka.md",
    "docs/architettura/modello-dati.md",
    "docs/architettura/contratti.md",
    "docs/architettura/flussi.md",
    "docs/architettura/nfr.md",
    "docs/architettura/dipendenze.md",
    "docs/architettura/adr/index.md",
]

MD_LINK = re.compile(r"\[([^\]]+)\]\((?:\.{1,2}/)*[\w./-]+\.md(?:#[\w-]+)?\)")


def adr_files():
    """ADR 'vivi' in ordine numerico (esclude gli stub RINUMERATO/SPOSTATO)."""
    out = []
    for p in sorted((ROOT / "docs/architettura/adr").glob("[0-9][0-9][0-9][0-9]-*.md")):
        head = p.read_text(encoding="utf-8").splitlines()[0]
        if "RINUMERATO" in head or "SPOSTATO" in head:
            continue
        out.append(p.relative_to(ROOT).as_posix())
    return out


def render(rel, tag):
    """Ritorna il Markdown di `rel` con i blocchi mermaid sostituiti da immagini."""
    src = ROOT / rel
    text = src.read_text(encoding="utf-8")
    if "```mermaid" not in text:
        return text
    tmp = BUILD / f"_{tag}.in.md"
    tmp.write_text(text, encoding="utf-8")
    subprocess.run(
        ["docker", "run", "--rm", "-v", f"{BUILD}:/data", MMDC,
         "-i", f"/data/{tmp.name}", "-o", f"/data/assets/{tag}.md",
         "-e", "png", "-b", "white", "--scale", "4", "-w", "1600"],
        check=True,
    )
    out = (ASSETS / f"{tag}.md").read_text(encoding="utf-8")
    # path relativo alla combined.md; via l'alt "diagram" generato da mmdc
    return out.replace("](./", "](assets/").replace("![diagram](", "![](")


def main():
    combined = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else BUILD / "combined.md"
    if BUILD.exists():
        shutil.rmtree(BUILD)
    ASSETS.mkdir(parents=True)

    parts = []
    for i, rel in enumerate(ORDER + adr_files()):
        tag = re.sub(r"[^\w-]", "_", rel[len("docs/"):-len(".md")])
        md = MD_LINK.sub(r"\1", render(rel, tag))
        if i:
            parts.append("\n\n\\newpage\n\n")
        parts.append(md.rstrip() + "\n")

    combined.parent.mkdir(parents=True, exist_ok=True)
    combined.write_text("".join(parts), encoding="utf-8")
    print(combined)


if __name__ == "__main__":
    main()
