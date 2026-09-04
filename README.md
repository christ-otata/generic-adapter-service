# generic-service-adapter

A **Kafka → Kafka** adapter between two distinct clusters: it consumes **JSON**
events (user profile, wallet top-ups and withdrawals) from three topics of the
source cluster, applies **in-process** normalization and enrichment, re-serializes
to **Protobuf** and publishes to the destination cluster, registering the schema
with a **Confluent Schema Registry**. Messages that stay in error become **cases**
in MySQL and are rolled up into **XML reports** sent periodically to an HTTP
**Vault**.

> Demonstrative portfolio project. No real counterparts: the data schemas and the
> `.proto` contract are owned by this repository; the decisions are assumptions
> confirmed with the stakeholder, treated as firm requirements.

- **Stack**: Java 21 · Spring Boot 4.1.1 · Spring Kafka · Spring Data JDBC ·
  Flyway · MySQL 8.0 · Protobuf + Schema Registry · Maven
- **Environments**: `dev` (docker-compose, PLAINTEXT) and `prod` (Kubernetes +
  Kustomize, SASL_SSL)

## Documentation

All in Markdown under [`docs/`](docs/), with diagrams as inline Mermaid:

| Folder | Content |
|---|---|
| `docs/analisi/` | Functional analysis — `RF-*` / `RNF-*` requirements, flows, error taxonomy, glossary. Status: **AGREED**. |
| `docs/architettura/` | Technical design: overview, components, Kafka topology, data model, contracts, flows, NFR, dependencies. |
| `docs/architettura/adr/` | Architecture Decision Records. |

### Export to PDF / Word

Via **Pandoc + Eisvogel** (in Docker, no local install):

```bash
make pdf        # dist/analisi-funzionale.pdf   (functional analysis only)
make docx       # dist/analisi-funzionale.docx
make doc        # both of the above
make doc-full   # dist/documentazione.{pdf,docx} — analysis + architecture + ADRs
```

`make help` lists the targets. On Apple Silicon the `pandoc/extra` image (amd64)
runs under emulation: Rosetta must be enabled in Docker Desktop.

## Layout

```
docs/
  analisi/         functional analysis (status AGREED)
  architettura/
    *.md           overview, components, Kafka topology, data model,
                   contracts, flows, NFR, dependencies
    adr/           Architecture Decision Records
pandoc/            metadata + preamble + assembler for the PDF/Word export
src/               Spring Boot application
compose.yaml       development stack
```
