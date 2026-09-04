# Configuration

Two Spring profiles: `dev` and `prod` (RNF-09). Base keys in `application.yml`,
per-environment keys in `application-dev.yml` / `application-prod.yml`. Every
field of every `@ConfigurationProperties` class under
`it.generic_service_adapter.config.properties` has a corresponding key in one
of these files — no hardcoded value in the Java code (RF-15, RNF-09).

## `dev`

Local, plaintext, points at the services declared in
[`compose.yaml`](../../../../compose.yaml). Credentials are throwaway values
matching the compose file, safe to commit.

## `prod`

Every endpoint and credential is an environment variable. Nothing sensitive is
committed (RNF-05). The app MUST fail fast at startup if a required variable is
missing, not silently fall back to a dev value.

| Environment variable | Used for |
|---|---|
| `GSA_DB_URL` | MySQL 8.0 JDBC URL (`spring.datasource.url`, `spring.flyway.url`) |
| `GSA_DB_USERNAME` | MySQL user |
| `GSA_DB_PASSWORD` | MySQL password |
| `GSA_KAFKA_SOURCE_BOOTSTRAP_SERVERS` | Source cluster bootstrap servers |
| `GSA_KAFKA_SOURCE_USERNAME` | Source cluster SASL/SCRAM-SHA-512 username |
| `GSA_KAFKA_SOURCE_PASSWORD` | Source cluster SASL/SCRAM-SHA-512 password |
| `GSA_KAFKA_DESTINATION_BOOTSTRAP_SERVERS` | Destination cluster bootstrap servers |
| `GSA_KAFKA_DESTINATION_USERNAME` | Destination cluster SASL/SCRAM-SHA-512 username |
| `GSA_KAFKA_DESTINATION_PASSWORD` | Destination cluster SASL/SCRAM-SHA-512 password |
| `GSA_SCHEMA_REGISTRY_URL` | Confluent Schema Registry endpoint |
| `GSA_SCHEMA_REGISTRY_CREDENTIALS` | `username:password` for `basic.auth.credentials.source` |
| `GSA_VAULT_ENDPOINT` | Vault `POST` endpoint for the XML case report |
| `GSA_REPORT_SPOOL_DIR` | Mount point of the XML spool PVC |

A prod truststore for `SASL_SSL` is also required (RNF-16) but is not a single
environment variable: it is provisioned by `devops` as part of the deployment
(non-versioned).
