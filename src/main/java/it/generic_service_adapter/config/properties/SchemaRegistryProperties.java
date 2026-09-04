package it.generic_service_adapter.config.properties;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Confluent Schema Registry towards the destination cluster (ADR 0012, 0015). Fields only, no
 * logic; bound from {@code gsa.schema-registry.*}.
 *
 * @param url registry REST endpoint; per-environment parameter
 * @param basicAuthUserInfo {@code username:password} for {@code basic.auth.credentials.source};
 *     blank in dev, external secret in prod (RNF-05)
 * @param autoRegisterSchemas {@code auto.register.schemas}: {@code true} in dev, {@code false} in
 *     prod (registered by a dedicated pipeline)
 * @param useLatestVersion {@code use.latest.version}, expected {@code false}: the serializer uses
 *     the message schema
 */
@ConfigurationProperties(prefix = "gsa.schema-registry")
@Validated
public record SchemaRegistryProperties(
    @NotBlank String url,
    String basicAuthUserInfo,
    boolean autoRegisterSchemas,
    boolean useLatestVersion) {}
