package it.generic_service_adapter.config.properties;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Transformation-layer constants that must not be hardcoded in the mappers (RF-07, contratti.md
 * §2). Fields only, no logic; bound from {@code gsa.mapping.*}.
 *
 * @param userAccountSource literal value written to {@code UserAccount.source}: a constant
 *     identifying the adapter plus the source topic ({@code
 *     generic-service-adapter/user-account-data}). Environment-agnostic; kept as one explicit key
 *     rather than assembled from a prefix + the source topic name so it matches contratti.md §2
 *     verbatim.
 */
@ConfigurationProperties(prefix = "gsa.mapping")
@Validated
public record MappingProperties(@NotBlank String userAccountSource) {}
