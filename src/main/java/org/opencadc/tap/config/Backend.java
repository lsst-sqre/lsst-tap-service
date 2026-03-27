package org.opencadc.tap.config;

import org.apache.log4j.Logger;

/**
 * The data backend this TAP service instance is configured for. Resolved from
 * the {@code BACKEND} environment variable, which must be set to one of
 * {@code pg}, {@code qserv} or {@code bigquery}.
 *
 * @author stvoutsin
 */
public enum Backend {
    PG,
    QSERV,
    BIGQUERY;

    private static final Logger log = Logger.getLogger(Backend.class);

    /** Name of the environment variable that selects the backend. */
    public static final String ENV_VAR = "BACKEND";

    /**
     * Resolve the configured backend from the {@code BACKEND}.
     *
     * @return the configured backend (required)
     * @throws IllegalStateException if {@code BACKEND} is unset, blank or not a
     *         recognised value
     */
    public static Backend current() {
        return fromString(System.getenv(ENV_VAR));
    }

    /**
     * Resolve the configured backend, falling back to a default when
     * {@code BACKEND} is unset or blank.
     *
     * @param fallback the value to use when {@code BACKEND} is unset or blank
     * @return the resolved backend (required)
     */
    public static Backend currentOrDefault(Backend fallback) {
        String value = System.getenv(ENV_VAR);
        if (value == null || value.trim().isEmpty()) {
            log.debug(ENV_VAR + " not set, using default: " + fallback);
            return fallback;
        }
        return fromString(value);
    }

    static Backend fromString(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException(ENV_VAR + " environment variable is required"
                    + " (expected one of: pg, qserv, bigquery)");
        }
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("unrecognised " + ENV_VAR + " value '" + value.trim()
                    + "' (expected one of: pg, qserv, bigquery)");
        }
    }

    /**
     * @return true if query execution for this backend is dispatched
     *         asynchronously via Kafka (QServ, BigQuery) else false
     */
    public boolean usesKafka() {
        return this != PG;
    }
}
