package org.opencadc.tap.config;

/**
 * Central access point for the deployment configuration of the TAP service.
 *
 * <p>All configuration is supplied by the deployment (the phalanx
 * {@code cadc-tap} chart) as JVM system properties ({@code -D...}) or, for a few
 * values, environment variables.
 * 
 */
public final class TapConfig {

    private TapConfig() {
    }

    /** Base URL of the environment, e.g. https://data.lsst.cloud. */
    public static String baseUrl() {
        return property("base_url", null);
    }

    /** Ingress path prefix for this service, e.g. /api/tap. */
    public static String pathPrefix() {
        return property("path_prefix", null);
    }

    /** Name of the bucket async results are written to. */
    public static String gcsBucket() {
        return envOrProperty("GCS_BUCKET", "gcs_bucket", null);
    }

    /** Base URL for objects in the result bucket. */
    public static String gcsBucketUrl() {
        return envOrProperty("GCS_BUCKET_URL", "gcs_bucket_url", null);
    }

    /** Result bucket type: {@code GCS} or {@code S3}. */
    public static String gcsBucketType() {
        return property("gcs_bucket_type", null);
    }

    /** Whether the service advertises itself as available (VOSI availability). */
    public static boolean serviceAvailable() {
        return Boolean.parseBoolean(property("tap.service.available", "true"));
    }

    public static final long DEFAULT_MAX_EXEC_DURATION = 4 * 3600L;
    public static final long DEFAULT_MAX_DESTRUCTION = 7 * 24 * 3600L; // 1 week
    public static final long DEFAULT_MAX_QUOTE = 24 * 3600L;           // 24 hours
    public static final int DEFAULT_MAX_REC = 100_000_000;

    public static long maxExecutionDurationSeconds() {
        return longProperty("tap.maxExecutionDuration", DEFAULT_MAX_EXEC_DURATION);
    }

    public static long maxDestructionSeconds() {
        return longProperty("tap.maxDestruction", DEFAULT_MAX_DESTRUCTION);
    }

    public static long maxQuoteSeconds() {
        return longProperty("tap.maxQuote", DEFAULT_MAX_QUOTE);
    }

    public static int maxRec() {
        return (int) longProperty("tap.maxRec", DEFAULT_MAX_REC);
    }

    public static final String DEFAULT_OUTPUT_LIMIT = "100000000";
    public static final String DEFAULT_OUTPUT_LIMIT_UNIT = "row";

    public static String outputLimit() {
        return property("tap.outputLimit", DEFAULT_OUTPUT_LIMIT);
    }

    public static String outputLimitUnit() {
        return property("tap.outputLimitUnit", DEFAULT_OUTPUT_LIMIT_UNIT);
    }

    public static boolean voParquetEnabled() {
        return Boolean.parseBoolean(property("tap.enableVOParquet", "false"));
    }

    public static final String DEFAULT_URL_REWRITE_RULES = "ivoa.ObsCore:access_url";

    public static boolean urlRewriteEnabled() {
        return Boolean.parseBoolean(property("url.rewrite.enabled", "true"));
    }

    public static String urlRewriteRules() {
        return property("url.rewrite.rules", DEFAULT_URL_REWRITE_RULES);
    }

    public static String bigQueryProject() {
        return property("tap.bigquery.project", null);
    }

    public static String bigQueryDataset() {
        return property("tap.bigquery.dataset", null);
    }

    public static String bigQuerySchema() {
        return property("tap.bigquery.schema", "ppdb");
    }

    /** visible:backend table-name mappings, comma separated (may be empty). */
    public static String tableMappings() {
        return property("tap.table.mappings", "");
    }

    /** Qserv director-table config for upload partition detection (may be empty). */
    public static String uploadPartitionDirectors() {
        return property("upload.partition.directors", "");
    }

    public static final int DEFAULT_SYNC_POLL_MAX_ATTEMPTS = 20;
    public static final int DEFAULT_SYNC_POLL_INTERVAL_MS = 3000;

    public static int syncPollingMaxAttempts() {
        return (int) longProperty("tap.sync.polling.maxAttempts", DEFAULT_SYNC_POLL_MAX_ATTEMPTS);
    }

    public static int syncPollingIntervalMs() {
        return (int) longProperty("tap.sync.polling.intervalMs", DEFAULT_SYNC_POLL_INTERVAL_MS);
    }

    public static String database() {
        return envOrProperty("DATABASE", "database", null);
    }

    // helpers

    private static String property(String key, String defaultValue) {
        String v = System.getProperty(key);
        return (v != null && !v.isEmpty()) ? v : defaultValue;
    }

    private static String envOrProperty(String envKey, String propKey, String defaultValue) {
        String v = System.getenv(envKey);
        if (v == null || v.isEmpty()) {
            v = System.getProperty(propKey);
        }
        return (v != null && !v.isEmpty()) ? v : defaultValue;
    }

    private static long longProperty(String key, long defaultValue) {
        String v = System.getProperty(key);
        if (v == null || v.isEmpty()) {
            return defaultValue;
        }
        return Long.parseLong(v.trim());
    }
}
