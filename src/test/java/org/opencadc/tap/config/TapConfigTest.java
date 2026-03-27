package org.opencadc.tap.config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link TapConfig} defaulting and parsing. Each test sets and
 * clears the system properties it exercises.
 */
public class TapConfigTest {

    @After
    public void clearProps() {
        for (String k : new String[] {
                "base_url", "gcs_bucket", "gcs_bucket_type",
                "tap.service.available", "tap.maxExecutionDuration", "tap.maxRec",
                "tap.outputLimit", "tap.enableVOParquet", "url.rewrite.enabled",
                "tap.bigquery.schema", "tap.table.mappings", "tap.sync.polling.maxAttempts"}) {
            System.clearProperty(k);
        }
    }

    @Test
    public void defaultsWhenUnset() {
        Assert.assertNull(TapConfig.baseUrl());
        Assert.assertNull(TapConfig.gcsBucket());
        Assert.assertTrue(TapConfig.serviceAvailable());
        Assert.assertEquals(TapConfig.DEFAULT_MAX_EXEC_DURATION, TapConfig.maxExecutionDurationSeconds());
        Assert.assertEquals(TapConfig.DEFAULT_MAX_DESTRUCTION, TapConfig.maxDestructionSeconds());
        Assert.assertEquals(TapConfig.DEFAULT_MAX_QUOTE, TapConfig.maxQuoteSeconds());
        Assert.assertEquals(TapConfig.DEFAULT_MAX_REC, TapConfig.maxRec());
        Assert.assertEquals("100000000", TapConfig.outputLimit());
        Assert.assertEquals("row", TapConfig.outputLimitUnit());
        Assert.assertFalse(TapConfig.voParquetEnabled());
        Assert.assertTrue(TapConfig.urlRewriteEnabled());
        Assert.assertEquals("ivoa.ObsCore:access_url", TapConfig.urlRewriteRules());
        Assert.assertEquals("ppdb", TapConfig.bigQuerySchema());
        Assert.assertEquals("", TapConfig.tableMappings());
        Assert.assertEquals(20, TapConfig.syncPollingMaxAttempts());
    }

    @Test
    public void readsOverrides() {
        System.setProperty("base_url", "https://example.org");
        System.setProperty("gcs_bucket", "my-bucket");
        System.setProperty("tap.service.available", "false");
        System.setProperty("tap.maxExecutionDuration", "60");
        System.setProperty("tap.maxRec", "42");
        System.setProperty("tap.enableVOParquet", "true");
        System.setProperty("url.rewrite.enabled", "false");
        System.setProperty("tap.sync.polling.maxAttempts", "7");

        Assert.assertEquals("https://example.org", TapConfig.baseUrl());
        Assert.assertEquals("my-bucket", TapConfig.gcsBucket());
        Assert.assertFalse(TapConfig.serviceAvailable());
        Assert.assertEquals(60L, TapConfig.maxExecutionDurationSeconds());
        Assert.assertEquals(42, TapConfig.maxRec());
        Assert.assertTrue(TapConfig.voParquetEnabled());
        Assert.assertFalse(TapConfig.urlRewriteEnabled());
        Assert.assertEquals(7, TapConfig.syncPollingMaxAttempts());
    }
}
