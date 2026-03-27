package org.opencadc.tap.config;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Unit tests for {@link Backend} value resolution.
 */
public class BackendTest {

    @Test
    public void fromStringRecognisedValues() {
        Assert.assertEquals(Backend.PG, Backend.fromString("pg"));
        Assert.assertEquals(Backend.QSERV, Backend.fromString("qserv"));
        Assert.assertEquals(Backend.BIGQUERY, Backend.fromString("bigquery"));
        // case / whitespace tolerant
        Assert.assertEquals(Backend.QSERV, Backend.fromString("  QServ "));
    }

    @Test
    public void fromStringRejectsMissing() {
        assertIllegalState(null);
        assertIllegalState("");
        assertIllegalState("   ");
    }

    @Test
    public void fromStringRejectsUnknown() {
        assertIllegalState("postgres");
        assertIllegalState("mysql");
    }

    @Test
    public void currentOrDefaultFallsBackWhenUnset() {
        Assume.assumeTrue("BACKEND env var must be unset for this test",
                System.getenv(Backend.ENV_VAR) == null);
        Assert.assertEquals(Backend.QSERV, Backend.currentOrDefault(Backend.QSERV));
        Assert.assertEquals(Backend.PG, Backend.currentOrDefault(Backend.PG));
    }

    @Test
    public void usesKafka() {
        Assert.assertFalse(Backend.PG.usesKafka());
        Assert.assertTrue(Backend.QSERV.usesKafka());
        Assert.assertTrue(Backend.BIGQUERY.usesKafka());
    }

    private void assertIllegalState(String value) {
        try {
            Backend.fromString(value);
            Assert.fail("expected IllegalStateException for '" + value + "'");
        } catch (IllegalStateException expected) {
            // ok
        }
    }
}
