package org.opencadc.tap.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for database and table name construction.
 */
public class DatabaseNameUtilTest {

    private static final String JOB_ID = "abc123def456ghi7";
    private static final String LONG_USERNAME = "someone-with-a-long-name";

    @Test
    public void testShortNamesKept() {
        Assert.assertEquals("user_alice_" + JOB_ID + ".targets",
                DatabaseNameUtil.getDatabaseTableName("targets", "alice", JOB_ID));
    }

    @Test
    public void testLongUsernameHashed() {
        Assert.assertEquals("user_66b36542_" + JOB_ID,
                DatabaseNameUtil.constructDatabaseName(LONG_USERNAME, JOB_ID));
    }

    @Test
    public void testLongTableNameShortened() {
        String full = DatabaseNameUtil.getDatabaseTableName(
                "a_rather_long_upload_table_name", LONG_USERNAME, JOB_ID);

        Assert.assertEquals("user_66b36542_" + JOB_ID + ".a_rather_long_upl_0c741e3a", full);
        Assert.assertEquals(DatabaseNameUtil.MAX_QSERV_NAME_LENGTH, full.length() - 1);
    }

    @Test
    public void testShortenedTableNamesStayDistinct() {
        String db = DatabaseNameUtil.constructDatabaseName(LONG_USERNAME, JOB_ID);

        Assert.assertNotEquals(
                DatabaseNameUtil.shortenTableName(db, "a_rather_long_upload_table_name_1"),
                DatabaseNameUtil.shortenTableName(db, "a_rather_long_upload_table_name_2"));
    }
}
