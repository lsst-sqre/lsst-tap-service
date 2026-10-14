package org.opencadc.tap.upload;

import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.tap.upload.UploadTable;
import java.net.URI;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the BigQuery upload target.
 */
public class BigQueryUploadTargetTest {

    private final BigQueryUploadTarget target = new BigQueryUploadTarget("upload-project", "tap_uploads");

    private static VOTableField field(String datatype, String arraysize) {
        return new VOTableField("col", datatype, arraysize);
    }

    @Test
    public void testDatabaseTableName() {
        UploadTable upload = new UploadTable("mytable", "abc-123", URI.create("http://example.com/t.xml"));

        // No authenticated caller in the test, so the name uses the anonymous user.
        Assert.assertEquals("`upload-project.tap_uploads.user_anonymous_abc_123_mytable`",
                target.getDatabaseTableName(upload));
    }

    @Test
    public void testValidatedTableName() {
        Assert.assertEquals("user_me_abc_123_mytable",
                target.getValidatedTableName("`upload-project.tap_uploads.user_me_abc_123_mytable`"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNotConfigured() {
        UploadTable upload = new UploadTable("mytable", "abc123", URI.create("http://example.com/t.xml"));
        new BigQueryUploadTarget(null, "tap_uploads").getDatabaseTableName(upload);
    }

    @Test
    public void testColumnTypes() {
        Assert.assertEquals("FLOAT64", target.getColumnType(field("double", null)));
        Assert.assertEquals("FLOAT64", target.getColumnType(field("float", null)));
        Assert.assertEquals("INT64", target.getColumnType(field("long", null)));
        Assert.assertEquals("INT64", target.getColumnType(field("int", null)));
        Assert.assertEquals("INT64", target.getColumnType(field("short", null)));
        Assert.assertEquals("INT64", target.getColumnType(field("unsignedByte", null)));
        Assert.assertEquals("BOOL", target.getColumnType(field("boolean", null)));
        Assert.assertEquals("STRING", target.getColumnType(field("char", "*")));
        Assert.assertEquals("STRING", target.getColumnType(field("unicodeChar", "16")));
        Assert.assertEquals("STRING", target.getColumnType(field("doubleComplex", null)));

        // Arrays, including geometry xtypes, are stored as strings
        Assert.assertEquals("STRING", target.getColumnType(field("double", "2")));
        Assert.assertEquals("STRING", target.getColumnType(field("int", "*")));

        VOTableField timestamp = field("char", "*");
        timestamp.xtype = "timestamp";
        Assert.assertEquals("TIMESTAMP", target.getColumnType(timestamp));
    }
}
