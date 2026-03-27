package org.opencadc.tap.query.pg;

import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the PostgreSQL ADQL query implementation
 */
public class PostgresAdqlQueryTest {
    private static final Logger log = Logger.getLogger(PostgresAdqlQueryTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.tap", Level.INFO);
    }

    private final Job job = new Job() {
        @Override
        public String getID() {
            return "testJob";
        }
    };

    @Test
    public void testOrigConverters() {
        try {
            job.getParameterList().add(new Parameter("QUERY", "select * from test.foo as t"));

            PostgresAdqlQuery q = new PostgresAdqlQuery();
            q.setJob(job);
            q.setTapSchema(mockTapSchema());

            String sql = q.getSQL().toLowerCase();
            log.debug("SQL: " + sql);
            String selectList = sql.substring(sql.indexOf("select") + 6, sql.indexOf("from") - 1);
            Assert.assertTrue("f1", selectList.contains("t.f1"));
            Assert.assertTrue("f2", selectList.contains("t.f2"));
            Assert.assertFalse("no star", selectList.contains("*"));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            job.getParameterList().clear();
        }
    }

    @Test
    public void testTopConverter() {
        try {
            job.getParameterList().add(new Parameter("QUERY", "select top 5 * from test.foo"));

            PostgresAdqlQuery q = new PostgresAdqlQuery();
            q.setJob(job);
            q.setTapSchema(mockTapSchema());

            String sql = q.getSQL();
            log.debug("SQL: " + sql);
            Assert.assertTrue("limit", sql.toLowerCase().endsWith("limit 5"));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            job.getParameterList().clear();
        }
    }

    private TapSchema mockTapSchema() {
        TapSchema ret = new TapSchema();
        SchemaDesc sd = new SchemaDesc("test");
        TableDesc foo = new TableDesc("test", "test.foo");
        foo.getColumnDescs().add(new ColumnDesc("test.foo", "f1", TapDataType.INTEGER));
        foo.getColumnDescs().add(new ColumnDesc("test.foo", "f2", new TapDataType("char", "8", null)));
        sd.getTableDescs().add(foo);
        ret.getSchemaDescs().add(sd);
        return ret;
    }
}
