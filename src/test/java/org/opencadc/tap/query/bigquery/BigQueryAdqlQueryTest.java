package org.opencadc.tap.query.bigquery;

import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.FunctionDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for spatial queries on the BigQuery backend, including cross-matches
 * against uploaded tables.
 */
public class BigQueryAdqlQueryTest {

    private static final Logger log = Logger.getLogger(BigQueryAdqlQueryTest.class);

    private static final String UPLOAD_TABLE = "`upload-project.tap_uploads.user_me_job1_targets`";

    @BeforeClass
    public static void setUp() {
        System.setProperty("tap.bigquery.project", "ppdb-project");
        System.setProperty("tap.bigquery.dataset", "ppdb_public");
    }

    @AfterClass
    public static void tearDown() {
        System.clearProperty("tap.bigquery.project");
        System.clearProperty("tap.bigquery.dataset");
    }

    /**
     * Translate an ADQL query and normalise the SQL for comparison.
     */
    private String toSql(String adql) {
        Job job = new Job() {
            @Override
            public String getID() {
                return "job1";
            }
        };
        job.getParameterList().add(new Parameter("QUERY", adql));

        BigQueryAdqlQuery q = new BigQueryAdqlQuery();
        q.setJob(job);
        q.setTapSchema(mockTapSchema());
        Map<String, TableDesc> uploads = new HashMap<>();
        uploads.put(UPLOAD_TABLE, uploadTable());
        q.setExtraTables(uploads);

        String sql = q.getSQL();
        log.debug("SQL: " + sql);
        return sql.replaceAll("\\s", "").toLowerCase();
    }

    @Test
    public void testConeSearchUsesGeoPoint() {
        String sql = toSql("SELECT o.diaObjectId FROM ppdb.DiaObject AS o"
                + " WHERE CONTAINS(POINT('ICRS', o.ra, o.dec), CIRCLE('ICRS', 10.0, 20.0, 0.1)) = 1");
        Assert.assertTrue(sql, sql.contains("st_dwithin(st_geogpoint(10.0,20.0),o.geo_point,"));
        Assert.assertTrue(sql, sql.contains("`ppdb-project.ppdb_public.diaobject`"));
    }

    @Test
    public void testConeSearchWithoutGeoPoint() {
        // DiaSource is a view in the public dataset and has no geo_point
        String sql = toSql("SELECT s.diaSourceId FROM ppdb.DiaSource AS s"
                + " WHERE CONTAINS(POINT('ICRS', s.ra, s.dec), CIRCLE('ICRS', 10.0, 20.0, 0.1)) = 1");
        Assert.assertFalse(sql, sql.contains("geo_point"));
        Assert.assertTrue(sql, sql.contains("st_dwithin(st_geogpoint(10.0,20.0),st_geogpoint(s.ra,s.dec),"));
    }

    @Test
    public void testConeSearchUnqualifiedColumns() {
        String sql = toSql("SELECT diaObjectId FROM ppdb.DiaObject"
                + " WHERE CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', 10.0, 20.0, 0.1)) = 1");
        Assert.assertTrue(sql, sql.contains(",geo_point,"));
    }

    @Test
    public void testLiteralPointIsNotReplaced() {
        String sql = toSql("SELECT o.diaObjectId FROM ppdb.DiaObject AS o"
                + " WHERE CONTAINS(POINT('ICRS', 10.0, 20.0), CIRCLE('ICRS', o.ra, o.dec, 0.1)) = 1");
        Assert.assertFalse(sql, sql.contains("geo_point"));
        Assert.assertTrue(sql, sql.contains("st_dwithin(st_geogpoint(o.ra,o.dec),st_geogpoint(10.0,20.0),"));
    }

    @Test
    public void testCrossMatchPointOnPpdb() {
        String sql = toSql("SELECT o.diaObjectId, u.id FROM ppdb.DiaObject AS o"
                + " JOIN TAP_UPLOAD.targets AS u"
                + " ON CONTAINS(POINT('ICRS', o.ra, o.dec), CIRCLE('ICRS', u.ra, u.dec, 0.001)) = 1");
        Assert.assertTrue(sql, sql.contains("st_dwithin(st_geogpoint(u.ra,u.dec),o.geo_point,"));
        Assert.assertTrue(sql, sql.contains(UPLOAD_TABLE.toLowerCase()));
    }

    @Test
    public void testCrossMatchPointOnUpload() {
        // The upload table has no geo_point, so the point is built from its columns
        String sql = toSql("SELECT o.diaObjectId, u.id FROM ppdb.DiaObject AS o"
                + " JOIN TAP_UPLOAD.targets AS u"
                + " ON CONTAINS(POINT('ICRS', u.ra, u.dec), CIRCLE('ICRS', o.ra, o.dec, 0.001)) = 1");
        Assert.assertFalse(sql, sql.contains("u.geo_point"));
        Assert.assertTrue(sql, sql.contains("st_dwithin(st_geogpoint(o.ra,o.dec),st_geogpoint(u.ra,u.dec),"));
    }

    @Test
    public void testColumnRadius() {
        String sql = toSql("SELECT o.diaObjectId, u.id FROM ppdb.DiaObject AS o"
                + " JOIN TAP_UPLOAD.targets AS u"
                + " ON CONTAINS(POINT('ICRS', o.ra, o.dec), CIRCLE('ICRS', u.ra, u.dec, u.radius)) = 1");
        Assert.assertTrue(sql, sql.contains("st_dwithin(st_geogpoint(u.ra,u.dec),o.geo_point,(u.radius)*"));
    }

    @Test
    public void testGeoPointTablesDefault() {
        Assert.assertEquals(Collections.singletonList("ppdb.DiaObject"),
                BigQueryAdqlQuery.getGeoPointTables("ppdb"));
    }

    private static TableDesc uploadTable() {
        TableDesc td = new TableDesc("TAP_UPLOAD", "TAP_UPLOAD.targets");
        td.getColumnDescs().add(new ColumnDesc("TAP_UPLOAD.targets", "id", TapDataType.LONG));
        td.getColumnDescs().add(new ColumnDesc("TAP_UPLOAD.targets", "ra", TapDataType.DOUBLE));
        td.getColumnDescs().add(new ColumnDesc("TAP_UPLOAD.targets", "dec", TapDataType.DOUBLE));
        td.getColumnDescs().add(new ColumnDesc("TAP_UPLOAD.targets", "radius", TapDataType.DOUBLE));
        return td;
    }

    private static TableDesc ppdbTable(String name, String idColumn) {
        String tableName = "ppdb." + name;
        TableDesc td = new TableDesc("ppdb", tableName);
        td.getColumnDescs().add(new ColumnDesc(tableName, idColumn, TapDataType.LONG));
        td.getColumnDescs().add(new ColumnDesc(tableName, "ra", TapDataType.DOUBLE));
        td.getColumnDescs().add(new ColumnDesc(tableName, "dec", TapDataType.DOUBLE));
        return td;
    }

    private static TapSchema mockTapSchema() {
        TapSchema ret = new TapSchema();
        ret.getFunctionDescs().add(new FunctionDesc("CONTAINS", TapDataType.INTEGER));
        ret.getFunctionDescs().add(new FunctionDesc("POINT", TapDataType.POINT));
        ret.getFunctionDescs().add(new FunctionDesc("CIRCLE", TapDataType.CIRCLE));
        SchemaDesc ppdb = new SchemaDesc("ppdb");
        ppdb.getTableDescs().add(ppdbTable("DiaObject", "diaObjectId"));
        ppdb.getTableDescs().add(ppdbTable("DiaSource", "diaSourceId"));
        ret.getSchemaDescs().add(ppdb);

        // QueryRunner adds the uploaded tables to the TAP schema
        SchemaDesc upload = new SchemaDesc("TAP_UPLOAD");
        upload.getTableDescs().add(uploadTable());
        ret.getSchemaDescs().add(upload);
        return ret;
    }
}
