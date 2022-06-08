package org.opencadc.tap.impl;

import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.FunctionDesc;
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
 *
 * @author cbanek
 */
public class QServAdqlTest
{
    private static final Logger log = Logger.getLogger(QServAdqlTest.class);

    static
    {
        Log4jInit.setLevel(QServAdqlTest.class.getName(), Level.INFO);
    }

    Job job = new Job()
    {
        @Override
        public String getID() { return "testJob"; }
    };

    public QServAdqlTest() { }

    @Test
    public void testDistance()
    {
        String a = "SELECT ra, dec FROM s.t WHERE DISTANCE(POINT('ICRS', ra, dec), POINT('ICRS', 1, 2)) < 1";
        String s = "SELECT ra, dec FROM s.t WHERE scisql_angSep(ra, dec, 1, 2) < 1";
        compareAdqlToSql(a, s);
    }

    @Test
    public void testPointInCircle()
    {
        String a = "SELECT ra, dec FROM s.t WHERE CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', 1, 2, 3)) = 1";
        String s = "SELECT ra, dec FROM s.t WHERE scisql_s2PtInCircle(ra, dec, 1, 2, 3) = 1";
        compareAdqlToSql(a, s);
    }

    @Test
    public void testPointInPolygon()
    {
        String a = "SELECT ra, dec FROM s.t WHERE CONTAINS(POINT('ICRS', ra, dec), POLYGON('ICRS', 1, 2, 3, 4, 5, 6)) = 1";
        String s = "SELECT ra, dec FROM s.t WHERE scisql_s2PtInCPoly(ra, dec, 1, 2, 3, 4, 5, 6) = 1";
        compareAdqlToSql(a, s);
    }

    @Test
    public void testObscore()
    {
        String a = "SELECT ra, dec FROM s.t WHERE CONTAINS(POINT('ICRS', ra, dec), s_region) = 1";
        String s = "SELECT ra, dec FROM s.t WHERE MBRWITHIN(POINT(ra, dec), s_region_bounds) AND scisql_s2PtInCPoly(ra, dec, s_region_scisql) AND 1 = 1";
        compareAdqlToSql(a, s);
    }

    public void compareAdqlToSql(String adql, String expectedSql)
    {
        try
        {
            log.debug("ADQL: " + adql);
            job.getParameterList().add(new Parameter("QUERY", adql));

            AdqlQueryImpl q = new AdqlQueryImpl();
            q.setJob(job);
            q.setTapSchema(mockTapSchema());

            String sql = q.getSQL();
            log.debug("SQL: " + sql);
            log.debug("Expected SQL: " + expectedSql);
            Assert.assertEquals(expectedSql, sql);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        finally
        {
            job.getParameterList().clear();
        }
    }

    TapSchema mockTapSchema()
    {
        TapSchema ts = new TapSchema();
        ts.getFunctionDescs().add(new FunctionDesc("CONTAINS", TapDataType.INTEGER));
        ts.getFunctionDescs().add(new FunctionDesc("POINT", TapDataType.FUNCTION_ARG));

        ts.getFunctionDescs().add(new FunctionDesc("CIRCLE", TapDataType.DOUBLE));
        ts.getFunctionDescs().add(new FunctionDesc("POLYGON", TapDataType.FUNCTION_ARG));
        ts.getFunctionDescs().add(new FunctionDesc("DISTANCE", TapDataType.DOUBLE));

        SchemaDesc schema = new SchemaDesc("s");
        ts.getSchemaDescs().add(schema);

        TableDesc table = new TableDesc("s", "s.t");
        table.getColumnDescs().add(new ColumnDesc("s.t", "ra", TapDataType.INTEGER));
        table.getColumnDescs().add(new ColumnDesc("s.t", "dec", TapDataType.INTEGER));
        table.getColumnDescs().add(new ColumnDesc("s.t", "s_region", TapDataType.STRING));

        schema.getTableDescs().add(table);
        return ts;
    }
}
