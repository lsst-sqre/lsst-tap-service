package org.opencadc.tap.impl;

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
import static org.junit.Assert.assertEquals;
import java.lang.reflect.Method;

public class ResultsServletTest {

    private static final Logger log = Logger.getLogger(ResultsServletTest.class);
    
    static
    {
        Log4jInit.setLevel("org.opencadc.tap.impl", Level.INFO);
                
    }
    
    Job job = new Job()
    {
        @Override
        public String getID() { return "testJob"; }
    };
    
    public ResultsServletTest() { }
    
    @Test
    public void testGenerateRedirectUrl() throws Exception {
        String bucketUrl = "https://tap-files.lsst.codes";
        String expectedUrl = "https://tap-files.lsst.codes/result_qz4z5hf6qy5509p1.xml";
        ResultsServlet resultsServlet = new ResultsServlet();
        resultsServlet.init();
        String path = "/result_qz4z5hf6qy5509p1.xml";
        
        Method method = ResultsServlet.class.getDeclaredMethod("generateRedirectUrl", String.class, String.class);
        method.setAccessible(true);
        String actualUrl = (String) method.invoke(resultsServlet, bucketUrl, path);
        
        assertEquals(expectedUrl, actualUrl);
    }

    @Test
    public void testGenerateRedirectUrlWithBucket() throws Exception {
        String bucketUrl = "https://tap-files.lsst.codes";
        String expectedUrl = "https://tap-files.lsst.codes/bucket12345/result_qz4z5hf6qy5509p1.xml";
        ResultsServlet resultsServlet = new ResultsServlet();
        resultsServlet.init();
        String path = "/bucket12345/result_qz4z5hf6qy5509p1.xml";
        
        Method method = ResultsServlet.class.getDeclaredMethod("generateRedirectUrl", String.class, String.class);
        method.setAccessible(true);
        String actualUrl = (String) method.invoke(resultsServlet, bucketUrl, path);
        
        assertEquals(expectedUrl, actualUrl);
    }

}