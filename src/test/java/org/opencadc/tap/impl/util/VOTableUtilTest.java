package org.opencadc.tap.impl.util;

import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.tap.QueryRunner;
import ca.nrc.cadc.tap.TapSelectItem;
import ca.nrc.cadc.tap.schema.TapDataType;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.tap.kafka.models.JobRun.ResultFormat;
import org.opencadc.tap.kafka.models.JobRun.ResultFormat.ColumnType;
import org.opencadc.tap.kafka.util.VOTableUtil;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for the VOTableUtil class.
 * 
 * These tests cover some of the public methods provided by VOTableUtil
 */
public class VOTableUtilTest {

    private TestQueryRunner queryRunner;
    private VOTableDocument testDocument;
    private List<TapSelectItem> testSelectList;

    @Before
    public void setUp() throws Exception {
        testDocument = new VOTableDocument();
        VOTableResource resource = new VOTableResource("results");
        testDocument.getResources().add(resource);

        VOTableTable table = new VOTableTable();
        resource.setTable(table);

        VOTableField field1 = new VOTableField("col1", "char", "10");
        VOTableField field2 = new VOTableField("col2", "int", "*");
        table.getFields().add(field1);
        table.getFields().add(field2);

        VOTableWriter writer = new VOTableWriter();
        StringWriter sw = new StringWriter();
        writer.write(testDocument, sw);
        String votableXml = sw.toString();

        String commentMarker = "<!--data goes here-->";
        int insertPoint = votableXml.indexOf("<TABLE");
        if (insertPoint != -1) {
            insertPoint = votableXml.indexOf(">", insertPoint) + 1;
            votableXml = votableXml.substring(0, insertPoint) + commentMarker + votableXml.substring(insertPoint);
        }
        

        testSelectList = new ArrayList<>();
        
        TapSelectItem item1 = new TapSelectItem("table_name", new TapDataType("VARCHAR", null, null));
        item1.tableName = "tap_schema.tables";
        
        TapSelectItem item2 = new TapSelectItem("column_index", new TapDataType("INT", null, null));
        item2.tableName = "tap_schema.columns";
        
        TapSelectItem item3 = new TapSelectItem("access_url", new TapDataType("VARCHAR", "4096", null));
        item3.tableName = "ivoa.ObsCore";
        
        testSelectList.add(item1);
        testSelectList.add(item2);
        testSelectList.add(item3);
        
        queryRunner = new TestQueryRunner();
        queryRunner.resultTemplate = testDocument;
        queryRunner.selectList = testSelectList;
        
        System.setProperty("base_url", "http://example.org/data/");
    }
    
    private static class TestQueryRunner extends QueryRunner {
        public VOTableDocument resultTemplate;
        public List<TapSelectItem> selectList;
        
        public TestQueryRunner() {
            super();
        }
    }

    @Test
    public void testCreateResultFormat() {
        String jobId = "test-job-123";
        ResultFormat resultFormat = new ResultFormat();
        resultFormat.setFormat(new ResultFormat.Format("VOTable", "BINARY2"));
        resultFormat.setBaseUrl("http://example.org/data/");
        resultFormat.setEnvelope(new ResultFormat.Envelope("header", "footer", "footerOverflow"));
        resultFormat.setColumnTypes(VOTableUtil.convertSelectListToColumnTypes(testSelectList));
            
        
        assertNotNull("ResultFormat should not be null", resultFormat);
        assertEquals("Format name should be VOTable", "VOTable", resultFormat.getFormat().getType());
        assertEquals("Format encoding should be BINARY2", "BINARY2", resultFormat.getFormat().getSerialization());
        assertEquals("Base URL should be set correctly", "http://example.org/data/", resultFormat.getBaseUrl());
        
        assertNotNull("Envelope should not be null", resultFormat.getEnvelope());
        assertNotNull("Header should not be null", resultFormat.getEnvelope().getHeader());
        assertNotNull("Footer should not be null", resultFormat.getEnvelope().getFooter());
        assertNotNull("FooterOverflow should not be null", resultFormat.getEnvelope().getFooterOverflow());
        
        assertTrue("Header should contain 'header'", resultFormat.getEnvelope().getHeader().contains("header"));
        assertTrue("Footer should contain 'footer'", resultFormat.getEnvelope().getFooter().contains("footer"));
        assertTrue("FooterOverflow should contain 'footerOverflow'", 
                resultFormat.getEnvelope().getFooterOverflow().contains("footerOverflow"));
        
        assertNotNull("Column types should not be null", resultFormat.getColumnTypes());
        assertEquals("Should have correct number of columns", testSelectList.size(), resultFormat.getColumnTypes().size());
    }
    
    @Test
    public void testConvertSelectListToColumnTypes() {
        List<ColumnType> columnTypes = VOTableUtil.convertSelectListToColumnTypes(testSelectList);
        
        assertNotNull("Column types should not be null", columnTypes);
        assertEquals("Should have correct number of columns", testSelectList.size(), columnTypes.size());
        
        assertEquals("First column name should match", "table_name", columnTypes.get(0).getName());
        assertEquals("First column type should be char", "char", columnTypes.get(0).getDatatype());
        
        assertEquals("Second column name should match", "column_index", columnTypes.get(1).getName());
        assertEquals("Second column type should be int", "int", columnTypes.get(1).getDatatype());
        
        assertEquals("Third column name should match", "access_url", columnTypes.get(2).getName());
        assertEquals("Third column type should be char", "char", columnTypes.get(2).getDatatype());
        assertEquals("Third column arraysize should be 4096", "4096", columnTypes.get(2).getArraysize());
    }

    @Test
    public void testConvertSelectListToColumnTypes_EmptyList() {
        List<ColumnType> columnTypes = VOTableUtil.convertSelectListToColumnTypes(new ArrayList<>());
        
        assertNotNull("Column types should not be null for empty list", columnTypes);
        assertTrue("Column types should be empty for empty list", columnTypes.isEmpty());
    }

    @Test
    public void testConvertSelectListToColumnTypes_NullList() {
        List<ColumnType> columnTypes = VOTableUtil.convertSelectListToColumnTypes(null);
        
        assertNotNull("Column types should not be null for null list", columnTypes);
        assertTrue("Column types should be empty for null list", columnTypes.isEmpty());
    }

    @Test
    public void testConvertTapDataTypeToVOTableType() {
        assertEquals("boolean", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("BOOLEAN", null, null)));
        assertEquals("short", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("SHORT", null, null)));
        assertEquals("int", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("INT", null, null)));
        assertEquals("long", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("LONG", null, null)));
        assertEquals("float", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("FLOAT", null, null)));
        assertEquals("double", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("DOUBLE", null, null)));
        assertEquals("char", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("CHAR", null, null)));
        assertEquals("char", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("VARCHAR", null, null)));
        assertEquals("char", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("STRING", null, null)));
        assertEquals("char", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("TIMESTAMP", null, null)));
        assertEquals("char", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("DATE", null, null)));
        assertEquals("boolean", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("boolean", null, null)));
        assertEquals("int", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("int", null, null)));
        assertEquals("char", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("varchar", null, null)));
        assertEquals("char", VOTableUtil.convertTapDataTypeToVOTableType(new TapDataType("UNKNOWN_TYPE", null, null)));        
        assertEquals("char", VOTableUtil.convertTapDataTypeToVOTableType(null));
    }

    @Test
    public void testGenerateErrorVOTable() {
        String errorMessage = "Test error message";
        String errorVOTable = VOTableUtil.generateErrorVOTable(errorMessage);
        
        assertNotNull("Error VOTable should not be null", errorVOTable);
        assertTrue("Error VOTable should be valid XML", isValidXml(errorVOTable));
        assertTrue("Error VOTable should contain error message", errorVOTable.contains(errorMessage));
        assertTrue("Error VOTable should have ERROR status", errorVOTable.contains("value=\"ERROR\""));
        assertTrue("Error VOTable should contain CDATA section", errorVOTable.contains("<![CDATA["));
        assertTrue("Error VOTable should be proper VOTable v1.3", 
                errorVOTable.contains("xmlns=\"http://www.ivoa.net/xml/VOTable/v1.3\""));
    }
    
    @Test
    public void testGenerateErrorVOTable_EmptyMessage() {
        String errorMessage = "";
        String errorVOTable = VOTableUtil.generateErrorVOTable(errorMessage);
        
        assertNotNull("Error VOTable should not be null", errorVOTable);
        assertTrue("Error VOTable should be valid XML", isValidXml(errorVOTable));
        assertTrue("Error VOTable should have ERROR status", errorVOTable.contains("value=\"ERROR\""));
    }
    
    @Test
    public void testGenerateErrorVOTable_NullMessage() {
        String errorVOTable = VOTableUtil.generateErrorVOTable(null);
        
        assertNotNull("Error VOTable should not be null", errorVOTable);
        assertTrue("Error VOTable should be valid XML", isValidXml(errorVOTable));
        assertTrue("Error VOTable should have ERROR status", errorVOTable.contains("value=\"ERROR\""));
    }
    
    /**
     * Helper method to check if a string is valid XML
     */
    private boolean isValidXml(String xml) {
        try {
            VOTableReader reader = new VOTableReader();
            reader.read(new ByteArrayInputStream(xml.getBytes()));
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}