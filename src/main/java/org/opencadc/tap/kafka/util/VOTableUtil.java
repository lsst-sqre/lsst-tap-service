package org.opencadc.tap.kafka.util;

import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.tap.QueryRunner;
import ca.nrc.cadc.tap.TapSelectItem;
import ca.nrc.cadc.tap.schema.TapDataType;
import org.apache.log4j.Logger;
import org.opencadc.tap.kafka.models.JobRun;
import org.opencadc.tap.kafka.models.JobRun.ResultFormat;
import org.opencadc.tap.kafka.models.JobRun.ResultFormat.ColumnType;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for VOTable operations.
 * Handles operations such as VOTable generation, column type conversions and
 * format handling.
 * 
 * @author stvoutsin
 */
public class VOTableUtil {
    private static final Logger log = Logger.getLogger(VOTableUtil.class);
    private static final String BASE_URL = System.getProperty("base_url");

    /**
     * Create a ResultFormat configuration for the job.
     * 
     * @param jobId     Job identifier
     * @param jobRunner QueryRunner instance with result template
     * @return ResultFormat with proper configuration
     */
    public static ResultFormat createResultFormat(String jobId, QueryRunner jobRunner) {
        JobRun.ResultFormat.Format format = new JobRun.ResultFormat.Format("VOTable", "BINARY2");

        String[] headerFooter = extractEnvelope(jobRunner);
        String header = headerFooter[0];
        String footer = headerFooter[1];
        String footerOverflow = headerFooter[2];

        JobRun.ResultFormat.Envelope envelope = new JobRun.ResultFormat.Envelope(header, footer, footerOverflow);

        List<ColumnType> columnTypes = convertSelectListToColumnTypes(jobRunner.selectList);

        return new ResultFormat(format, envelope, columnTypes, BASE_URL);
    }

    /**
     * Extract VOTable header, headerOverflow and footer from the result template.
     * The current implementation assumes binary2 encoding.
     * 
     * @param jobRunner QueryRunner containing the result template
     * @return String array with header and footer
     */
    private static String[] extractEnvelope(QueryRunner jobRunner) {
        // Hard coded comment marker
        String commentMarker = "<!--data goes here-->";
        String header = "";
        String footer = "";
        String footerOverflow = "";

        try {
            VOTableWriter w = new VOTableWriter();
            StringWriter sw = new StringWriter();
            w.write(jobRunner.resultTemplate, sw);
            String xmlInput = sw.toString();
            int splitPoint = xmlInput.indexOf(commentMarker);

            if (splitPoint == -1) {
                throw new IllegalArgumentException("Cannot find data placeholder in XML input");
            }

            header = xmlInput.substring(0, splitPoint);
            header += "<DATA>\n      <BINARY2>\n        <STREAM encoding='base64'>\n";

            String rawFooter = xmlInput.substring(splitPoint + commentMarker.length());

            int tableCloseIndex = rawFooter.indexOf("</TABLE>");
            if (tableCloseIndex != -1) {
                String beforeTableClose = "        </STREAM>\n      </BINARY2>\n    </DATA>\n";
                String afterTableClose = rawFooter.substring(tableCloseIndex + "</TABLE>".length());

                footer = beforeTableClose + "</TABLE>" + afterTableClose;

                footerOverflow = beforeTableClose + "</TABLE>\n        <INFO name=\"QUERY_STATUS\" value=\"OVERFLOW\"/>"
                        + afterTableClose;
            } else {
                footer = "        </STREAM>\n      </BINARY2>\n    </DATA>\n" + rawFooter;
                footerOverflow = "        </STREAM>\n      </BINARY2>\n    </DATA>\n" +
                        "<INFO name=\"QUERY_STATUS\" value=\"OVERFLOW\"/>" + rawFooter;
            }

        } catch (Exception e) {
            log.error("Error generating VOTable XML", e);
            throw new RuntimeException("Failed to generate VOTable XML: " + e.getMessage());
        }

        return new String[] { header, footer, footerOverflow};
    }

    /**
     * Convert TapSelectItem list to ColumnType list.
     * 
     * @param selectList List of TapSelectItem from the query
     * @return List of ColumnType for ResultFormat
     */
    public static List<ColumnType> convertSelectListToColumnTypes(List<TapSelectItem> selectList) {
        if (selectList == null || selectList.isEmpty()) {
            return new ArrayList<>();
        }

        List<ColumnType> columnTypes = new ArrayList<>();

        for (TapSelectItem item : selectList) {
            ColumnType columnType = new ColumnType(
                    item.getName(),
                    convertTapDataTypeToVOTableType(item.getDatatype()));

            if (item.getDatatype() != null && item.getDatatype().arraysize != null) {
                columnType.setArraysize(item.getDatatype().arraysize);
            }

            // Handle columns that need rewriting
            // Currently only the access_url column in ivoa.ObsCore
            if (item != null) {
                if ("ivoa.ObsCore".equalsIgnoreCase(item.tableName)) {
                    if ("access_url".equalsIgnoreCase(item.getColumnName())) {
                        columnType.setRequiresUrlRewrite(true);
                    }
                }
            }

            columnTypes.add(columnType);
        }

        return columnTypes;
    }

    /**
     * Convert TapDataType to VOTable datatype.
     * 
     * @param tapDataType TapDataType to convert
     * @return VOTable datatype string
     */
    public static String convertTapDataTypeToVOTableType(TapDataType tapDataType) {
        if (tapDataType == null) {
            return "char";
        }

        switch (tapDataType.getDatatype().toUpperCase()) {
            case "BOOLEAN":
                return "boolean";
            case "SHORT":
                return "short";
            case "INT":
                return "int";
            case "LONG":
                return "long";
            case "FLOAT":
                return "float";
            case "DOUBLE":
                return "double";
            case "CHAR":
            case "VARCHAR":
            case "STRING":
                return "char";
            case "TIMESTAMP":
            case "DATE":
                return "char";
            default:
                return "char";
        }
    }

    /**
     * Generate a VOTable containing an error message.
     * 
     * @param errorMessage The error message to include
     * @return String XML VOTable
     */
    public static String generateErrorVOTable(String errorMessage) {

        // We may be able to use an upstream method for this?
        return "<?xml version=\"1.0\"?>"
                + "<VOTABLE xmlns=\"http://www.ivoa.net/xml/VOTable/v1.3\" "
                + "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
                + "version=\"1.3\">"
                + "<RESOURCE type=\"results\">"
                + "<INFO name=\"QUERY_STATUS\" value=\"ERROR\">"
                + "<![CDATA[" + errorMessage + "]]>"
                + "</INFO>"
                + "</RESOURCE>"
                + "</VOTABLE>";
    }
}