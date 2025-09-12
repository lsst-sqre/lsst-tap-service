package org.opencadc.tap.kafka.util;

import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.tap.QueryRunner;
import ca.nrc.cadc.tap.TapSelectItem;
import ca.nrc.cadc.tap.schema.TapDataType;
import org.apache.log4j.Logger;
import org.opencadc.tap.kafka.models.JobRun;
import org.opencadc.tap.kafka.models.JobRun.ResultFormat;
import org.opencadc.tap.kafka.models.JobRun.ResultFormat.ColumnType;
import org.opencadc.tap.kafka.models.OutputFormat;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private static final boolean URL_REWRITE_ENABLED = Boolean.parseBoolean(
            System.getProperty("url.rewrite.enabled", "true"));
    private static final String URL_REWRITE_RULES = System.getProperty(
            "url.rewrite.rules", "ivoa.ObsCore:access_url");

    private static Map<String, List<String>> urlRewriteRules = null;

    /**
     * Create a ResultFormat configuration for the job.
     * 
     * @param jobId     Job identifier
     * @param jobRunner QueryRunner instance with result template
     * @param format    OutputFormat for the job
     * 
     * @return ResultFormat with proper configuration
     */
    public static ResultFormat createResultFormat(String jobId, QueryRunner jobRunner, OutputFormat format) {
        JobRun.ResultFormat.Format resultformat = new JobRun.ResultFormat.Format(format.getName(), format.getSerialization());
        
        String[] headerFooter = extractEnvelope(jobRunner, format);
        String header = headerFooter[0];
        String footer = headerFooter[1];
        String footerOverflow = headerFooter[2];

        JobRun.ResultFormat.Envelope envelope = new JobRun.ResultFormat.Envelope(header, footer, footerOverflow);

        List<ColumnType> columnTypes = convertSelectListToColumnTypes(jobRunner.selectList);

        return new ResultFormat(resultformat, envelope, columnTypes, BASE_URL);
    }

    /**
     * Extract VOTable header, headerOverflow and footer from the result template.
     * The current implementation assumes binary2 encoding.
     * 
     * @param jobRunner QueryRunner containing the result template
     * @param format    OutputFormat for the job
     * 
     * @return String array with header and footer
     */
    private static String[] extractEnvelope(QueryRunner jobRunner, OutputFormat format) {
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
            String rawFooter = xmlInput.substring(splitPoint + commentMarker.length());
    
            if (format.getName().equalsIgnoreCase("parquet")) {
                // VOParquet metadata only — no DATA block
                footer = rawFooter;
                footerOverflow =
                        "<INFO name=\"QUERY_STATUS\" value=\"OVERFLOW\"/>" + rawFooter;
            } else {
                // Normal VOTable binary2
                header += "<DATA>\n      <BINARY2>\n        <STREAM encoding='base64'>\n";
    
                int tableCloseIndex = rawFooter.indexOf("</TABLE>");
                if (tableCloseIndex != -1) {
                    String beforeTableClose = "        </STREAM>\n      </BINARY2>\n    </DATA>\n";
                    String afterTableClose = rawFooter.substring(tableCloseIndex + "</TABLE>".length());
    
                    footer = beforeTableClose + "</TABLE>" + afterTableClose;
    
                    footerOverflow = beforeTableClose + "</TABLE>\n"
                            + "        <INFO name=\"QUERY_STATUS\" value=\"OVERFLOW\"/>"
                            + afterTableClose;
                } else {
                    footer = "        </STREAM>\n      </BINARY2>\n    </DATA>\n" + rawFooter;
                    footerOverflow = "        </STREAM>\n      </BINARY2>\n    </DATA>\n"
                            + "<INFO name=\"QUERY_STATUS\" value=\"OVERFLOW\"/>" + rawFooter;
                }
            }
    
        } catch (Exception e) {
            log.error("Error generating VOTable XML", e);
            throw new RuntimeException("Failed to generate VOTable XML: " + e.getMessage());
        }
    
        return new String[] { header, footer, footerOverflow };
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
            if (item != null && shouldRewriteUrl(item.tableName, item.getColumnName())) {
                columnType.setRequiresUrlRewrite(true);
            }

            columnTypes.add(columnType);
        }

        return columnTypes;
    }

    /**
     * Check if a column requires URL rewriting
     * 
     * @param tableName  The table name
     * @param columnName The column name
     * @return true if URL rewriting is needed
     */
    private static boolean shouldRewriteUrl(String tableName, String columnName) {
        if (!URL_REWRITE_ENABLED || tableName == null || columnName == null) {
            return false;
        }

        Map<String, List<String>> rules = getUrlRewriteRules();

        List<String> columnsForTable = rules.get(tableName.toLowerCase());
        if (columnsForTable != null) {
            return columnsForTable.contains(columnName.toLowerCase());
        }

        return false;
    }

    /**
     * Parse and cache URL rewrite rules
     * 
     * @return Map of table names to lists of column names that need rewriting
     */
    private static synchronized Map<String, List<String>> getUrlRewriteRules() {
        if (urlRewriteRules == null) {
            urlRewriteRules = new HashMap<>();

            if (URL_REWRITE_RULES != null && !URL_REWRITE_RULES.trim().isEmpty()) {
                try {
                    String[] rules = URL_REWRITE_RULES.split(",");
                    for (String rule : rules) {
                        String[] parts = rule.trim().split(":");
                        if (parts.length == 2) {
                            String tableName = parts[0].trim().toLowerCase();
                            String columnName = parts[1].trim().toLowerCase();

                            urlRewriteRules.computeIfAbsent(tableName, k -> new ArrayList<>())
                                    .add(columnName);
                            log.debug("Added URL rewrite rule: " + tableName + "." + columnName);
                        } else {
                            log.warn("Invalid URL rewrite rule format: " + rule +
                                    " (expected format: table:column)");
                        }
                    }
                } catch (Exception e) {
                    log.error("Error parsing URL rewrite rules: " + URL_REWRITE_RULES, e);
                }
            }

            if (urlRewriteRules.isEmpty()) {
                log.debug("No URL rewrite rules configured");
            }
        }

        return urlRewriteRules;
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
            case "UNICODECHAR":
                return "unicodeChar";
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
