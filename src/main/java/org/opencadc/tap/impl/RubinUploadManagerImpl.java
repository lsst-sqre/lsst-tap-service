package org.opencadc.tap.impl;

import java.security.AccessControlContext;
import java.security.AccessController;
import javax.security.auth.Subject;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.dali.util.DefaultFormat;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.dali.util.FormatFactory;
import ca.nrc.cadc.tap.BasicUploadManager;
import ca.nrc.cadc.tap.db.DatabaseDataType;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.upload.UploadLimits;
import ca.nrc.cadc.tap.upload.UploadTable;
import org.opencadc.tap.kafka.util.DatabaseNameUtil;
import ca.nrc.cadc.uws.Job;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.tap.impl.logging.TAPLogger;
import com.csvreader.CsvWriter;
import com.google.cloud.storage.HttpMethod;

/**
 * Implementation of the UploadManager interface for Rubin.
 * This class handles the upload of tables to cloud storage and generates signed
 * URLs for accessing
 * the uploaded files. It also handles generating the JSON schema for the
 * uploaded tables.
 *
 * @author stvoutsin
 */
public class RubinUploadManagerImpl extends BasicUploadManager {

    private static final Logger log = Logger.getLogger(RubinUploadManagerImpl.class);
    private static final TAPLogger tapLog = new TAPLogger(RubinUploadManagerImpl.class);

    public static final String US_ASCII = "US-ASCII";

    public static final String UTF_8 = "UTF-8";

    // CSV format delimiter.
    public static final char CSV_DELI = ',';

    // TSV format delimiter.
    public static final char TSV_DELI = '\t';

    /**
     * Default expiration time for signed URLs in hours.
     */
    private static final long DEFAULT_URL_EXPIRATION_HOURS = 24;

    public static final UploadLimits MAX_UPLOAD;
    /**
     * Use A filesize limit of 32 Mb using UploadLimits.
     */
    static {
        MAX_UPLOAD = new UploadLimits(32 * 1024L * 1024L); // 32 Mb
    }

    /**
     * DataSource for the DB.
     */
    protected DataSource dataSource;

    /**
     * Database Specific data type.
     */
    protected DatabaseDataType databaseDataType;

    /**
     * IVOA DateFormat
     */
    protected DateFormat dateFormat;

    /**
     * Limitations on the UPLOAD VOTable.
     */
    protected final UploadLimits uploadLimits;

    protected Job job;

    /**
     * Storage for file metadata and signed URLs
     */
    protected Map<String, String> signedUrls;

    public RubinUploadManagerImpl() {
        this(MAX_UPLOAD);
    }

    /**
     * Subclass constructor.
     *
     * @param uploadLimits limits on table upload
     */
    public RubinUploadManagerImpl(UploadLimits uploadLimits) {
        super(uploadLimits);
        this.uploadLimits = uploadLimits;
        this.signedUrls = new HashMap<>();
    }

    @Override
    protected void storeTable(TableDesc table, VOTableTable vot) {

        if (table.dataLocation != null) {
            log.debug("Table already stored in cloud storage: " + table.dataLocation);
            return;
        }

        final int maxRetries = 3;
        final long[] backoffMs = {100, 500, 1000};  // Exponential backoff delays
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                if (attempt > 1) {
                    tapLog.logUpload(table.getTableName(), "Retry attempt " + attempt + " of " + maxRetries);
                }

                doStoreTable(table, vot);
                return;  // Success

            } catch (Exception e) {
                lastException = e;
                tapLog.logUploadWarn(table.getTableName(),
                        "Upload attempt " + attempt + " failed: " + e.getMessage());

                if (attempt < maxRetries) {
                    long delay = backoffMs[attempt - 1];
                    tapLog.logUpload(table.getTableName(), "Waiting " + delay + "ms before retry");
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Upload interrupted during retry", ie);
                    }
                }
            }
        }

        // All retries exhausted
        tapLog.logUploadError(table.getTableName(),
                "Failed to store table after " + maxRetries + " attempts: " + lastException.getMessage());
        throw new RuntimeException("Failed to store table in cloud storage after " + maxRetries + " attempts",
                lastException);
    }

    /**
     * Internal method that performs the actual table storage.
     * Called by storeTable with retry logic.
     */
    private void doStoreTable(TableDesc table, VOTableTable vot) throws Exception {
        String baseFileName = table.getTableName();
        String xmlEmptyFilename = baseFileName + ".empty.xml";
        String csvFilename = baseFileName + ".csv";
        String schemaFilename = baseFileName + ".schema.json";

        VOTableDocument doc = new VOTableDocument();
        VOTableResource resource = new VOTableResource("results");
        resource.setTable(vot);
        doc.getResources().add(resource);

        VOTableTable votable = resource.getTable();
        TableData originalData = votable.getTableData();
        List<VOTableField> fields = votable.getFields();

        // Write JSON Schema file
        writeSchemaFile(votable.getFields(), schemaFilename);
        log.debug("Schema file written to: " + schemaFilename);

        // Generate and store signed URL for schema file
        String schemaSignedUrl = StorageUtils.getSignedUrl(schemaFilename, HttpMethod.GET,
                DEFAULT_URL_EXPIRATION_HOURS);
        signedUrls.put(schemaFilename, schemaSignedUrl);

        // Write CSV version of the data
        tapLog.logUpload(csvFilename, "Starting CSV upload with " + fields.size() + " fields");
        OutputStream csvOs = StorageUtils.getOutputStream(csvFilename, "text/csv");
        tapLog.logUpload(csvFilename, "GCS OutputStream obtained: " + csvOs.getClass().getName());

        long writeStartTime = System.currentTimeMillis();
        long writeEndTime;
        try {
            writeDataWithoutHeaders(fields, originalData, csvOs);
            writeEndTime = System.currentTimeMillis();
            tapLog.logUpload(csvFilename, "CSV data written in " + (writeEndTime - writeStartTime) + "ms, starting GCS flush/close");

            csvOs.flush();
            tapLog.logUpload(csvFilename, "GCS OutputStream flushed, starting close");
        } catch (Exception writeEx) {
            try {
                csvOs.close();
            } catch (Exception ignored) {
            }
            throw writeEx;
        }

        try {
            csvOs.close();
        } catch (Exception closeEx) {
            tapLog.logUploadWarn(csvFilename, "Exception during GCS stream close, verifying upload: " + closeEx.getMessage());
            if (!StorageUtils.blobExists(csvFilename)) {
                throw new IOException("GCS upload failed: blob does not exist after close error", closeEx);
            }
            tapLog.logUpload(csvFilename, "Blob verified to exist despite close exception");
        }
        long closeEndTime = System.currentTimeMillis();
        tapLog.logUploadComplete(csvFilename, closeEndTime - writeStartTime, null, "CSV upload complete");

        // Generate and store signed URL for CSV file
        String csvSignedUrl = StorageUtils.getSignedUrl(csvFilename, HttpMethod.GET, DEFAULT_URL_EXPIRATION_HOURS);
        signedUrls.put(csvFilename, csvSignedUrl);

        // Empty the table data for metadata-only version
        votable.setTableData(null);
        try {
            OutputStream xmlOsEmpty = StorageUtils.getOutputStream(xmlEmptyFilename, "application/x-votable+xml");
            try {
                VOTableWriter voWriterEmpty = new VOTableWriter();
                voWriterEmpty.write(doc, xmlOsEmpty);
                xmlOsEmpty.flush();
            } catch (Exception writeEx) {
                try {
                    xmlOsEmpty.close();
                } catch (Exception ignored) {
                }
                throw writeEx;
            }

            try {
                xmlOsEmpty.close();
            } catch (Exception closeEx) {
                tapLog.logUploadWarn(xmlEmptyFilename, "Exception during GCS stream close, verifying upload: " + closeEx.getMessage());
                if (!StorageUtils.blobExists(xmlEmptyFilename)) {
                    throw new IOException("GCS upload failed - blob does not exist after close error", closeEx);
                }
                log.debug("Blob verified to exist despite close exception: " + xmlEmptyFilename);
            }
            log.debug("Empty VOTable file written to: " + xmlEmptyFilename);

            // Generate and store signed URL for empty XML file
            String xmlSignedUrl = StorageUtils.getSignedUrl(xmlEmptyFilename, HttpMethod.GET,
                    DEFAULT_URL_EXPIRATION_HOURS);
            signedUrls.put(xmlEmptyFilename, xmlSignedUrl);
        } finally {
            // Always restore the original data for potential retries
            votable.setTableData(originalData);
        }

        // Store the signed URL for the CSV file and its schema as the data location
        // and schema location.
        TableDesc.TableLocationInfo location = new TableDesc.TableLocationInfo();
        location.map.put("data", new URI(signedUrls.get(csvFilename)));
        location.map.put("schema", new URI(signedUrls.get(schemaFilename)));
        location.map.put("metadata", new URI(signedUrls.get(xmlEmptyFilename)));
        table.dataLocation = location;
    }

    /**
     * Get all generated signed URLs
     *
     * @return Map of filenames to signed URLs
     */
    public Map<String, String> getSignedUrls() {
        return signedUrls;
    }

    /**
     * Get signed URL for a specific file
     *
     * @param filename The name of the file
     * @return The signed URL or null if not found
     */
    public String getSignedUrl(String filename) {
        return signedUrls.get(filename);
    }

    /**
     * Write the schema.json file containing the field names and types
     * extracted from the VOTable.
     */
    private void writeSchemaFile(List<VOTableField> fields, String schemaFilename)
            throws IOException {
        log.debug("Writing schema to: " + schemaFilename);
        OutputStream schemaOs = StorageUtils.getOutputStream(schemaFilename, "application/json");
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(schemaOs, "UTF-8"));

        try {
            writer.write("[");
            boolean first = true;

            for (VOTableField field : fields) {
                if (!first) {
                    writer.write(",");
                }
                first = false;

                writer.write("\n  { ");
                writer.write("\"name\": \"" + escapeJsonString(field.getName()) + "\", ");
                String datatype = field.getDatatype().toLowerCase();
                String typeString = convertVOTableTypeToMySQL(datatype);

                if (field.getArraysize() != null && !field.getArraysize().trim().isEmpty()) {
                    String arraysize = field.getArraysize().trim();
                    if ("*".equals(arraysize)) {
                        if ("char".equals(datatype) || "unicodechar".equals(datatype)) {
                            typeString = "VARCHAR(255)";
                        } else if ("bit".equals(datatype)) {
                            typeString = "BIT(64)";
                        } else {
                            log.debug("Array type detected for " + datatype + " with arraysize=*, converting to TEXT");
                            typeString = "TEXT";
                        }
                    } else {
                        if ("char".equals(datatype) || "unicodechar".equals(datatype)) {
                            typeString += "(" + arraysize + ")";
                        } else if ("bit".equals(datatype)) {
                            typeString += "(" + arraysize + ")";
                        } else {
                            log.debug("Array type detected for " + datatype + " with arraysize=" + arraysize
                                    + ", converting to TEXT");
                            typeString = "TEXT";
                        }
                    }
                }
                writer.write("\"type\": \"" + escapeJsonString(typeString) + "\"");

                writer.write(" }");
            }

            writer.write("\n]");
            writer.flush();
        } catch (IOException writeEx) {
            try {
                writer.close();
            } catch (Exception ignored) {
            }
            throw writeEx;
        }

        try {
            writer.close();
        } catch (Exception closeEx) {
            tapLog.logUploadWarn(schemaFilename, "Exception during GCS stream close, verifying upload: " + closeEx.getMessage());
            if (!StorageUtils.blobExists(schemaFilename)) {
                throw new IOException("GCS upload failed - blob does not exist after close error", closeEx);
            }
            log.debug("Blob verified to exist despite close exception: " + schemaFilename);
        }
    }

    /**
     * Convert VOTable data types to MySQL equivalents
     */
    private String convertVOTableTypeToMySQL(String voTableType) {
        switch (voTableType) {
            case "boolean":
                return "BOOLEAN";
            case "unsignedbyte":
                return "TINYINT UNSIGNED";
            case "short":
                return "SMALLINT";
            case "int":
                return "INT";
            case "long":
                return "BIGINT";
            case "float":
                return "FLOAT";
            case "double":
                return "DOUBLE";
            case "char":
            case "unicodechar":
                return "CHAR";
            case "bit":
                return "BIT";
            case "floatcomplex":
            case "doublecomplex":
                log.debug("Complex type " + voTableType + " not supported in MySQL, using VARCHAR");
                return "VARCHAR(255)";
            default:
                log.debug("Unknown VOTable type: " + voTableType + ", keeping as-is");
                return voTableType.toUpperCase();
        }
    }

    /**
     * Helper method to escape special characters in JSON strings
     */
    private String escapeJsonString(String input) {
        if (input == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (c) {
                case '\"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '/':
                    sb.append("\\/");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Write CSV data without the header row.
     *
     */
    private void writeDataWithoutHeaders(List<VOTableField> fields, TableData tableData, OutputStream out)
            throws IOException {
        int fieldCount = fields != null ? fields.size() : 0;
        tapLog.logUpload(null, "writeDataWithoutHeaders starting with " + fieldCount + " fields");

        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(out, UTF_8));
        CsvWriter csvWriter = new CsvWriter(bufferedWriter, CSV_DELI);
        int rowCount = 0;

        try {
            FormatFactory formatFactory = new FormatFactory();

            List<Format<Object>> formats = new ArrayList<Format<Object>>();
            if (fields != null && !fields.isEmpty()) {
                for (VOTableField field : fields) {
                    Format<Object> format = null;
                    format = formatFactory.getFormat(field);
                    formats.add(format);
                }
            }

            Iterator<List<Object>> rows = tableData.iterator();
            while (rows.hasNext()) {
                List<Object> row = rows.next();
                if (!fields.isEmpty() && row.size() != fields.size()) {
                    throw new IllegalStateException("cannot write row: " + fields.size() +
                            " metadata fields, " + row.size() + " data columns");
                }

                for (int i = 0; i < row.size(); i++) {
                    Object o = row.get(i);
                    String value = formatValue(o, i, fields, formats);
                    csvWriter.write(value);
                }
                csvWriter.endRecord();
                rowCount++;
            }

            tapLog.logUpload(null, "writeDataWithoutHeaders wrote " + rowCount + " rows");

        } catch (Exception ex) {
            tapLog.logUploadError(null, "Error writing CSV data after " + rowCount + " rows: " + ex.getMessage());
            throw new IOException("error while writing CSV data", ex);
        } finally {
            tapLog.logUpload(null, "writeDataWithoutHeaders flushing buffers");
            csvWriter.flush();
            bufferedWriter.flush();
            tapLog.logUploadComplete(null, null, (long) rowCount, "CSV data write and flush complete");
        }
    }

    /**
     * Format a value for CSV output
     *
     * @param value      The value to format
     * @param fieldIndex The index of the field in the row
     * @param fields     The list of VOTableField definitions
     * @param formats    The list of Format objects for each field
     * @return A string representation
     */
    private String formatValue(Object value, int fieldIndex, List<VOTableField> fields,
            List<Format<Object>> formats) {
        if (value == null) {
            return "\\N";
        }

        if (value instanceof String) {
            String strValue = (String) value;
            if (isSpecialNumericValue(strValue)) {
                return "\\N";
            }
            return strValue;
        }

        if (value instanceof Double || value instanceof Float) {
            double dval = ((Number) value).doubleValue();
            if (Double.isNaN(dval) || Double.isInfinite(dval)) {
                return "\\N";
            }
        }

        Format<Object> fmt = new DefaultFormat();
        if (!fields.isEmpty() && fieldIndex < formats.size()) {
            fmt = formats.get(fieldIndex);
        }
        return fmt.format(value);
    }

    /**
     * Check if a string represents a special/invalid numeric value that should be
     * converted to NULL for database ingestion.
     *
     * @param str The string to check
     * @return true if the string represents NaN or Infinity
     */
    private boolean isSpecialNumericValue(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }

        String lower = str.toLowerCase().trim();
        return lower.equals("nan") ||
                lower.equals("inf") ||
                lower.equals("+inf") ||
                lower.equals("-inf") ||
                lower.equals("infinity") ||
                lower.equals("+infinity") ||
                lower.equals("-infinity");
    }

    /**
     * Constructs the database table name from the schema, upload table name,
     * and the jobID.
     *
     * @return the database table name.
     */
    @Override
    public String getDatabaseTableName(UploadTable uploadTable) {
        return DatabaseNameUtil.getDatabaseTableName(uploadTable);
    }

    /**
     * Get the username of the caller.
     *
     * @return the username of the caller
     */
    protected static String getUsername() {

        AccessControlContext acContext = AccessController.getContext();
        Subject caller = Subject.getSubject(acContext);
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(caller);
        String username;

        if ((authMethod != null) && (authMethod != AuthMethod.ANON)) {
            final Set<HttpPrincipal> curPrincipals = caller.getPrincipals(HttpPrincipal.class);
            final HttpPrincipal[] principalArray = new HttpPrincipal[curPrincipals.size()];
            username = ((HttpPrincipal[]) curPrincipals.toArray(principalArray))[0].getName();
        } else {
            username = null;
        }

        return username;
    }

}
