package org.opencadc.tap.impl;

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
import ca.nrc.cadc.uws.Job;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import com.csvreader.CsvWriter;
import com.google.cloud.storage.HttpMethod;

/**
 * Implementation of the UploadManager interface for Rubin.
 * This class handles the upload of tables to cloud storage and generates signed URLs for accessing
 * the uploaded files. It also handles generating the JSON schema for the uploaded tables.
 * 
 * @author stvoutsin
 */
public class RubinUploadManagerImpl extends BasicUploadManager {

    private static final Logger log = Logger.getLogger(RubinUploadManagerImpl.class);

    public static final String US_ASCII = "US-ASCII";

    // CSV format delimiter.
    public static final char CSV_DELI = ',';

    // TSV format delimiter.
    public static final char TSV_DELI = '\t';

    public static final int MAX_UPLOAD_ROWS = 100000;

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
        MAX_UPLOAD.rowLimit = MAX_UPLOAD_ROWS;
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

    /**
     * Backwards compatible constructor. This uses the default byte limit of 10MiB.
     *
     * @param rowLimit maximum number of rows
     * @deprecated use UploadLimits instead
     */
    @Deprecated
    protected RubinUploadManagerImpl(int rowLimit) {
        // 10MiB of votable xml is roughly 17k rows x 10 columns
        this(new UploadLimits(10 * 1024L * 1024L));
        this.uploadLimits.rowLimit = rowLimit;
    }

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
            log.info("Table already stored in cloud storage: " + table.dataLocation);
            return;
        }

        try {
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
            String schemaSignedUrl = StorageUtils.getSignedUrl(schemaFilename, HttpMethod.GET, DEFAULT_URL_EXPIRATION_HOURS);
            signedUrls.put(schemaFilename, schemaSignedUrl);

            // Write CSV version of the data
            OutputStream csvOs = StorageUtils.getOutputStream(csvFilename, "text/csv");
            writeDataWithoutHeaders(fields, originalData, csvOs);
            csvOs.flush();
            csvOs.close();
            log.debug("CSV file written to: " + csvFilename);
            
            // Generate and store signed URL for CSV file
            String csvSignedUrl = StorageUtils.getSignedUrl(csvFilename, HttpMethod.GET, DEFAULT_URL_EXPIRATION_HOURS);
            signedUrls.put(csvFilename, csvSignedUrl);

            // Empty the table data for metadata-only version
            votable.setTableData(null);

            OutputStream xmlOsEmpty = StorageUtils.getOutputStream(xmlEmptyFilename, "application/x-votable+xml");
            VOTableWriter voWriterEmpty = new VOTableWriter();
            voWriterEmpty.write(doc, xmlOsEmpty);
            xmlOsEmpty.flush();
            xmlOsEmpty.close();
            log.debug("Empty VOTable file written to: " + xmlEmptyFilename);
            
            // Generate and store signed URL for empty XML file
            String xmlSignedUrl = StorageUtils.getSignedUrl(xmlEmptyFilename, HttpMethod.GET, DEFAULT_URL_EXPIRATION_HOURS);
            signedUrls.put(xmlEmptyFilename, xmlSignedUrl);
            // This isn't currently used, but could be useful for debugging?

            votable.setTableData(originalData);

            // Store the signed URL for the CSV file and it's schema as the data location and schema location
            table.dataLocation = new URI(signedUrls.get(csvFilename));
            table.schemaLocation = new URI(signedUrls.get(schemaFilename));

        } catch (Exception e) {
            log.error("Failed to store table in cloud storage", e);
            throw new RuntimeException("Failed to store table in cloud storage", e);
        }
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

        writer.write("[");
        boolean first = true;

        for (VOTableField field : fields) {
            if (!first) {
                writer.write(",");
            }
            first = false;

            writer.write("\n  { ");
            writer.write("\"name\": \"" + escapeJsonString(field.getName()) + "\", ");
            writer.write("\"type\": \"" + escapeJsonString(field.getDatatype()) + "\"");
            writer.write(" }");
        }

        writer.write("\n]");
        writer.flush();
        writer.close();
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
     * Write CSV data without the header row
     */
    private void writeDataWithoutHeaders(List<VOTableField> fields, TableData tableData, OutputStream out)
            throws IOException {
        Writer writer = new BufferedWriter(new OutputStreamWriter(out, US_ASCII));
        CsvWriter csvWriter = new CsvWriter(writer, CSV_DELI);

        try {
            FormatFactory formatFactory = new FormatFactory();

            List<Format<Object>> formats = new ArrayList<Format<Object>>();
            if (fields != null && !fields.isEmpty()) {
                for (VOTableField field : fields) {
                    Format<Object> format = null;
                    if (field.getFormat() == null) {
                        format = formatFactory.getFormat(field);
                    } else {
                        format = field.getFormat();
                    }
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
                    Format<Object> fmt = new DefaultFormat();
                    if (!fields.isEmpty()) {
                        fmt = formats.get(i);
                    }
                    csvWriter.write(fmt.format(o));
                }
                csvWriter.endRecord();
            }
        } catch (Exception ex) {
            throw new IOException("error while writing CSV data", ex);
        } finally {
            csvWriter.flush();
        }
    }

}
