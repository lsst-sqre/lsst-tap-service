package org.opencadc.tap.impl;

import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.tap.DefaultTableWriter;
import ca.nrc.cadc.tap.TapSelectItem;
import ca.nrc.cadc.tap.writer.ResultSetTableData;
import ca.nrc.cadc.tap.writer.format.FormatFactory;
import ca.nrc.cadc.uws.Job;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencadc.tap.impl.votable.CoordSysMetadata;
import org.opencadc.tap.impl.votable.RubinVOTableWriter;
import org.opencadc.tap.impl.votable.UcdCoordDetector;
import org.stringtemplate.v4.ST;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Extension of DefaultTableWriter that adds:
 * <ul>
 *   <li>Template-based datalink service descriptors</li>
 *   <li>Automatic COOSYS and TIMESYS elements derived from field UCDs</li>
 * </ul>
 *
 * @author stvoutsin
 */
public class RubinTableWriter extends DefaultTableWriter {

    private static final Logger log = Logger.getLogger(RubinTableWriter.class);

    private static final String baseUrl = System.getProperty("base_url");
    private static final String datalinkConfig = "/tmp/datalink/";

    // Captured via setFormatFactory so our write() override can use it.
    private FormatFactory rubinFormatFactory;

    // Non-null only when the active format is VOTable; null for CSV/TSV/RSS/Parquet.
    private RubinVOTableWriter rubinVOTableWriter;

    // Populated by generateOutputTable() and consumed by write().
    private CoordSysMetadata currentCoordSysMetadata;

    // Tracks rows written when we handle the write() ourselves.
    private long rubinRowCount = 0L;

    public RubinTableWriter() {
        super();
    }

    public RubinTableWriter(boolean errorWriter) {
        super(errorWriter);
    }

    // -----------------------------------------------------------------------
    // Job / format initialisation
    // -----------------------------------------------------------------------

    /**
     * After the parent initialises the format we inspect the content type to
     * decide whether to create a {@link RubinVOTableWriter} for this request.
     */
    @Override
    public void setJob(Job job) {
        super.setJob(job);

        String ct = getContentType();
        if (ct != null && ct.startsWith(VOTableWriter.CONTENT_TYPE)) {
            VOTableWriter.SerializationType ser = ct.contains("binary2")
                    ? VOTableWriter.SerializationType.BINARY2
                    : VOTableWriter.SerializationType.TABLEDATA;
            rubinVOTableWriter = new RubinVOTableWriter(ser);
        }
    }

    /**
     * Capture the TAP FormatFactory so the overridden {@link #write} can use it.
     */
    @Override
    public void setFormatFactory(FormatFactory ff) {
        super.setFormatFactory(ff);
        this.rubinFormatFactory = ff;
    }

    // -----------------------------------------------------------------------
    // VOTableDocument generation – adds field IDs, detects coordinate systems
    // -----------------------------------------------------------------------

    /**
     * Extends the parent implementation by:
     * <ol>
     *   <li>Assigning sequential {@code id} attributes ("col_0", "col_1", …)</li>
     *   <li>Running UCD-based coordinate/time system detection</li>
     *   <li>Setting {@code ref} on each field that belongs to a detected system</li>
     * </ol>
     */
    @Override
    public VOTableDocument generateOutputTable() throws IOException {
        VOTableDocument votableDocument = super.generateOutputTable();

        if (votableDocument == null) {
            return null;
        }

        VOTableResource resultsResource = votableDocument.getResourceByType("results");
        if (resultsResource == null || resultsResource.getTable() == null) {
            return votableDocument;
        }

        VOTableTable resultsTable = resultsResource.getTable();
        List<VOTableField> fields = resultsTable.getFields();

        for (int i = 0; i < fields.size(); i++) {
            fields.get(i).id = "col_" + i;
        }

        // Detect coordinate / time systems from UCDs and record ref assignments.
        currentCoordSysMetadata = UcdCoordDetector.detect(fields);
        for (Map.Entry<Integer, String> entry : currentCoordSysMetadata.getFieldRefs().entrySet()) {
            int idx = entry.getKey();
            if (idx < fields.size()) {
                fields.get(idx).ref = entry.getValue();
            }
        }

        if (!currentCoordSysMetadata.isEmpty()) {
            log.debug("Detected " + currentCoordSysMetadata.getCoosysDefs().size()
                    + " COOSYS and " + currentCoordSysMetadata.getTimesysDefs().size()
                    + " TIMESYS definition(s)");
        }

        return votableDocument;
    }

    // -----------------------------------------------------------------------
    // Write – use RubinVOTableWriter for VOTable output
    // -----------------------------------------------------------------------

    /**
     * For VOTable output, delegates serialisation to {@link RubinVOTableWriter}
     * so that COOSYS and TIMESYS elements are injected.  All other formats
     * (CSV, TSV, RSS, Parquet) are handled by the parent as usual.
     */
    @Override
    public void write(ResultSet rs, OutputStream out, Long maxrec) throws IOException {
        if (rubinVOTableWriter == null) {
            // Non-VOTable format: parent handles everything (CSV, TSV, RSS, Parquet).
            super.write(rs, out, maxrec);
            return;
        }

        VOTableDocument votableDocument = generateOutputTable();
        VOTableResource resultsResource = votableDocument.getResourceByType("results");
        VOTableTable resultsTable = resultsResource.getTable();

        List<Format<Object>> formats = rubinFormatFactory.getFormats(selectList);
        ResultSetTableData tableData = new ResultSetTableData(rs, formats);
        resultsTable.setTableData(tableData);

        rubinVOTableWriter.setCoordSysMetadata(currentCoordSysMetadata);

        if (maxrec != null) {
            rubinVOTableWriter.write(votableDocument, out, maxrec);
        } else {
            rubinVOTableWriter.write(votableDocument, out);
        }

        rubinRowCount = tableData.getRowCount();
    }

    /**
     * Returns the row count from whichever write path was taken.
     */
    @Override
    public long getRowCount() {
        // If our write() path ran it stored a count; otherwise fall through to parent.
        return rubinRowCount > 0 ? rubinRowCount : super.getRowCount();
    }

    // -----------------------------------------------------------------------
    // Datalink meta-resources (unchanged logic, kept below)
    // -----------------------------------------------------------------------

    /**
     * Override the addMetaResources method to include the template-based
     * implementation.
     *
     * @param votableDocument the document to add meta resources to
     * @param serviceIDs      list of FIELD ID attributes for items in the select list
     * @throws IOException if failing to read files from the config dir
     */
    @Override
    protected void addMetaResources(VOTableDocument votableDocument, List<String> serviceIDs)
            throws IOException {

        List<String> columnNames = new ArrayList<>();
        for (TapSelectItem resultCol : selectList) {
            String fullColumnName = resultCol.tableName + "_" + resultCol.getColumnName();
            columnNames.add(fullColumnName.replace(".", "_"));
        }

        super.addMetaResources(votableDocument, serviceIDs);

        try {
            List<String> datalinks = determineDatalinks(columnNames);
            log.debug("Found " + datalinks.size() + " applicable datalinks for columns");

            List<VOTableResource> metaResources = generateMetaResources(datalinks, columnNames);
            for (VOTableResource metaResource : metaResources) {
                votableDocument.getResources().add(metaResource);
            }
        } catch (IOException e) {
            log.error("Failed to process datalink templates", e);
        }
    }

    /**
     * Determines which datalinks to include based on the columns present in the
     * result set. Reads a JSON manifest file that maps datalink service IDs to
     * required columns.
     *
     * @param columns List of column names in the result set
     * @return List of datalink service IDs that should be included
     * @throws IOException If the manifest file cannot be read
     */
    private List<String> determineDatalinks(List<String> columns) throws IOException {
        String content;
        try {
            Path manifestPath = Path.of(datalinkConfig + "datalink-manifest.json");
            content = Files.readString(manifestPath, StandardCharsets.US_ASCII);
        } catch (IOException e) {
            log.warn("Failed to open datalink manifest", e);
            return new ArrayList<>();
        }

        List<String> datalinks = new ArrayList<>();
        JsonObject manifest = JsonParser.parseString(content).getAsJsonObject();

        for (Map.Entry<String, JsonElement> entry : manifest.entrySet()) {
            JsonArray requiredColumnsArray = entry.getValue().getAsJsonArray();
            List<String> requiredColumns = new ArrayList<>();

            for (JsonElement col : requiredColumnsArray) {
                requiredColumns.add(col.getAsString());
            }

            if (columns.containsAll(requiredColumns)) {
                datalinks.add(entry.getKey());
                log.debug("Adding datalink service: " + entry.getKey());
            }
        }

        return datalinks;
    }

    /**
     * Generates a list of VOTable meta resources.
     *
     * @param serviceIDs A list of service ids used to locate corresponding XML templates
     * @param columns    A list of column names that will be mapped to template variables
     * @return A list of VOTableResource objects containing the processed meta info
     * @throws IOException           If template files cannot be read or processed
     * @throws IllegalStateException If templates cannot be parsed
     */
    private List<VOTableResource> generateMetaResources(List<String> serviceIDs, List<String> columns)
            throws IOException {
        List<VOTableResource> metaResources = new ArrayList<>();
        log.debug("Generating meta resources for " + serviceIDs.size() + " datalink services");

        for (String serviceID : serviceIDs) {
            try {
                Path snippetPath = Path.of(datalinkConfig + serviceID + ".xml");
                String content = Files.readString(snippetPath, StandardCharsets.US_ASCII);

                ST datalinkTemplate = new ST(content, '$', '$');
                datalinkTemplate.add("baseUrl", baseUrl);

                int columnIndex = 0;
                for (String col : columns) {
                    datalinkTemplate.add(col, "col_" + columnIndex);
                    columnIndex++;
                }

                String renderedContent = datalinkTemplate.render();
                VOTableReader reader = new VOTableReader();
                VOTableDocument serviceDocument = reader.read(new StringReader(renderedContent));

                for (VOTableResource resource : serviceDocument.getResources()) {
                    if ("meta".equals(resource.getType())) {
                        metaResources.add(resource);
                        log.debug("Added meta resource from template: " + serviceID + " - "
                                + (resource.getName() != null ? resource.getName() : "unnamed"));
                    }
                }
            } catch (Exception e) {
                log.error("Error processing template " + serviceID, e);
            }
        }

        return metaResources;
    }
}
