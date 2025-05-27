package org.opencadc.tap.impl;

import ca.nrc.cadc.tap.DefaultTableWriter;
import ca.nrc.cadc.tap.TapSelectItem;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.date.DateUtil;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.stringtemplate.v4.ST;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Extension of DefaultTableWriter that adds support for template-based datalink
 * service descriptors.
 * 
 * @author stvoutsin
 */
public class RubinTableWriter extends DefaultTableWriter {

    private static final Logger log = Logger.getLogger(RubinTableWriter.class);

    private static final String baseUrl = System.getProperty("base_url");
    private static final String datalinkConfig = "/usr/share/tomcat/webapps/tap##1000/datalink/";
        
    public RubinTableWriter() {
        super();
    }

    public RubinTableWriter(boolean errorWriter) {
        super(errorWriter);
    }

    /**
     * Override the addMetaResources method to include the template-based
     * implementation.
     * 
     * @param votableDocument the document to add meta resources to
     * @param serviceIDs      list of FIELD ID attributes for items in the select
     *                        list
     * @throws IOException if failing to read files from the config dir
     */
    @Override
    protected void addMetaResources(VOTableDocument votableDocument, List<String> serviceIDs)
            throws IOException {

        List<String> columnNames = new ArrayList<>();
        int columnIndex = 0;
        for (TapSelectItem resultCol : selectList) {
            String fullColumnName = resultCol.tableName + "_" + resultCol.getColumnName();
            columnNames.add(fullColumnName.replace(".", "_"));
            columnIndex++;
        }

        super.addMetaResources(votableDocument, serviceIDs);

        try {
            List<String> datalinks = determineDatalinks(columnNames);
            log.info("Found " + datalinks.size() + " applicable datalinks for columns");

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
     * result set.
     * Reads a JSON manifest file that maps datalink service IDs to required
     * columns.
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
            return new ArrayList<String>();
        }

        List<String> datalinks = new ArrayList<String>();
        JsonObject manifest = JsonParser.parseString(content).getAsJsonObject();

        for (Map.Entry<String, JsonElement> entry : manifest.entrySet()) {
            JsonArray requiredColumnsArray = entry.getValue().getAsJsonArray();
            List<String> requiredColumns = new ArrayList<String>();

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
     * @param serviceIDs A list of service ids used to locate corresponding XML
     *                   templates
     * @param columns    A list of column names that will be mapped to template
     *                   variables
     * @return A list of VOTableResource objects containing the processed meta info
     * @throws IOException           If template files cannot be read or processed
     * @throws IllegalStateException If templates cannot be parsed
     */
    private List<VOTableResource> generateMetaResources(List<String> serviceIDs, List<String> columns)
            throws IOException {
        List<VOTableResource> metaResources = new ArrayList<>();
        log.info("Generating meta resources for " + serviceIDs.size() + " datalink services");

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
                        log.debug("Added meta resource from template: " + serviceID + " - " + 
                                 (resource.getName() != null ? resource.getName() : "unnamed"));
                    }
                }


            } catch (Exception e) {
                log.error("Error processing template " + serviceID, e);
            }
        }

        return metaResources;
    }

    @Override
    public VOTableDocument generateOutputTable() throws IOException {
        VOTableDocument votableDocument = super.generateOutputTable();

        if (votableDocument == null) {
            return null;
        }

        VOTableResource resultsResource = votableDocument.getResourceByType("results");
        if (resultsResource == null || resultsResource.getTable() == null) {
            return votableDocument; // No modification needed if structure is unexpected
        }

        VOTableTable resultsTable = resultsResource.getTable();
        List<VOTableField> fields = resultsTable.getFields();

        for (int i = 0; i < fields.size(); i++) {
            VOTableField field = fields.get(i);
            field.id = "col_" + i;
        }

        return votableDocument;
    }

}
