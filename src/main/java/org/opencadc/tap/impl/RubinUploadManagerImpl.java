/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2016.                            (c) 2016.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
*  $Revision: 4 $
*
************************************************************************
 */

package org.opencadc.tap.impl;

import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.dali.tables.TableWriter;
import ca.nrc.cadc.dali.tables.ascii.AsciiTableWriter;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.dali.util.DefaultFormat;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.dali.util.FormatFactory;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.tap.BasicUploadManager;
import ca.nrc.cadc.tap.UploadManager;
import ca.nrc.cadc.tap.db.DatabaseDataType;
import ca.nrc.cadc.tap.db.TableCreator;
import ca.nrc.cadc.tap.db.TableLoader;
import ca.nrc.cadc.tap.db.TapConstants;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.upload.JDOMVOTableParser;
import ca.nrc.cadc.tap.upload.UploadLimits;
import ca.nrc.cadc.tap.upload.UploadParameters;
import ca.nrc.cadc.tap.upload.UploadTable;
import ca.nrc.cadc.tap.upload.VOTableParser;
import ca.nrc.cadc.tap.upload.VOTableParserException;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.uws.web.UWSInlineContentHandler;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.apache.solr.s3.S3OutputStream;
import org.opencadc.tap.io.TableDataInputStream;

import com.csvreader.CsvWriter;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/**
 *
 * @author stvoutsin
 */
public class RubinUploadManagerImpl extends BasicUploadManager {

    public static final String US_ASCII = "US-ASCII";

    // CSV format delimiter.
    public static final char CSV_DELI = ',';

    // TSV format delimiter.
    public static final char TSV_DELI = '\t';

    private static final String bucket = System.getProperty("gcs_bucket");
    private static final String bucketURL = System.getProperty("gcs_bucket_url");
    private static final String bucketType = System.getProperty("gcs_bucket_type");

    private static final Logger log = Logger.getLogger(RubinUploadManagerImpl.class);

    // TAP-1.0 xtypes that can just be dropped from ColumnDesc
    private static final List<String> TAP10_XTYPES = Arrays.asList(
            TapConstants.TAP10_CHAR, TapConstants.TAP10_VARCHAR,
            TapConstants.TAP10_DOUBLE, TapConstants.TAP10_REAL,
            TapConstants.TAP10_BIGINT, TapConstants.TAP10_INTEGER, TapConstants.TAP10_SMALLINT);

    public static final int MAX_UPLOAD_ROWS = 100000;

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
    }

    @Override
    protected void storeTable(TableDesc table, VOTableTable vot) {

        if (table.dataLocation != null) {
            log.info("Table already stored in cloud storage: " + table.dataLocation);
            return; // Table already handled by inline content handler
        }

        try {
            String uniqueId = new RandomStringGenerator(16).getID();
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


            // Write CSV version of the data
            log.info("Writing CSV to: " + csvFilename);
            OutputStream csvOs = StorageUtils.getOutputStream(csvFilename, "text/csv");
            writeDataWithoutHeaders(fields, originalData, csvOs);
            csvOs.flush();
            csvOs.close();
            log.info("CSV file written to: " + csvFilename);

            // Empty the table data for metadata-only version
            votable.setTableData(null);

 
            log.info("Writing empty VOTable to: " + xmlEmptyFilename);
            OutputStream xmlOsEmpty = StorageUtils.getOutputStream(xmlEmptyFilename, "application/x-votable+xml");
            VOTableWriter voWriterEmpty = new VOTableWriter();
            voWriterEmpty.write(doc, xmlOsEmpty);
            xmlOsEmpty.flush();
            xmlOsEmpty.close();

            votable.setTableData(originalData);

            URI votableUri = new URI(StorageUtils.getStorageBaseUrl() + "/" + xmlEmptyFilename);
            table.dataLocation = votableUri;
            log.info("Table " + table.getTableName() + " stored at " + table.dataLocation);
        } catch (Exception e) {
            log.error("Failed to store table in cloud storage", e);
            throw new RuntimeException("Failed to store table in cloud storage", e);
        }
        
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
            // Initialize the format factory
            FormatFactory formatFactory = new FormatFactory();

            // Initialize the list of associated formats
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

            // Skip writing headers - just write the data rows
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
