package org.opencadc.tap.impl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.csvreader.CsvWriter;

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
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.uws.web.InlineContentException;
import ca.nrc.cadc.uws.web.UWSInlineContentHandler;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.dali.util.FormatFactory;
import java.io.Writer;
import java.util.Iterator;


public class RubinUWSContentHandler implements UWSInlineContentHandler {
    private static Logger log = Logger.getLogger(RubinUWSContentHandler.class);

    public RubinUWSContentHandler() {
    }

    @Override
    public Content accept(String name, String contentType, InputStream inputStream)
            throws InlineContentException, IOException {
        log.info("name: " + name);
        log.debug("Content-Type: " + contentType);

        if (inputStream == null) {
            throw new IOException("InputStream cannot be null");
        }

        try {
            String baseFilename = name + "-" + new RandomStringGenerator(16).getID();
            String xmlFilename = baseFilename + ".xml";

            log.info("Reading VOTable from input stream");
            VOTableReader voTableReader = new VOTableReader();
            VOTableDocument doc = voTableReader.read(inputStream);

            VOTableResource resource = doc.getResourceByType("results");
            if (resource == null && !doc.getResources().isEmpty()) {
                resource = doc.getResources().get(0);
            }

            if (resource == null || resource.getTable() == null) {
                throw new IOException("No table found in the uploaded VOTable");
            }

            VOTableTable table = resource.getTable();
            TableData originalData = table.getTableData();
            List<VOTableField> fields = table.getFields();

            log.info("Writing full VOTable to: " + xmlFilename);
            OutputStream xmlOs = StorageUtils.getOutputStream(xmlFilename, contentType);
            VOTableWriter voWriter = new VOTableWriter();
            voWriter.write(doc, xmlOs);
            xmlOs.flush();
            xmlOs.close();

            // Return URL to the XML file that contains metadata
            String xmlUrl = StorageUtils.getStorageUrl(xmlFilename);
            log.info("Returning URL: " + xmlUrl);

            Content ret = new Content();
            ret.name = UWSInlineContentHandler.CONTENT_PARAM_REPLACE;
            ret.value = new UWSInlineContentHandler.ParameterReplacement(
                    "param:" + name,
                    xmlUrl);
            return ret;
        } catch (Exception e) {
            log.error("Failed to process VOTable content", e);
            throw new IOException("Failed to process VOTable content: " + e.getMessage(), e);
        }

    }

}
