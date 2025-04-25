package org.opencadc.tap.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;

import org.apache.log4j.Logger;
import org.apache.solr.s3.S3OutputStream;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import ca.nrc.cadc.dali.tables.TableWriter;
import ca.nrc.cadc.dali.tables.ascii.AsciiTableWriter;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.tap.upload.VOTableParser;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.uws.web.InlineContentException;
import ca.nrc.cadc.uws.web.UWSInlineContentHandler;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

public class RubinUWSContentHandler implements UWSInlineContentHandler {
    private static Logger log = Logger.getLogger(RubinUWSContentHandler.class);
    private static final String bucket = System.getProperty("gcs_bucket");
    private static final String bucketURL = System.getProperty("gcs_bucket_url");
    private static final String bucketType = System.getProperty("gcs_bucket_type");

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

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[16384];
        int num;
        while ((num = inputStream.read(buf)) > 0) {
            baos.write(buf, 0, num);
        }
        baos.flush();
        byte[] data = baos.toByteArray();

        String filename = name + "-" + new RandomStringGenerator(16).getID() + ".xml";
        OutputStream os = getOutputStream(filename, contentType);

        os.write(data);
        os.flush();
        os.close();

        // Now also write the uploaded table as a CSV (Used for QServ Uploading)
        String csvFilename = filename + ".csv";
        OutputStream csvOs = getOutputStream(csvFilename, "text/csv");

        InputStream voDis = new ByteArrayInputStream(data);

        // Process the VOTable and write as CSV
        final VOTableReader voTableReader = new VOTableReader();
        VOTableDocument doc = voTableReader.read(voDis);
        TableWriter<VOTableDocument> tableWriter = new AsciiTableWriter(AsciiTableWriter.ContentType.CSV);
        tableWriter.write(doc, csvOs);

        csvOs.flush();
        csvOs.close();

        URL retURL;
        if (bucketType.equals("S3")) {
            retURL = new URL(bucketURL + "/" + bucket + "/" + csvFilename);
        } else {
            retURL = new URL(bucketURL + "/" + csvFilename);
        }
        log.info("retURL: " + retURL);

        Content ret = new Content();
        ret.name = UWSInlineContentHandler.CONTENT_PARAM_REPLACE;
        ret.value = new UWSInlineContentHandler.ParameterReplacement(
                "param:" + name,
                retURL.toExternalForm());
        return ret;
    }

    private OutputStream getOutputStream(String filename, String contentType) {
        if (bucketType.equals("S3")) {
            return getOutputStreamS3(filename);
        } else {
            return getOutputStreamGCS(filename, contentType);
        }
    }

    private OutputStream getOutputStreamS3(String filename) {
        S3Configuration config = S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .useArnRegionEnabled(true)
                .build();

        S3Client s3Client = S3Client.builder()
                .endpointOverride(getURI())
                .serviceConfiguration(config)
                .region(Region.US_WEST_2)
                .build();

        return new S3OutputStream(s3Client, filename, bucket);
    }

    private OutputStream getOutputStreamGCS(String filename, String contentType) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobId blobId = BlobId.of(bucket, filename);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                .setContentType("application/x-votable+xml")
                .build();
        Blob blob = storage.create(blobInfo);
        log.info("GCS blob created: " + blob.getSelfLink());
        return Channels.newOutputStream(blob.writer());
    }

    private URI getURI() {
        try {
            return new URI(bucketURL);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Invalid bucket URL in configuration: " + e.getMessage(),
                    e);
        }
    }
}