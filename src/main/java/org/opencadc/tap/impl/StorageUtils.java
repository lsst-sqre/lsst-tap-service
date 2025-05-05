package org.opencadc.tap.impl;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.log4j.Logger;
import org.apache.solr.s3.S3OutputStream;

import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

public class StorageUtils {
    private static Logger log = Logger.getLogger(StorageUtils.class);
    private static final String bucket = System.getProperty("gcs_bucket");
    private static final String bucketURL = System.getProperty("gcs_bucket_url");
    private static final String bucketType = System.getProperty("gcs_bucket_type");

    public static OutputStream getOutputStream(String filename, String contentType) {
        if (bucketType.equals("S3")) {
            return getOutputStreamS3(filename);
        } else {
            return getOutputStreamGCS(filename, contentType);
        }
    }

    public static String getStorageBaseUrl() {
        if (bucketType.equals("S3")) {
            return bucketURL + "/" + bucket;
        } else {
            return bucketURL;
        }
    }

    private static OutputStream getOutputStreamS3(String filename) {
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

    private static OutputStream getOutputStreamGCS(String filename, String contentType) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobId blobId = BlobId.of(bucket, filename);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                .setContentType(contentType)
                .build();
        Blob blob = storage.create(blobInfo);
        log.info("GCS blob created: " + blob.getSelfLink());
        return Channels.newOutputStream(blob.writer());
    }

    /**
     * Get full URL for a stored file
     * 
     * @param filename Filename of the stored object
     * @return Full URL as a string
     */
    public static String getStorageUrl(String filename) {
        if (bucketType.equals("S3")) {
            return bucketURL + "/" + bucket + "/" + filename;
        } else {
            return bucketURL + "/" + filename;
        }
    }

    private static URI getURI() {
        try {
            return new URI(bucketURL);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Invalid bucket URL in configuration: " + e.getMessage(),
                    e);
        }
    }
}