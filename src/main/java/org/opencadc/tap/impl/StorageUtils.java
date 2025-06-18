package org.opencadc.tap.impl;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.HttpMethod;
import org.apache.log4j.Logger;
import org.apache.solr.s3.S3OutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * Utility class for storage operations across GCS and S3.
 * Handles file operations, URL generation, and streaming.
 */
public class StorageUtils {
    private static final Logger log = Logger.getLogger(StorageUtils.class);

    // Configuration properties
    private static final String bucket = System.getProperty("gcs_bucket");
    private static final String bucketURL = System.getProperty("gcs_bucket_url");
    private static final String bucketType = System.getProperty("gcs_bucket_type");

    // Default settings
    private static final long DEFAULT_EXPIRATION_HOURS = 1;
    private static final int DEFAULT_BUFFER_SIZE = 8192;

    /**
     * Get an output stream for writing to a file in storage.
     * 
     * @param filename    Filename of the stored object
     * @param contentType Content type of the file
     * @return OutputStream for writing to the file
     */
    public static OutputStream getOutputStream(String filename, String contentType) {
        if (isS3Storage()) {
            return getOutputStreamS3(filename);
        } else {
            return getOutputStreamGCS(filename, contentType);
        }
    }

    /**
     * Get the base URL for the storage bucket.
     * 
     * @return Base URL string
     */
    public static String getStorageBaseUrl() {
        if (isS3Storage()) {
            return bucketURL + "/" + bucket;
        } else {
            return bucketURL;
        }
    }

    /**
     * Get full URL for a stored file.
     * 
     * @param filename Filename of the stored object
     * @return Full URL as a string
     */
    public static String getStorageUrl(String filename) {
        if (isS3Storage()) {
            return bucketURL + "/" + bucket + "/" + filename;
        } else {
            return bucketURL + "/" + filename;
        }
    }

    /**
     * Generate a signed URL for a storage object with default expiration time.
     *
     * @param filename   Filename of the stored object
     * @param httpMethod HTTP method (GET, PUT, etc)
     * @return Signed URL string
     */
    public static String getSignedUrl(String filename, HttpMethod httpMethod) {
        return getSignedUrl(filename, httpMethod, DEFAULT_EXPIRATION_HOURS);
    }

    /**
     * Generate a signed URL for a storage object with custom expiration time.
     *
     * @param filename        Filename of the stored object
     * @param httpMethod      HTTP method (GET, PUT, etc)
     * @param expirationHours Hours until the signed URL expires
     * @return Signed URL string
     */
    public static String getSignedUrl(String filename, HttpMethod httpMethod, long expirationHours) {
        if (isS3Storage()) {
            throw new UnsupportedOperationException("S3 signed URLs not yet implemented");
        } else {
            try {
                Storage storage = StorageOptions.getDefaultInstance().getService();
                BlobId blobId = BlobId.of(bucket, filename);

                long expiration = TimeUnit.HOURS.toSeconds(expirationHours);
                URL signedUrl = storage.signUrl(
                        BlobInfo.newBuilder(blobId).build(),
                        expiration,
                        TimeUnit.SECONDS,
                        Storage.SignUrlOption.httpMethod(httpMethod),
                        Storage.SignUrlOption.withV4Signature());

                log.debug("Generated signed URL for " + filename + ": " + signedUrl.toString());
                return signedUrl.toString();
            } catch (Exception e) {
                log.error("Error generating signed URL for " + filename, e);
                throw new RuntimeException("Failed to generate signed URL", e);
            }
        }
    }

    /**
     * Creates a signed URL specifically for storing job results with write access.
     * 
     * @param jobId        Job identifier to create object name
     * @param contentType  The content type of the file
     * @param validMinutes How long the signed URL should be valid for
     * @return Destination URL for result storage with write access
     */
    public static String generateJobResultSignedUrl(String jobId, String contentType, int validMinutes) {
        if (isS3Storage()) {
            throw new UnsupportedOperationException("S3 signed URLs for job results not yet implemented");
        } else {
            try {
                String objectName = "result_" + jobId + ".xml";
                Storage storage = StorageOptions.getDefaultInstance().getService();

                BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, objectName))
                        .setContentType(contentType)
                        .build();

                URL signedUrl = storage.signUrl(
                        blobInfo,
                        validMinutes,
                        TimeUnit.MINUTES,
                        Storage.SignUrlOption.httpMethod(HttpMethod.PUT),
                        Storage.SignUrlOption.withV4Signature());

                log.debug("Generated signed URL for job result: " + jobId);
                return signedUrl.toString();
            } catch (Exception e) {
                log.error("Error generating signed URL for job " + jobId, e);
                throw new RuntimeException("Failed to generate job result signed URL", e);
            }
        }
    }

    /**
     * Generates a URL for accessing the job results file.
     * 
     * @param jobId Job identifier
     * @return URL to the job results
     */
    public static String generateResultLocation(String jobId) {
        try {
            return new URL(new URL(bucketURL), "/result_" + jobId + ".xml").toString();
        } catch (MalformedURLException e) {
            log.error("Error generating result destination URL", e);
            return bucketURL + "/result_" + jobId + ".xml";
        }
    }

    /**
     * Check if a blob exists in storage.
     * 
     * @param filename Filename to check
     * @return true if the blob exists, false otherwise
     */
    public static boolean blobExists(String filename) {
        if (isS3Storage()) {
            throw new UnsupportedOperationException("S3 blob existence check not yet implemented");
        } else {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            BlobId blobId = BlobId.of(bucket, filename);
            Blob blob = storage.get(blobId);
            return blob != null;
        }
    }

    /**
     * Streams a file from storage bucket to an output stream.
     * 
     * @param filename     The file name in the bucket
     * @param outputStream The output stream to write to
     * @return true if successful otherwise false
     * @throws IOException If an I/O error occurs
     */
    public static boolean streamBlobToOutput(String filename, OutputStream outputStream) throws IOException {
        if (isS3Storage()) {
            throw new UnsupportedOperationException("S3 streaming not yet implemented");
        } else {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            BlobId blobId = BlobId.of(bucket, filename);
            Blob blob = storage.get(blobId);

            if (blob == null) {
                log.error("Failed to fetch blob from storage: " + bucket + ", file: " + filename);
                return false;
            }

            try (InputStream inputStream = Channels.newInputStream(blob.reader())) {
                byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
                outputStream.flush();
                return true;
            } catch (IOException e) {
                log.error("Error streaming file from storage: " + e.getMessage(), e);
                throw e;
            }
        }
    }

    /**
     * Gets the content type of a blob in storage.
     * 
     * @param filename The file name in the bucket
     * @return The content type or null if not found
     */
    public static String getBlobContentType(String filename) {
        if (isS3Storage()) {
            throw new UnsupportedOperationException("S3 content type retrieval not yet implemented");
        } else {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            BlobId blobId = BlobId.of(bucket, filename);
            Blob blob = storage.get(blobId);

            if (blob == null) {
                return null;
            }

            return blob.getContentType();
        }
    }

    /**
     * Helper method to create an output stream for S3 storage.
     */
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

    /**
     * Helper method to create an output stream for GCS storage.
     */
    private static OutputStream getOutputStreamGCS(String filename, String contentType) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobId blobId = BlobId.of(bucket, filename);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                .setContentType(contentType)
                .build();
        Blob blob = storage.create(blobInfo);
        log.debug("GCS blob created: " + blob.getSelfLink());
        return Channels.newOutputStream(blob.writer());
    }

    /**
     * Helper method to convert the bucket URL to a URI.
     */
    private static URI getURI() {
        try {
            return new URI(bucketURL);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Invalid bucket URL in configuration: " + e.getMessage(),
                    e);
        }
    }

    /**
     * Helper method to check if we're using S3 storage.
     */
    private static boolean isS3Storage() {
        return "S3".equals(bucketType);
    }
}