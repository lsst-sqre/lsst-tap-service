package org.opencadc.tap.impl;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.HttpMethod;
import org.apache.log4j.Logger;
import org.apache.solr.s3.S3OutputStream;
import org.opencadc.tap.kafka.models.OutputFormat;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

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

    private static final Region REGION = Region.US_EAST_1;

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
        return bucketURL + "/" + bucket;  
    }

    /**
     * Get full URL for a stored file.
     * 
     * @param filename Filename of the stored object
     * @return Full URL as a string
     */
    public static String getStorageUrl(String filename) {
        return bucketURL + "/" + bucket + "/" + filename; 
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
     * Unified method to generate signed URLs for GCS and S3
     */
    public static String getSignedUrl(String filename, HttpMethod httpMethod, double expirationHours) {
        if (isS3Storage()) {
            try (S3Presigner presigner = getS3Presigner()) {
                if (httpMethod == HttpMethod.GET) {
                    GetObjectRequest getRequest = GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(filename)
                            .build();

                    GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                            .getObjectRequest(getRequest)
                            .signatureDuration(Duration.ofMillis((long) (expirationHours * 3600 * 1000)))
                            .build();

                    PresignedGetObjectRequest presigned = presigner.presignGetObject(presignRequest);
                    log.debug("Generated S3 GET signed URL for " + filename + ": " + presigned.url());
                    return presigned.url().toString();

                } else if (httpMethod == HttpMethod.PUT) {
                    PutObjectRequest putRequest = PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(filename)
                            .build();

                    PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                            .putObjectRequest(putRequest)
                            .signatureDuration(Duration.ofMillis((long) (expirationHours * 3600 * 1000)))
                            .build();

                    PresignedPutObjectRequest presigned = presigner.presignPutObject(presignRequest);
                    log.debug("Generated S3 PUT signed URL for " + filename + ": " + presigned.url());
                    return presigned.url().toString();
                } else {
                    throw new UnsupportedOperationException(
                            "HTTP method not supported for S3 signed URL: " + httpMethod);
                }
            }
        } else {
            try {
                Storage storage = StorageOptions.getDefaultInstance().getService();
                BlobId blobId = BlobId.of(bucket, filename);

                long expiration = TimeUnit.HOURS.toSeconds((long) expirationHours);
                URL signedUrl = storage.signUrl(
                        BlobInfo.newBuilder(blobId).build(),
                        expiration,
                        TimeUnit.SECONDS,
                        Storage.SignUrlOption.httpMethod(httpMethod),
                        Storage.SignUrlOption.withV4Signature());

                log.debug("Generated GCS signed URL for " + filename + ": " + signedUrl);
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
     * @param format       The output format for the job results
     * 
     * @return Destination URL for result storage with write access
     */
    public static String generateJobResultSignedUrl(String jobId, String contentType, int validMinutes, OutputFormat format) {
        String objectName = "result_" + jobId + "." + format.getExtension();

        if (isS3Storage()) {
            // Convert validMinutes to hours fraction
            double expirationHours = validMinutes / 60.0;
            return getSignedUrl(objectName, HttpMethod.PUT, expirationHours);
        } else {
            try {
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
    public static String generateResultLocation(String jobId, OutputFormat format) {
        return getStorageUrl("result_" + jobId + "." + format.getExtension());

    }

    /**
     * Check if a blob exists in storage.
     * 
     * @param filename Filename to check
     * @return true if the blob exists, false otherwise
     */
    public static boolean blobExists(String filename) {
        if (isS3Storage()) {
            try (S3Client s3Client = getS3Client()) {
                HeadObjectRequest head = HeadObjectRequest.builder()
                        .bucket(bucket)
                        .key(filename)
                        .build();
                s3Client.headObject(head);
                return true;
            } catch (Exception e) {
                return false;
            }
        } else {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            Blob blob = storage.get(BlobId.of(bucket, filename));
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
            try (S3Client s3Client = getS3Client();
                    InputStream inputStream = s3Client.getObject(
                            GetObjectRequest.builder().bucket(bucket).key(filename).build())) {
                byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
                outputStream.flush();
                return true;
            }
        } else {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            Blob blob = storage.get(BlobId.of(bucket, filename));
            if (blob == null)
                return false;

            try (InputStream inputStream = Channels.newInputStream(blob.reader())) {
                byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
                outputStream.flush();
                return true;
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
            try (S3Client s3Client = getS3Client()) {
                HeadObjectResponse head = s3Client.headObject(
                        HeadObjectRequest.builder().bucket(bucket).key(filename).build());
                return head.contentType();
            }
        } else {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            Blob blob = storage.get(BlobId.of(bucket, filename));
            return blob == null ? null : blob.getContentType();
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

        S3Client s3Client = getS3Client(config);
        return new S3OutputStream(s3Client, filename, bucket);
    }

    /**
     * Helper method to create an output stream for GCS storage.
     * Uses storage.writer() directly instead of create() + blob.writer() to avoid
     * potential timing issues where the channel could be closed prematurely.
     */
    private static OutputStream getOutputStreamGCS(String filename, String contentType) {
        try {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, filename))
                    .setContentType(contentType)
                    .build();
            return Channels.newOutputStream(storage.writer(blobInfo));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create GCS output stream", e);
        }
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
        return "S3".equalsIgnoreCase(bucketType);
    }

    private static S3Presigner getS3Presigner() {
        return S3Presigner.builder()
                .endpointOverride(getURI())
                .region(REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    private static S3Client getS3Client() {
        return getS3Client(
                S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build());
    }
    
    private static S3Client getS3Client(S3Configuration config) {
        return S3Client.builder()
                .endpointOverride(getURI())
                .serviceConfiguration(config)
                .region(REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .httpClientBuilder(ApacheHttpClient.builder()
                        .maxConnections(50)
                        .connectionTimeout(Duration.ofSeconds(10))
                        .socketTimeout(Duration.ofSeconds(30)))
                .build();
    }
}