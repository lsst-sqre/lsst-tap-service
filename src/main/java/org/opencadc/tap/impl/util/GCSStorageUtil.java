package org.opencadc.tap.impl.util;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for GCS operations.
 * Handles URL generation, result streaming, and other GCS actions.
 * 
 * @author stvoutsin
 */
public class GCSStorageUtil {
    private static final Logger log = Logger.getLogger(GCSStorageUtil.class);

    /**
     * Creates a signed URL for storing job results in GCS with write access.
     * 
     * @param bucket       The GCS bucket name
     * @param jobId        Job identifier to create object name
     * @param contentType  The content type of the file
     * @param validMinutes How long the signed URL should be valid for
     * @return Destination URL for result storage with write access
     */
    public static String generateSignedUrl(String bucket, String jobId, String contentType, int validMinutes) {
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
            return null;
        }
    }

    /**
     * Generates a URL for accessing the results file.
     * 
     * @param bucketURL Base URL for the bucket
     * @param jobId     Job identifier
     * @return URL to the job results
     */
    public static String generateResultLocation(String bucketURL, String jobId) {
        try {
            return new URL(new URL(bucketURL), "/result_" + jobId + ".xml").toString();
        } catch (MalformedURLException e) {
            log.error("Error generating result destination URL", e);
            return bucketURL + "/result_" + jobId + ".xml";
        }
    }

    /**
     * Streams a file from GCS bucket to an output stream.
     * 
     * @param bucket       The GCS bucket name
     * @param filename     The file name in the bucket
     * @param outputStream The output stream to write to
     * @return true if successful otherwise false 
     * @throws IOException If an I/O error occurs
     */
    public static boolean streamGCSBlobToOutput(String bucket, String filename, OutputStream outputStream)
            throws IOException {
                
        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobId blobId = BlobId.of(bucket, filename);
        Blob blob = storage.get(blobId);

        if (blob == null) {
            log.error("Failed to fetch result from GCS bucket: " + bucket + ", file: " + filename);
            return false;
        }

        try (InputStream inputStream = Channels.newInputStream(blob.reader())) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            outputStream.flush();
            return true;
        } catch (IOException e) {
            log.error("Error streaming file from GCS: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Gets the content type of a blob in GCS.
     * 
     * @param bucket   The GCS bucket name
     * @param filename The file name in the bucket
     * @return The content type or null if not found
     */
    public static String getBlobContentType(String bucket, String filename) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobId blobId = BlobId.of(bucket, filename);
        Blob blob = storage.get(blobId);

        if (blob == null) {
            return null;
        }

        return blob.getContentType();
    }
}