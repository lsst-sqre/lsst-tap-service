package org.opencadc.tap.ws;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;
import org.opencadc.tap.config.TapConfig;
import org.opencadc.tap.storage.StorageUtils;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.uws.web.InlineContentException;
import ca.nrc.cadc.uws.web.UWSInlineContentHandler;

/**
 * Stores inline UPLOAD content in the bucket and swaps the parameter for its URL.
 *
 */
public class RubinUWSContentHandler implements UWSInlineContentHandler {
    private static Logger log = Logger.getLogger(RubinUWSContentHandler.class);

    private static final int BUFFER_SIZE = 64 * 1024;

    private static final String VOTABLE_CONTENT_TYPE = "application/x-votable+xml";

    private final long maxUploadSize;

    public RubinUWSContentHandler() {
        this(TapConfig.maxUploadSize());
    }

    RubinUWSContentHandler(long maxUploadSize) {
        this.maxUploadSize = maxUploadSize;
    }

    @Override
    public Content accept(String name, String contentType, InputStream inputStream)
            throws InlineContentException, IOException {
        log.debug("name: " + name);
        log.debug("Content-Type: " + contentType);

        if (inputStream == null) {
            throw new IOException("InputStream cannot be null");
        }

        String xmlFilename = name + "-" + new RandomStringGenerator(16).getID() + ".xml";

        // The bytes are stored as received, so don't trust the client's content type.
        log.debug("Copying upload to: " + xmlFilename);
        OutputStream xmlOs;
        try {
            xmlOs = StorageUtils.getOutputStream(xmlFilename, VOTABLE_CONTENT_TYPE);
        } catch (RuntimeException e) {
            throw storageFailure(xmlFilename, e);
        }

        // On failure the stream is left unclosed. On GCS that means nothing gets
        // committed. On S3 a large upload may already have started a multipart
        // upload, which is only cleaned up by an AbortIncompleteMultipartUpload
        // lifecycle rule on the bucket.
        long total = 0;
        byte[] buf = new byte[BUFFER_SIZE];
        int n;
        while ((n = inputStream.read(buf)) != -1) {
            total += n;
            if (total > maxUploadSize) {
                // cadc-rest turns an IOException here into a 500, but an
                // IllegalArgumentException into a 400 that keeps this message.
                throw new IllegalArgumentException("Upload " + name + " exceeds the maximum upload size of "
                        + maxUploadSize + " bytes");
            }
            try {
                xmlOs.write(buf, 0, n);
            } catch (IOException | RuntimeException e) {
                throw storageFailure(xmlFilename, e);
            }
        }
        try {
            xmlOs.close();
        } catch (IOException | RuntimeException e) {
            throw storageFailure(xmlFilename, e);
        }
        log.debug("Copied " + total + " bytes to " + xmlFilename);

        String xmlUrl = StorageUtils.getStorageUrl(xmlFilename);
        log.debug("Returning URL: " + xmlUrl);

        Content ret = new Content();
        ret.name = UWSInlineContentHandler.CONTENT_PARAM_REPLACE;
        ret.value = new UWSInlineContentHandler.ParameterReplacement(
                "param:" + name,
                xmlUrl);
        return ret;
    }

    /**
     * Log a failure on the storage side, which points at a problem on our end.
     *
     */
    private static IOException storageFailure(String filename, Exception e) {
        log.error("Failed to store upload " + filename, e);
        return new IOException("Failed to store upload " + filename + ": " + e.getMessage(), e);
    }

}
