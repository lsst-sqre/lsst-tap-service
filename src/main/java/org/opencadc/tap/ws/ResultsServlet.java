package org.opencadc.tap.ws;

import org.opencadc.tap.config.TapConfig;
import org.opencadc.tap.storage.StorageUtils;

import com.google.cloud.storage.HttpMethod;
import org.apache.log4j.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * A servlet that handles redirecting to specific job results.
 * This servlet extracts the VOTable file name from the request path and constructs a URL to redirect the client.
 *
 * @author stvoutsin
 */
public class ResultsServlet extends HttpServlet {
    private static final Logger log = Logger.getLogger(ResultsServlet.class);
    private static final String bucket = TapConfig.gcsBucket();
    private static final String bucketURL = TapConfig.gcsBucketUrl();
    private static final String bucketType = TapConfig.gcsBucketType();

    // A new URL is signed on every request, so it only has to last until the
    // client starts the download. GCS doesn't check expiry after that.
    private static final double SIGNED_URL_EXPIRATION_HOURS = 1;

    /**
     * Processes GET requests by extracting the result filename from the request path and redirecting to the corresponding results URL.
     * The filename is assumed to be the path info of the request URL, following the first '/' character.
     *
     * @param request  the HttpServletRequest object that contains the request
     * @param response the HttpServletResponse object that contains the response
     * @throws ServletException if an input or output error is detected when the servlet handles the GET request
     * @throws IOException if the request for the GET could not be handled
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String path = request.getPathInfo();
        try {
            String redirectUrl;
            if (StorageUtils.useSignedReadUrls()) {
                // Only sign single object names, never arbitrary paths.
                if (!isResultFilePath(path)) {
                    response.sendError(HttpServletResponse.SC_NOT_FOUND);
                    return;
                }
                redirectUrl = StorageUtils.getSignedUrl(path.substring(1), HttpMethod.GET,
                        SIGNED_URL_EXPIRATION_HOURS);
            } else {
                redirectUrl = generateRedirectUrl(bucketURL, path);
            }
            response.sendRedirect(redirectUrl);
        } catch (Exception e) {
            log.error("Failed to build result redirect for " + path, e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "An error occurred while processing the request.");
        }
    }

    /**
     * Whether the request path names a single object, e.g. /result_abc.xml.
     *
     * @param path the request path
     * @return true if the path is a slash followed by a name with no further slashes
     */
    static boolean isResultFilePath(String path) {
        return path != null && path.length() > 1 && path.charAt(0) == '/' && path.indexOf('/', 1) == -1;
    }

    /**
     * Generates the redirect URL based on a path.
     *
     * @param path the request path
     * @return the redirect URL constructed using the bucket URL and results file
     */
    private String generateRedirectUrl(String bucketUrlString, String path) {
        String resultsFile = path.substring(1);

        if (bucket != null && !bucket.trim().isEmpty()) {
            return bucketUrlString + "/" + bucket + "/" + resultsFile;
        } else {
            return bucketUrlString + "/" + resultsFile;
        }
    }
}
