package org.opencadc.tap.impl;

import org.apache.log4j.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * A servlet that handles redirecting to specific job results.
 * This servlet extracts the VOTable file name from the request path and constructs a URL to redirect the client.
 *
 * @author stvoutsin
 */
public class ResultsServlet extends HttpServlet {
    private static final Logger log = Logger.getLogger(ResultsServlet.class);
    private static final String bucket = System.getProperty("gcs_bucket");
    private static final String bucketURL = System.getProperty("gcs_bucket_url");
    private static final String bucketType = System.getProperty("gcs_bucket_type");
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
        try {
            String path = request.getPathInfo();
            String redirectUrl = generateRedirectUrl(bucketURL, path);
            response.sendRedirect(redirectUrl);
        } catch (Exception e) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "An error occurred while processing the request.");
        }
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
            String encodedBucket = URLEncoder.encode(bucket, StandardCharsets.UTF_8);
            return bucketUrlString + "/" + encodedBucket + "/" + resultsFile;
        } else {
            return bucketUrlString + "/" + resultsFile;
        }
    }
}
