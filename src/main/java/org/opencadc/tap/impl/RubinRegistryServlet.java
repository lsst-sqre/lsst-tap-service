package org.opencadc.tap.impl;

import org.apache.log4j.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;

/**
 * A servlet that serves registry content from a webapp resource file.
 *
 * @author stvoutsin
 */
public class RubinRegistryServlet extends HttpServlet {
	
    private static final Logger log = Logger.getLogger(RubinRegistryServlet.class);
    private static final String DEFAULT_REGISTRY_FILE = "/resource-caps";
    
    /**
     * Read and return the content of the registry file from the webapp 'resource-caps' resource.
     *
     * @param request  the HttpServletRequest object that contains the request
     * @param response the HttpServletResponse object that contains the response
     * @throws ServletException if an input or output error is detected when the servlet handles the GET request
     * @throws IOException if the request for the GET could not be handled
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) 
        throws ServletException, IOException {
        try {
            String registryContent = getRegistryContent(request);
            
            response.setContentType("text/plain");
            response.setCharacterEncoding("UTF-8");
            
            try (PrintWriter writer = response.getWriter()) {
                writer.write(registryContent);
            }
        } catch (IOException e) {
            log.error("Error serving registry content", e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 
                "An error occurred while processing the request: " + e.getMessage());
        }
    }
    
    /**
     * Reads and returns the content of the registry file from webapp resources.
     *
     * @param request the HttpServletRequest
     * @return the content of the registry file as a string
     * @throws IOException if there's an error reading the file
     */
    private String getRegistryContent(HttpServletRequest request) throws IOException {
        String pathInfo = request.getPathInfo();
        String resourcePath = DEFAULT_REGISTRY_FILE;
        
        if (pathInfo != null && !pathInfo.isEmpty()) {
            resourcePath = pathInfo;
        }
        
        InputStream inputStream = getServletContext().getResourceAsStream(resourcePath);
        if (inputStream == null) {
            throw new IOException("Registry file not found in webapp: " + resourcePath);
        }
        
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        }
        
        return content.toString();
    }
}