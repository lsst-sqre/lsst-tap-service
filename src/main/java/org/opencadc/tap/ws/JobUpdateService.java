package org.opencadc.tap.ws;

import org.apache.log4j.Logger;
import org.opencadc.tap.kafka.models.JobStatus.ExecutionStatus;
import org.opencadc.tap.kafka.services.CreateJobStatus;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;

public class JobUpdateService extends HttpServlet {
    private static final Logger log = Logger.getLogger(JobUpdateService.class);
    
    private CreateJobStatus createJobStatusService;
    /**
     * Initializes the servlet by retrieving the CreateJobEvent service from the servlet context.
     *
     * @param config the ServletConfig object containing the servlet's configuration
     * @throws ServletException if the CreateJobEvent service is not properly initialized
     */
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        
        log.info("Initializing JobUpdateService");
        ServletContext context = config.getServletContext();
        
        createJobStatusService = (CreateJobStatus) context.getAttribute("jobStatusProducer");
            
        if (createJobStatusService == null) {
            log.error("Job Producer service not found in servlet context");
            throw new ServletException("Job Producer service not properly initialized");
        }
        
        log.info("JobUpdateService initialized successfully");
    }
    
    /**
     * Processes GET requests by extracting query parameters and submitting the query to Kafka.
     *
     * @param request the HttpServletRequest object that contains the request
     * @param response the HttpServletResponse object that contains the response
     * @throws ServletException if an input or output error is detected when the servlet handles the GET request
     * @throws IOException if the request for the GET could not be handled
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        log.info("Received GET request");
        
        String jobID = request.getParameter("jobid");
        String status = request.getParameter("status");

        if (jobID == null || jobID.trim().isEmpty()) {
            log.warn("Missing 'jobID' parameter in request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing 'jobID' parameter");
            return;
        }
        
        if (status == null || status.trim().isEmpty()) {
            log.warn("Missing 'status' parameter in request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing 'status' parameter");
            return;
        }
                
        log.info("Processing jobID: " + jobID);
        
        
        try {
            // Submit the query to Kafka
            String jobId = createJobStatusService.submitQuery(jobID, ExecutionStatus.COMPLETED);
            log.info("Query submitted successfully with job ID: " + jobId);
            
            // Return success response
            response.setContentType("application/json");
            PrintWriter out = response.getWriter();
            out.println("{");
            out.println("  \"status\": \"success\",");
            out.println("  \"jobId\": \"" + jobId + "\",");
            out.println("  \"message\": \"Query submitted to Kafka successfully\"");
            out.println("}");
            
        } catch (ExecutionException | InterruptedException e) {
            log.error("Error submitting query to Kafka", e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 
                "Failed to submit query: " + e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error processing query", e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                "An unexpected error occurred while processing the request: " + e.getMessage());
        }
    }
    
    /**
     * Processes POST requests by delegating to the doGet method.
     *
     * @param request the HttpServletRequest object that contains the request
     * @param response the HttpServletResponse object that contains the response
     * @throws ServletException if an input or output error is detected when the servlet handles the POST request
     * @throws IOException if the request for the POST could not be handled
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        log.info("Received POST request - delegating to GET handler");
        doGet(request, response);
    }
    
    /**
     * Cleans up resources when the servlet is being destroyed.
     */
    @Override
    public void destroy() {
        log.info("JobUpdateService is being destroyed");
        super.destroy();
    }
}