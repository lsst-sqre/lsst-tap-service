package org.opencadc.tap.kafka.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.HttpURLConnection;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobUpdater;
import org.apache.log4j.Logger;
import org.opencadc.tap.impl.StorageUtils;

/**
 * Service for polling jobs until completion and handling results.
 * Manages the execution flow for Kafka-based sync jobs.
 * 
 * @author stvoutsin
 */
public class JobPollingService {
    private static final Logger log = Logger.getLogger(JobPollingService.class);

    private static final int DEFAULT_MAX_ATTEMPTS = Integer.parseInt(
        System.getProperty("tap.sync.polling.maxAttempts", "20"));
    private static final int DEFAULT_POLL_INTERVAL_MS = Integer.parseInt(
        System.getProperty("tap.sync.polling.intervalMs", "3000"));

    private final JobUpdater jobUpdater;
    private final JobPersistence jobPersistence;
    private final String bucket;

    /**
     * JobPollingService Constructor.
     * 
     * @param jobUpdater     JobUpdater implementation
     * @param jobPersistence JobPersistence implementation
     * @param bucket         Storage bucket name
     */
    public JobPollingService(JobUpdater jobUpdater, JobPersistence jobPersistence, String bucket) {
        this.jobUpdater = jobUpdater;
        this.jobPersistence = jobPersistence;
        this.bucket = bucket;
    }

    /**
     * Poll for job completion and handle the result.
     * 
     * @param jobId      The job identifier to poll
     * @param syncOutput The synchronous output for writing results
     * @return true if job completed and results were handled, false otherwise
     * @throws TransientException      If polling times out or is interrupted
     * @throws JobNotFoundException    If the job is not found
     * @throws JobPersistenceException If there's an error accessing job data
     * @throws IOException
     * @throws JobServiceUnavailableException If the job service is unavailable
     * @throws IOException
     */
    public boolean pollAndHandleResults(String jobId, SyncOutput syncOutput)
            throws TransientException, JobNotFoundException, JobPersistenceException, IOException,
            JobServiceUnavailableException {
        try {
            ExecutionPhase finalPhase = pollUntilTerminal(
                    jobId, jobUpdater, DEFAULT_MAX_ATTEMPTS, DEFAULT_POLL_INTERVAL_MS);

            if (finalPhase == null) {
                throw new TransientException("Job execution timed out");
            }

            // Retrieve job details, used for refreshing job status
            // and streaming results or error information
            Job job = jobPersistence.get(jobId);
            jobPersistence.getDetails(job);

            if (ExecutionPhase.COMPLETED.equals(finalPhase)) {
                log.debug("Job " + jobId + " completed successfully, streaming results");
                return streamResults(job, syncOutput);
            } else if (ExecutionPhase.ERROR.equals(finalPhase)) {
                log.debug("Job " + jobId + " completed with ERROR, retrieving error information");
                return streamError(job, syncOutput);
            } else if (ExecutionPhase.ABORTED.equals(finalPhase)) {
                log.debug("Job " + jobId + " was ABORTED");
                handleAborted(syncOutput);
                return true;
            } else {
                log.warn("Job " + jobId + " in unexpected final phase: " + finalPhase);
                handleUnexpectedPhase(syncOutput, finalPhase);
                return false;
            }
        } catch (InterruptedException e) {
            log.error("Polling interrupted for job: " + jobId, e);
            Thread.currentThread().interrupt();
            throw new TransientException("Polling interrupted", e);
        } 
    }

    /**
     * Stream results from a successful job.
     * 
     * @param job        The completed job
     * @param syncOutput The synchronous output
     * @return true if streaming succeeded, false otherwise
     * @throws MalformedURLException
     */
    private boolean streamResults(Job job, SyncOutput syncOutput) throws MalformedURLException {
        String resultURL = null;
        String jobId = job.getID();

        if (job.getResultsList() != null) {
            for (Result result : job.getResultsList()) {
                if ("result".equals(result.getName())) {
                    resultURL = result.getURI().toURL().toString();
                    break;
                }
            }
        }

        if (resultURL == null) {
            log.warn("No result URL found for completed job: " + jobId);
            handleError(syncOutput, 404, "No result found for completed job");
            return false;
        }

        log.debug("Streaming result from URL: " + resultURL);

        try {
            URL url = new URL(resultURL);
            String path = url.getPath();
            String filename = path.substring(path.lastIndexOf('/') + 1);
            String contentType = StorageUtils.getBlobContentType(filename);

            if (contentType != null) {
                syncOutput.setHeader("Content-Type", contentType);
            } else {
                syncOutput.setHeader("Content-Type", "application/x-votable+xml");
            }

            String contentDisposition = "inline; filename=\"" + filename + "\"";
            syncOutput.setHeader("Content-Disposition", contentDisposition);

            boolean success = StorageUtils.streamBlobToOutput(filename, syncOutput.getOutputStream());

            if (success) {
                log.debug("Result successfully streamed for job: " + jobId);
                return true;
            } else {
                handleError(syncOutput, 404, "Result file not found in storage");
                return false;
            }
        } catch (IOException e) {
            log.error("Error streaming results from GCS: " + e.getMessage(), e);
            handleError(syncOutput, 500, "Failed to stream result: " + e.getMessage());
            return false;
        }
    }

    /**
     * Stream error information from a failed job.
     * 
     * @param job        The job that failed with ERROR
     * @param syncOutput The synchronous output
     * @return true if streaming succeeded, false otherwise
     */
    private boolean streamError(Job job, SyncOutput syncOutput) {
        try {
            ErrorSummary errorSummary = job.getErrorSummary();
            if (errorSummary == null) {
                log.warn("No error summary found for job in ERROR state: " + job.getID());
                handleError(syncOutput, 500, "Unknown error occurred during job execution");
                return false;
            }

            URL errorDocumentURL = errorSummary.getDocumentURL();
            if (errorDocumentURL != null) {
                URL url = errorDocumentURL;
                log.debug("Streaming error document from URL: " + url);

                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");

                int responseCode = conn.getResponseCode();
                if (responseCode != 200) {
                    log.error("Failed to fetch error document from " + url + ", response code: " + responseCode);
                    handleError(syncOutput, 500, errorSummary.getSummaryMessage());
                    return false;
                }

                String contentType = conn.getContentType();
                if (contentType != null &&
                        (contentType.contains("votable") || contentType.contains("xml"))) {
                    streamErrorDocument(conn, syncOutput, contentType);
                } else {
                    convertToErrorVOTable(conn, syncOutput);
                }
            } else {
                log.debug("No error document URL, sending VOTable error message");
                handleError(syncOutput, 400, errorSummary.getSummaryMessage());
            }

            log.debug("Error information streamed for job: " + job.getID());
            return true;
        } catch (Exception e) {
            log.error("Error streaming error information: " + e.getMessage(), e);
            try {
                syncOutput.setCode(500);
                syncOutput.setHeader("Content-Type", "application/x-votable+xml");
                String message = VOTableUtil.generateErrorVOTable("Error processing job failure: " + e.getMessage());
                syncOutput.getOutputStream().write(message.getBytes());
            } catch (IOException ioe) {
                log.error("Failed to write error message to output stream", ioe);
            }
            return false;
        }
    }

    /**
     * Poll for a job to reach a terminal state.
     * 
     * @param jobId          The job identifier
     * @param jobUpdater     The JobUpdater to use
     * @param maxAttempts    Maximum number of polling attempts
     * @param pollIntervalMs Polling interval in milliseconds
     * @return The final phase or null if timed out
     * @throws JobNotFoundException If the job is not found
     * @throws InterruptedException If polling is interrupted
     * @throws JobPersistenceException If there's an error accessing job data
     * @throws JobServiceUnavailableException If the job service is unavailable
     */
    public static ExecutionPhase pollUntilTerminal(String jobId, JobUpdater jobUpdater,
            int maxAttempts, int pollIntervalMs)
            throws JobNotFoundException, InterruptedException, JobPersistenceException, JobServiceUnavailableException {

        boolean isTerminal = false;
        ExecutionPhase finalPhase = null;
        int attempts = 0;

        log.debug("Starting to poll for job completion: " + jobId);

        while (!isTerminal && attempts < maxAttempts) {
            attempts++;

            Thread.sleep(pollIntervalMs);

            finalPhase = jobUpdater.getPhase(jobId);
            log.debug("Poll attempt " + attempts + ": Job " + jobId + " phase = " + finalPhase);

            isTerminal = JobPhaseManager.isTerminal(jobId, jobUpdater);
        }

        if (!isTerminal) {
            log.warn("Job " + jobId + " did not reach terminal state after " + attempts + " polling attempts");
            // We want to return a 503 if the job is still in a transient state and the
            // polling exceeded the max attempts
            // The retry-after header is set to 5 times the poll interval. No idea if 
            // this is the right value, but we can revisit it later.
            throw new JobServiceUnavailableException("Job " + jobId + " is still processing. Please retry later.",
                    pollIntervalMs * 5);
        }

        return finalPhase;
    }

    /**
     * Custom exception for service unavailable errors.
     * 
     */
    public static class JobServiceUnavailableException extends Exception {
        private final int retryAfterMs;

        public JobServiceUnavailableException(String message, int retryAfterMs) {
            super(message);
            this.retryAfterMs = retryAfterMs;
        }

        public int getRetryAfterMs() {
            return retryAfterMs;
        }
    }

    /**
     * Stream error document directly.
     * 
     * @param conn        The HTTP connection to the error document
     * @param syncOutput  The synchronous output
     * @param contentType The content type of the error document
     * @throws IOException If an I/O error occurs
     */
    private void streamErrorDocument(HttpURLConnection conn, SyncOutput syncOutput, String contentType)
            throws IOException {
        syncOutput.setCode(400);
        syncOutput.setHeader("Content-Type", contentType);

        InputStream inputStream = conn.getInputStream();
        OutputStream outputStream = syncOutput.getOutputStream();

        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
        }

        inputStream.close();
        outputStream.flush();
    }

    /**
     * Convert error content to VOTable format.
     * 
     * @param conn       The HTTP connection to the error document
     * @param syncOutput The synchronous output
     * @throws IOException If an I/O error occurs
     */
    private void convertToErrorVOTable(HttpURLConnection conn, SyncOutput syncOutput)
            throws IOException {
        syncOutput.setCode(400);
        syncOutput.setHeader("Content-Type", "application/x-votable+xml");

        InputStream inputStream = conn.getInputStream();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            baos.write(buffer, 0, bytesRead);
        }

        inputStream.close();

        String errorContent = baos.toString("UTF-8");
        String votable = VOTableUtil.generateErrorVOTable(errorContent);
        syncOutput.getOutputStream().write(votable.getBytes());
    }

    /**
     * Handle aborted job.
     * 
     * @param syncOutput The synchronous output
     * @throws IOException If an I/O error occurs
     */
    private void handleAborted(SyncOutput syncOutput) throws IOException {
        syncOutput.setCode(400);
        syncOutput.setHeader("Content-Type", "application/x-votable+xml");
        String message = VOTableUtil.generateErrorVOTable("Job was aborted before completion");
        syncOutput.getOutputStream().write(message.getBytes());
    }

    /**
     * Handle unexpected job phase.
     * 
     * @param syncOutput The synchronous output
     * @param phase      The unexpected phase
     * @throws IOException If an I/O error occurs
     */
    private void handleUnexpectedPhase(SyncOutput syncOutput, ExecutionPhase phase) throws IOException {
        syncOutput.setCode(500);
        syncOutput.setHeader("Content-Type", "text/plain");
        String message = "Job execution failed: unexpected job phase " + phase;
        syncOutput.getOutputStream().write(message.getBytes());
    }

    /**
     * Method to handle various Error codes.
     * 
     * @param syncOutput   The synchronous output
     * @param statusCode   The HTTP status code to set
     * @param errorMessage The error message
     */
    private void handleError(SyncOutput syncOutput, int statusCode, String errorMessage) {
        try {
            syncOutput.setCode(statusCode);

            if (statusCode >= 400 && statusCode < 500) {
                syncOutput.setHeader("Content-Type", "application/x-votable+xml");
                String message = VOTableUtil.generateErrorVOTable(errorMessage);
                syncOutput.getOutputStream().write(message.getBytes());
            } else {
                // Using plain text for Server errors, not sure if we should also return
                // VOTables here?
                syncOutput.setHeader("Content-Type", "text/plain");
                syncOutput.getOutputStream().write(errorMessage.getBytes());
            }
        } catch (IOException e) {
            log.error("Failed to write error message to output stream", e);
        }
    }
}
