package org.opencadc.tap.kafka.util;

import org.apache.log4j.Logger;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.tap.QueryRunner;
import ca.nrc.cadc.tap.schema.TableDesc;

import org.opencadc.tap.impl.QServQueryRunner;
import org.opencadc.tap.impl.StorageUtils;
import org.opencadc.tap.kafka.models.JobRun.UploadTable;
import org.opencadc.tap.kafka.models.OutputFormat;
import org.opencadc.tap.kafka.models.JobRun;
import org.opencadc.tap.kafka.util.DatabaseNameUtil;
import org.opencadc.tap.kafka.services.CreateDeleteEvent;
import org.opencadc.tap.kafka.services.CreateJobEvent;
import org.opencadc.tap.impl.context.WebAppContext;

/**
 * Service class for Kafka job operations.
 * Handles generating and sending jobs to Kafka.
 * 
 * @author stvoutsin
 * 
 */
public class KafkaJobService {
    private static final Logger log = Logger.getLogger(KafkaJobService.class);

    /**
     * Default expiration time in minutes for job result URLs
     */
    private static final int DEFAULT_JOB_RESULT_EXPIRATION_MINUTES = 120;

    /**
     * Prepares and submits a job to Kafka for execution.
     * 
     * @param job                   The job to execute
     * @param jobRunner             The job runner instance
     * @param createJobEventService Kafka job creation service
     * @param databaseString        Database connection string
     * @param bucketURL             Storage bucket URL
     * @param bucket                Storage bucket name
     * @param jobUpdater            JobUpdater implementation
     * @return true if submission was successful, false otherwise
     * @throws JobNotFoundException    If the job is not found
     * @throws JobPersistenceException If there's an error accessing job data
     */
    public static boolean prepareAndSubmitJob(Job job,
            JobRunner jobRunner,
            CreateJobEvent createJobEventService,
            String databaseString,
            String bucketURL,
            String bucket,
            JobUpdater jobUpdater)
            throws JobNotFoundException, JobPersistenceException {

        String jobId = job.getID();
        log.debug("Preparing to send job to Kafka: " + jobId);

        try {
            if (createJobEventService == null) {
                Object service = WebAppContext.getContextAttribute("jobProducer");
                if (service != null && service instanceof CreateJobEvent) {
                    createJobEventService = (CreateJobEvent) service;
                } else {
                    throw new RuntimeException("CreateJobEvent service not available");
                }
            }

            JobSubmissionInfo jobInfo = extractJobInfo(job, jobRunner, bucketURL, bucket);

            // Temp fix
            // If there is a table upload, use the username as the database
            if (!jobInfo.uploadTables.isEmpty()) {
                String username = DatabaseNameUtil.getUsername();
                databaseString = DatabaseNameUtil.constructDatabaseName(username, jobId);
            }

            // Submit job to Kafka
            String eventJobId = createJobEventService.submitQuery(
                    jobInfo.sql,
                    jobId,
                    jobInfo.resultDestination,
                    jobInfo.resultLocation,
                    jobInfo.resultFormat,
                    jobInfo.ownerID,
                    databaseString,
                    jobInfo.maxrec,
                    jobInfo.uploadTables);

            log.debug("Job sent to Kafka successfully with event ID: " + eventJobId);

            try {
                ExecutionPhase currentPhase = jobUpdater.getPhase(jobId);
                if (ExecutionPhase.HELD.equals(currentPhase)) {
                    boolean transitioned = JobPhaseManager.transitionJobPhase(
                            jobId, ExecutionPhase.HELD, ExecutionPhase.EXECUTING, jobUpdater);
                    if (!transitioned) {
                        log.warn("Failed to set job " + jobId + " to EXECUTING, phase may have changed");
                    }
                }
            } catch (Exception ex) {
                log.error("Failed to update job phase after Kafka submission: " + jobId, ex);
            }

            return true;
        } catch (Exception e) {
            log.error("Failed to send job to Kafka: " + jobId, e);
            try {
                JobPhaseManager.setErrorPhase(
                        jobId,
                        "Failed to send job to Kafka: " + e.getMessage(),
                        ErrorType.FATAL,
                        jobUpdater);
            } catch (Exception ex) {
                log.error("Failed to set job " + jobId + " to ERROR state", ex);
            }
            return false;
        }
    }

    /**
     * Submits a job deletion request to Kafka.
     * 
     * @param job                      The job to delete
     * @param createDeleteEventService Kafka delete event service
     * @param jobUpdater               JobUpdater implementation
     * @param jobPersistence           JobPersistence implementation
     * @return true if deletion request was successful, false otherwise
     * @throws JobNotFoundException    If the job is not found
     * @throws JobPersistenceException If there's an error accessing job data
     */
    public static boolean submitJobDeletion(Job job,
            CreateDeleteEvent createDeleteEventService,
            JobUpdater jobUpdater,
            JobPersistence jobPersistence)
            throws JobNotFoundException, JobPersistenceException {

        if (job.getID() == null || job.getID().trim().isEmpty()) {
            throw new IllegalArgumentException("Job ID cannot be null or empty");
        }

        log.debug("Preparing to send job deletion request to Kafka for jobId: " + job.getID());

        jobPersistence.getDetails(job);
        String executionId = null;
        String jobId = job.getID();
        
        if (job.getResultsList() != null) {
            for (Result result : job.getResultsList()) {
                if (result.getName().equals("executionId")) {
                    executionId = result.getURI().toString();
                }
            }
        }

        try {
            if (executionId == null || executionId.trim().isEmpty()) {
                log.warn("No executionId found for job " + jobId + ", skipping Kafka deletion request");
                if (jobId != null && !jobId.trim().isEmpty() && jobUpdater != null) {
                    try {
                        ExecutionPhase currentPhase = jobUpdater.getPhase(jobId);
                        if (ExecutionPhase.EXECUTING.equals(currentPhase)) {
                            boolean transitioned = JobPhaseManager.transitionJobPhase(
                                    jobId, ExecutionPhase.EXECUTING, ExecutionPhase.ABORTED, jobUpdater);
                            if (!transitioned) {
                                log.warn("Failed to set job " + jobId + " to ABORTED, phase may have changed");
                            }
                        }
                    } catch (Exception ex) {
                        log.error("Failed to update job phase after deletion request: " + jobId, ex);
                    }
                }
                return true;
            }

            if (createDeleteEventService == null) {
                Object service = WebAppContext.getContextAttribute("deleteProducer");
                if (service != null && service instanceof CreateDeleteEvent) {
                    createDeleteEventService = (CreateDeleteEvent) service;
                } else {
                    throw new RuntimeException("CreateDeleteEvent service not available");
                }
            }

            String ownerId = job.getOwnerID();

            // Update job phase if jobId and jobUpdater are provided
            // Do this before sending the deletion request so that the job phase is set to ABORTED
            // regardless of whether the deletion request succeeds or fails on the other side
            if (jobId != null && !jobId.trim().isEmpty() && jobUpdater != null) {
                try {
                    ExecutionPhase currentPhase = jobUpdater.getPhase(jobId);
                    if (ExecutionPhase.EXECUTING.equals(currentPhase)) {
                        boolean transitioned = JobPhaseManager.transitionJobPhase(
                                jobId, ExecutionPhase.EXECUTING, ExecutionPhase.ABORTED, jobUpdater);
                        if (!transitioned) {
                            log.warn("Failed to set job " + jobId + " to ABORTED, phase may have changed");
                        }
                    }
                } catch (Exception ex) {
                    log.error("Failed to update job phase after deletion request: " + jobId, ex);
                }
            }

            String deletedExecutionId = createDeleteEventService.submitDeletion(executionId, ownerId, jobId);
            log.debug("Job deletion request sent to Kafka successfully for executionId: " + deletedExecutionId);

            return true;
        } catch (Exception e) {
            log.error("Failed to send job deletion request to Kafka for jobId: " + jobId +
                    ", executionId: " + executionId, e);

            if (jobId != null && !jobId.trim().isEmpty() && jobUpdater != null) {
                try {
                    JobPhaseManager.setErrorPhase(
                            jobId,
                            "Failed to send job deletion request to Kafka: " + e.getMessage(),
                            ErrorType.FATAL,
                            jobUpdater);
                } catch (Exception ex) {
                    log.error("Failed to set job " + jobId + " to ERROR state", ex);
                }
            }

            return false;
        }
    }

    /**
     * Extract job information from the job runner.
     * 
     * @param job       The job
     * @param jobRunner The job runner instance
     * @param bucketURL Storage bucket URL
     * @param bucket    Storage bucket name
     * @return JobSubmissionInfo containing extracted job information
     */
    private static JobSubmissionInfo extractJobInfo(Job job, JobRunner jobRunner, String bucketURL, String bucket) {

        JobSubmissionInfo info = new JobSubmissionInfo();
        info.ownerID = job.getOwnerID();

        if (jobRunner instanceof QServQueryRunner) {
            QServQueryRunner qRunner = (QServQueryRunner) jobRunner;
            QueryRunner queryRunner = (QueryRunner) jobRunner;
            info.sql = qRunner.internalSQL;
            OutputFormat format = extractFormat(job);

            info.resultDestination = StorageUtils.generateJobResultSignedUrl(
                    job.getID(), format.getMimeType(), DEFAULT_JOB_RESULT_EXPIRATION_MINUTES, format);
            info.resultLocation = StorageUtils.generateResultLocation(job.getID(), format);
            info.resultFormat = VOTableUtil.createResultFormat(job.getID(), queryRunner, format);
            info.maxrec = queryRunner.maxRows;

            try {

                if (!queryRunner.uploadTableLocations.isEmpty()) {
                    log.debug("Found " + queryRunner.uploadTableLocations.size() + " upload table locations");
                    for (Map.Entry<String, TableDesc.TableLocationInfo> entry : queryRunner.uploadTableLocations
                            .entrySet()) {
                        String tableName = entry.getKey();
                        log.debug("Table name in KafkaJobService: " + tableName);
                        TableDesc.TableLocationInfo locationInfo = entry.getValue();
                        String sourceUrl = locationInfo.map.get("data").toString();
                        String schemaUrl = locationInfo.map.get("schema").toString();
                        UploadTable uploadTable = new UploadTable(tableName, sourceUrl, schemaUrl);
                        info.uploadTables.add(uploadTable);
                    }
                }
            } catch (Exception e) {
                log.error("Error parsing upload parameter: " + e.getMessage(), e);
            }
        }

        return info;
    }
    
    /**
     * Extract the desired output format from job parameters.
     * 
     * @param job The job
     * @return The output format string, or null if not specified
     */
    private static OutputFormat extractFormat(Job job) {
        List<Parameter> params = job.getParameterList();
        if (params != null) {
            for (Parameter param : params) {
                if ("RESPONSEFORMAT".equalsIgnoreCase(param.getName()) ||
                    "FORMAT".equalsIgnoreCase(param.getName())) {
                    return OutputFormat.fromString(param.getValue());
                }
            }
        }
        return OutputFormat.VOTABLE;
    }

    /**
     * Class to hold job submission information.
     */
    private static class JobSubmissionInfo {
        String sql = "";
        String resultDestination = "";
        String resultLocation = "";
        JobRun.ResultFormat resultFormat = null;
        String ownerID = "";
        Integer maxrec = null;
        List<UploadTable> uploadTables = new ArrayList<>();
    }

    /**
     * Get the username of the caller.
     * 
     * @return the username of the caller
     */
    protected static String getUsername() {
        // Note: This should probably be a utility method somewhere unless it already is
        AccessControlContext acContext = AccessController.getContext();
        Subject caller = Subject.getSubject(acContext);
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(caller);
        String username;

        if ((authMethod != null) && (authMethod != AuthMethod.ANON)) {
            final Set<HttpPrincipal> curPrincipals = caller.getPrincipals(HttpPrincipal.class);
            final HttpPrincipal[] principalArray = new HttpPrincipal[curPrincipals.size()];
            username = ((HttpPrincipal[]) curPrincipals.toArray(principalArray))[0].getName();
        } else {
            username = null;
        }

        return username;
    }
}
