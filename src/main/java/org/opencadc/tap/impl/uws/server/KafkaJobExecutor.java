package org.opencadc.tap.impl.uws.server;

import org.apache.log4j.Logger;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.Date;
import javax.security.auth.Subject;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.server.JobExecutor;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobPhaseException;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;

import org.opencadc.tap.impl.util.JobPhaseManager;
import org.opencadc.tap.impl.util.JobPollingService;
import org.opencadc.tap.impl.util.KafkaJobService;
import org.opencadc.tap.kafka.services.CreateDeleteEvent;
import org.opencadc.tap.kafka.services.CreateJobEvent;

/**
 * JobExecutor implementation that sends jobs to Kafka.
 *
 * This executor handles the job state transitions but delegates the
 * execution to the workers that consume from the queue.
 * 
 * @author stvoutsin
 */
public class KafkaJobExecutor implements JobExecutor {

    private static final Logger log = Logger.getLogger(KafkaJobExecutor.class);

    private JobUpdater jobUpdater;
    private Class jobRunnerClass;
    private String appName;
    private CreateJobEvent createJobEventService;
    private CreateDeleteEvent deleteJobEventService;
    private String bucketURL;
    private String bucket;
    private String databaseString;
    private JobPersistence jobPersistence;
    private JobPollingService jobPollingService;

    /**
     * KafkaJobExecutor Constructor.
     *
     * @param jobUpdater            JobUpdater implementation
     * @param jobRunnerClass        JobRunner implementation class
     * @param createJobEventService Kafka job Creation service
     * @param deleteJobEventService Kafka job Deletion service
     * @param bucketURL             Storage bucket URL
     * @param bucket                Storage bucket name
     * @param databaseString        Database connection string
     */
    public KafkaJobExecutor(JobUpdater jobUpdater, Class jobRunnerClass, JobPersistence jobPersistence,
            CreateJobEvent createJobEventService,
            CreateDeleteEvent deleteJobEventService,
            String bucketURL, String bucket, String databaseString) {
        this.jobUpdater = jobUpdater;
        this.jobRunnerClass = jobRunnerClass;
        this.createJobEventService = createJobEventService;
        this.deleteJobEventService = deleteJobEventService;
        this.bucketURL = bucketURL;
        this.bucket = bucket;
        this.databaseString = databaseString;
        this.jobPersistence = jobPersistence;
        this.jobPollingService = new JobPollingService(jobUpdater, jobPersistence, bucket);
        log.debug("KafkaJobExecutor created with jobRunnerClass: " + jobRunnerClass.getName());
    }

    @Override
    public void setAppName(String appName) {
        this.appName = appName;
    }

    @Override
    public void terminate() throws InterruptedException {
        log.debug("KafkaJobExecutor terminated");
    }

    /**
     * Execute async job. This method transitions the job to QUEUED state,
     * initializes it and then runs the JobRunner.
     * 
     * It finally sends it to Kafka for execution if in HELD state.
     *
     * @param job the job to execute
     */
    @Override
    public void execute(Job job)
            throws JobNotFoundException, JobPersistenceException,
            JobPhaseException, TransientException {
        if (job == null) {
            throw new IllegalArgumentException("job cannot be null");
        }

        AccessControlContext acContext = AccessController.getContext();
        Subject caller = Subject.getSubject(acContext);
        log.info("Starting execution of job: " + job.getID());

        try {
            JobRunner jobRunner = createJobRunner();
            jobRunner.setJob(job);
            jobRunner.setJobUpdater(jobUpdater);

            Date now = new Date();
            ExecutionPhase current = jobUpdater.getPhase(job.getID());
            if (!ExecutionPhase.PENDING.equals(current)) {
                log.warn("Cannot execute job " + job.getID() + ": unexpected phase: " + current);
                return;
            }

            // Set to QUEUED
            boolean transitioned = JobPhaseManager.transitionJobPhase(
                    job.getID(), ExecutionPhase.PENDING, ExecutionPhase.QUEUED, jobUpdater);
            if (!transitioned) {
                log.warn("Failed to set job " + job.getID() + " to QUEUED, phase may have changed");
                return;
            }

            log.debug("Running job runner: " + job.getID());
            if (caller != null) {
                Subject.doAs(caller, new RunnableAction(jobRunner));
            } else {
                jobRunner.run();
            }

            ExecutionPhase updatedPhase = jobUpdater.getPhase(job.getID());
            log.debug("Current job phase after preparation: " + updatedPhase);

            if (ExecutionPhase.HELD.equals(updatedPhase)) {
                log.debug("Job " + job.getID() + " is in HELD state, sending to Kafka");
                KafkaJobService.prepareAndSubmitJob(
                        job, jobRunner, createJobEventService, databaseString, bucketURL, bucket, jobUpdater);
            } else if (ExecutionPhase.COMPLETED.equals(updatedPhase) ||
                    ExecutionPhase.ERROR.equals(updatedPhase)) {
                log.debug("Job " + job.getID() + " already in terminal state: " + updatedPhase);
            } else {
                log.warn("Job " + job.getID() + " in unexpected phase: " + updatedPhase);
            }
        } catch (Exception ex) {
            log.error("Failed to execute job: " + job.getID(), ex);
            try {
                JobPhaseManager.setErrorPhase(
                        job.getID(),
                        ex.getMessage(),
                        ErrorType.FATAL,
                        jobUpdater);
            } catch (Exception e) {
                log.error("Failed to set job " + job.getID() + " to ERROR state", e);
            }
            throw new JobPersistenceException("Failed to execute job: " + ex.getMessage());
        }
    }

    @Override
    public void execute(Job job, SyncOutput syncOutput)
            throws JobNotFoundException, JobPersistenceException,
            JobPhaseException, TransientException {
        if (job == null) {
            throw new IllegalArgumentException("job cannot be null");
        }
        if (syncOutput == null) {
            throw new IllegalArgumentException("syncOutput cannot be null");
        }

        log.info("Starting synchronous execution of job: " + job.getID());

        AccessControlContext acContext = AccessController.getContext();
        Subject caller = Subject.getSubject(acContext);

        try {
            JobRunner jobRunner = createJobRunner();
            jobRunner.setJob(job);
            jobRunner.setJobUpdater(jobUpdater);

            try {
                java.lang.reflect.Method setSyncOutput = jobRunnerClass.getMethod("setSyncOutput", SyncOutput.class);
                setSyncOutput.invoke(jobRunner, syncOutput);
                log.debug("SyncOutput set on JobRunner");
            } catch (NoSuchMethodException e) {
                log.warn("JobRunner " + jobRunnerClass.getName() +
                        " does not have setSyncOutput method. Synchronous output may not work correctly.");
            } catch (Exception e) {
                log.error("Failed to set SyncOutput on JobRunner", e);
            }

            JobPhaseManager.transitionJobPhase(
                    job.getID(), ExecutionPhase.PENDING, ExecutionPhase.QUEUED, jobUpdater);

            log.debug("Running job runner to prepare the job: " + job.getID());
            if (caller != null) {
                Subject.doAs(caller, new RunnableAction(jobRunner));
            } else {
                jobRunner.run();
            }

            ExecutionPhase currentPhase = jobUpdater.getPhase(job.getID());
            log.debug("Current job phase after preparation: " + currentPhase);

            if (ExecutionPhase.HELD.equals(currentPhase)) {
                log.debug("Job " + job.getID() + " is in HELD state, sending to Kafka");

                boolean submitted = KafkaJobService.prepareAndSubmitJob(
                        job, jobRunner, createJobEventService, databaseString, bucketURL, bucket, jobUpdater);

                if (!submitted) {
                    throw new TransientException("Failed to submit job to Kafka");
                }

                // Poll for job completion and handle results
                boolean handled = jobPollingService.pollAndHandleResults(job.getID(), syncOutput);

                if (!handled) {
                    log.warn("Failed to handle results for job: " + job.getID());
                }

            } else if (ExecutionPhase.COMPLETED.equals(currentPhase)) {
                log.info("Job " + job.getID() + " was completed directly by the JobRunner");
            } else if (ExecutionPhase.ERROR.equals(currentPhase)) {
                log.info("Job " + job.getID() + " failed with ERROR directly in the JobRunner");

                // Refresh job details from persistent store
                job = jobPersistence.get(job.getID());
                jobPersistence.getDetails(job);

                jobPollingService.pollAndHandleResults(job.getID(), syncOutput);
            } else {
                log.warn("Job " + job.getID() + " in unexpected phase after JobRunner: " + currentPhase);

                try {
                    syncOutput.setCode(500);
                    syncOutput.setHeader("Content-Type", "text/plain");
                    String message = "Job execution failed: unexpected job phase " + currentPhase;
                    syncOutput.getOutputStream().write(message.getBytes());
                } catch (Exception e) {
                    log.error("Failed to write error message to output stream", e);
                }
            }

            log.debug("Synchronous job execution completed: " + job.getID());

        } catch (Exception ex) {
            log.error("Failed to execute job: " + job.getID(), ex);

            try {
                JobPhaseManager.setErrorPhase(
                        job.getID(),
                        "Failed to execute job: " + ex.getMessage(),
                        ErrorType.FATAL,
                        jobUpdater);

                try {
                    syncOutput.setCode(500);
                    syncOutput.setHeader("Content-Type", "text/plain");
                    String message = "Job execution failed: " + ex.getMessage();
                    syncOutput.getOutputStream().write(message.getBytes());
                } catch (Exception e) {
                    log.error("Failed to write error message to output stream", e);
                }
            } catch (Exception e) {
                log.error("Failed to set job " + job.getID() + " to ERROR state", e);
            }

            if (ex instanceof JobPhaseException) {
                throw (JobPhaseException) ex;
            } else if (ex instanceof JobNotFoundException) {
                throw (JobNotFoundException) ex;
            } else if (ex instanceof TransientException) {
                throw (TransientException) ex;
            } else {
                throw new JobPersistenceException("Failed to execute job: " + ex.getMessage());
            }
        }
    }

    @Override
    public void abort(Job job)
            throws JobNotFoundException, JobPersistenceException,
            JobPhaseException, TransientException {
        if (job == null) {
            throw new IllegalArgumentException("job cannot be null");
        }

        log.info("Aborting job: " + job.getID());

        try {
            ExecutionPhase current = jobUpdater.getPhase(job.getID());

            if (!ExecutionPhase.ABORTED.equals(current)) {

                boolean transitioned = JobPhaseManager.transitionJobPhase(
                        job.getID(), current, ExecutionPhase.ABORTED, jobUpdater);

                if (!transitioned) {
                    log.warn("Failed to set job " + job.getID() + " to ABORTED, phase may have changed");
                    return;
                }

                if (deleteJobEventService != null) {
                    try {
                        boolean submitted = KafkaJobService.submitJobDeletion(
                                job.getID(), job.getOwnerID(), deleteJobEventService, jobUpdater, jobPersistence);
    
                        if (!submitted) {
                            throw new TransientException("Failed to submit job to Kafka");
                        }

                        log.debug("Abort event sent to Kafka for job: " + job.getID());
                    } catch (Exception e) {
                        log.error("Failed to send abort event to Kafka for job: " + job.getID(), e);
                    }
                }

                log.debug("Job aborted successfully: " + job.getID());
            } else {
                log.debug("Job " + job.getID() + " already in terminal state: " + current);
            }
        } catch (Exception ex) {
            log.error("Failed to abort job: " + job.getID(), ex);
            throw new JobPersistenceException("Failed to abort job: " + ex.getMessage());
        }
    }

    /**
     * Create a new JobRunner instance.
     * 
     * @return A new JobRunner instance
     * @throws Exception if the JobRunner cannot be created
     */
    private JobRunner createJobRunner() throws Exception {
        try {
            JobRunner jobRunner = (JobRunner) jobRunnerClass.newInstance();
            return jobRunner;
        } catch (Exception ex) {
            log.error("Failed to create JobRunner instance", ex);
            throw ex;
        }
    }

}