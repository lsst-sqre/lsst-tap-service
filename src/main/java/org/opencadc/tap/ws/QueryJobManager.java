
package org.opencadc.tap.ws;


import org.opencadc.tap.runner.KafkaQueryRunner;
import org.opencadc.tap.runner.PostgresQueryRunner;
import org.opencadc.tap.kafka.executor.KafkaJobExecutorFactory;

import ca.nrc.cadc.uws.server.JobExecutor;
import ca.nrc.cadc.uws.server.ThreadPoolExecutor;
import ca.nrc.cadc.uws.server.impl.PostgresJobPersistence;
import ca.nrc.cadc.uws.server.SimpleJobManager;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.RandomStringGenerator;


/**
 * @author pdowler
 */
public class QueryJobManager extends SimpleJobManager {
    private static final long MAX_EXEC_DURATION = 4 * 3600L;    // 4 hours to dump a catalog to vpsace
    private static final long MAX_DESTRUCTION = 7 * 24 * 60 * 60L; // 1 week
    private static final long MAX_QUOTE = 24 * 3600L;         // 24 hours since we have a threadpool with
    // queued jobs

    private static final int PG_THREAD_POOL_SIZE = 6;

    public QueryJobManager() {
        super();

        IdentityManager im = AuthenticationUtil.getIdentityManager();
        // persist UWS jobs to PostgreSQL using default jdbc/uws connection pool
        JobPersistence jobPersist = new PostgresJobPersistence(new RandomStringGenerator(16), im, true);

        final JobExecutor jobExec;
        String backend = System.getenv("BACKEND");
        if ("pg".equals(backend)) {
            // Direct JDBC execution for PostgreSQL backend
            jobExec = new ThreadPoolExecutor(jobPersist, PostgresQueryRunner.class, PG_THREAD_POOL_SIZE);
        } else {
            // Kafka-based async execution for QServ and BigQuery backends
            jobExec = KafkaJobExecutorFactory.createExecutor(jobPersist, KafkaQueryRunner.class, jobPersist);
        }

        super.setJobPersistence(jobPersist);
        super.setJobExecutor(jobExec);
        super.setMaxExecDuration(MAX_EXEC_DURATION);
        super.setMaxDestruction(MAX_DESTRUCTION);
        super.setMaxQuote(MAX_QUOTE);
    }
}
