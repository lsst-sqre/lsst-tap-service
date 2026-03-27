
package org.opencadc.tap.ws;


import org.opencadc.tap.config.Backend;
import org.opencadc.tap.config.TapConfig;
import org.opencadc.tap.execution.kafka.KafkaQueryRunner;
import org.opencadc.tap.execution.direct.PostgresQueryRunner;
import org.opencadc.tap.execution.kafka.KafkaJobExecutorFactory;

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
    private static final long MAX_EXEC_DURATION = TapConfig.maxExecutionDurationSeconds();
    private static final long MAX_DESTRUCTION = TapConfig.maxDestructionSeconds();
    private static final long MAX_QUOTE = TapConfig.maxQuoteSeconds();
    private static final int PG_THREAD_POOL_SIZE = 6;

    public QueryJobManager() {
        super();

        IdentityManager im = AuthenticationUtil.getIdentityManager();
        // persist UWS jobs to PostgreSQL using default jdbc/uws connection pool
        JobPersistence jobPersist = new PostgresJobPersistence(new RandomStringGenerator(16), im, true);

        final JobExecutor jobExec;
        if (Backend.current().usesKafka()) {
            // Kafka-based async execution for QServ and BigQuery backends
            jobExec = KafkaJobExecutorFactory.createExecutor(jobPersist, KafkaQueryRunner.class, jobPersist);
        } else {
            // Direct JDBC execution for the PostgreSQL backend
            jobExec = new ThreadPoolExecutor(jobPersist, PostgresQueryRunner.class, PG_THREAD_POOL_SIZE);
        }

        super.setJobPersistence(jobPersist);
        super.setJobExecutor(jobExec);
        super.setMaxExecDuration(MAX_EXEC_DURATION);
        super.setMaxDestruction(MAX_DESTRUCTION);
        super.setMaxQuote(MAX_QUOTE);
    }
}
