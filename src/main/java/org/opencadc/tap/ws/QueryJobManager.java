
package org.opencadc.tap.ws;


import ca.nrc.cadc.auth.X500IdentityManager;
import ca.nrc.cadc.tap.QServQueryRunner;
import ca.nrc.cadc.uws.server.JobExecutor;
import ca.nrc.cadc.uws.server.impl.PostgresJobPersistence;
import ca.nrc.cadc.uws.server.SimpleJobManager;
import ca.nrc.cadc.uws.server.ThreadPoolExecutor;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.uws.server.RequestPathJobManager;
import org.apache.log4j.Logger;


/**
 * @author pdowler
 */
public class QueryJobManager extends SimpleJobManager {
    private static final long MAX_EXEC_DURATION = 4 * 3600L;    // 4 hours to dump a catalog to vpsace
    private static final long MAX_DESTRUCTION = 7 * 24 * 60 * 60L; // 1 week
    private static final long MAX_QUOTE = 24 * 3600L;         // 24 hours since we have a threadpool with
    // queued jobs

    public QueryJobManager() {
        super();

        IdentityManager im = AuthenticationUtil.getIdentityManager();
        // persist UWS jobs to PostgreSQL using default jdbc/uws connection pool
        JobPersistence jobPersist = new PostgresJobPersistence(new RandomStringGenerator(16), im, true);

        // max threads: 6 == number of simultaneously running async queries (per
        // web server), plus sync queries, plus VOSI-tables queries
        final JobExecutor jobExec = new ThreadPoolExecutor(jobPersist, QServQueryRunner.class, 6);

        super.setJobPersistence(jobPersist);
        super.setJobExecutor(jobExec);
        super.setMaxExecDuration(MAX_EXEC_DURATION);
        super.setMaxDestruction(MAX_DESTRUCTION);
        super.setMaxQuote(MAX_QUOTE);
    }
}
