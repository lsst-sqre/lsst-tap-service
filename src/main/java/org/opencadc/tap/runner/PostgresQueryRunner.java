package org.opencadc.tap.runner;

import ca.nrc.cadc.tap.QueryRunner;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

/**
 * QueryRunner for the PostgreSQL backend. Executes queries directly via JDBC
 * (no Kafka), using jdbc/tapuser for the data connection and jdbc/tapschemauser
 * for the tap_schema connection.
 */
public class PostgresQueryRunner extends QueryRunner {

    private static final String TAPSCHEMAUSER_DATASOURCE_NAME = "jdbc/tapschemauser";

    public PostgresQueryRunner() {
        super(false);  // returnHELD=false: execute directly, no Kafka
    }

    @Override
    protected DataSource getTapSchemaDataSource() throws Exception {
        Context envContext = (Context) new InitialContext().lookup("java:comp/env");
        return (DataSource) envContext.lookup(TAPSCHEMAUSER_DATASOURCE_NAME);
    }
}
