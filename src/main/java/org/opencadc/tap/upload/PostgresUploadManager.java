package org.opencadc.tap.upload;

import ca.nrc.cadc.tap.BasicUploadManager;
import ca.nrc.cadc.tap.upload.UploadLimits;

/**
 * UploadManager for the PostgreSQL backend.
 *
 * <p>TAP_UPLOAD tables are materialised directly in the data database (through
 * the {@code jdbc/tapuser} pool) using the base {@link BasicUploadManager} JDBC
 * table creation, so the direct-execution query can JOIN against them.
 *
 * <p>The Kafka backends (QServ, BigQuery) instead use {@link RubinUploadManager},
 * which stages the upload to object storage for the external worker to ingest;
 * that strategy does not create a database table and so cannot be used for the
 * direct-execution path.
 */
public class PostgresUploadManager extends BasicUploadManager {

    /** File-size limit for the uploaded VOTable (32 MiB). */
    public static final UploadLimits MAX_UPLOAD = new UploadLimits(32 * 1024L * 1024L);

    public PostgresUploadManager() {
        super(MAX_UPLOAD);
    }
}
