package org.opencadc.tap.upload;

import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.tap.upload.UploadTable;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.log4j.Logger;
import org.opencadc.tap.config.TapConfig;
import org.opencadc.tap.storage.StorageUtils;
import org.opencadc.tap.util.DatabaseNameUtil;

/**
 * Upload target for BigQuery. All uploaded tables go into one shared dataset,
 * so the table name carries the user and job.
 *
 * <p>The upload dataset may be in a different project from the queried
 * data  so table names are fully qualified and quoted.
 *
 * @author stvoutsin
 */
public class BigQueryUploadTarget implements UploadTarget {

    private static final Logger log = Logger.getLogger(BigQueryUploadTarget.class);

    private final String project;
    private final String dataset;

    public BigQueryUploadTarget() {
        this(TapConfig.bigQueryUploadProject(), TapConfig.bigQueryUploadDataset());
    }

    /**
     * @param project project holding the upload dataset, or null if uploads
     *                are not supported
     * @param dataset dataset that uploaded tables are loaded into
     */
    public BigQueryUploadTarget(String project, String dataset) {
        this.project = project;
        this.dataset = dataset;
    }

    /**
     * @return the fully qualified table name, e.g.
     *         {@code `project.tap_uploads.user_name_jobid_table`}
     * @throws IllegalArgumentException if uploads are not configured
     */
    @Override
    public String getDatabaseTableName(UploadTable uploadTable) {
        if (project == null || dataset == null) {
            throw new IllegalArgumentException("Table upload is not supported by this service");
        }

        String username = DatabaseNameUtil.getUsername();
        if (username == null || username.trim().isEmpty()) {
            username = "anonymous";
        }
        String prefix = DatabaseNameUtil.constructDatabaseName(username, uploadTable.jobID);
        return "`" + project + "." + dataset + "." + prefix + "_" + uploadTable.tableName + "`";
    }

    /**
     * The project and dataset come from our configuration and the project may
     * contain dashes, so only the table part is checked.
     */
    @Override
    public String getValidatedTableName(String databaseTableName) {
        String name = databaseTableName.replace("`", "");
        return name.substring(name.lastIndexOf('.') + 1);
    }

    @Override
    public String getColumnType(VOTableField field) {
        String datatype = field.getDatatype().toLowerCase();
        String arraysize = field.getArraysize();
        boolean isArray = arraysize != null && !arraysize.trim().isEmpty();

        switch (datatype) {
            case "char":
            case "unicodechar":
                if ("timestamp".equalsIgnoreCase(field.xtype)) {
                    return "TIMESTAMP";
                }
                return "STRING";
            case "boolean":
                return isArray ? "STRING" : "BOOL";
            case "bit":
            case "unsignedbyte":
            case "short":
            case "int":
            case "long":
                return isArray ? "STRING" : "INT64";
            case "float":
            case "double":
                // Arrays include the geometry xtypes (point, circle, polygon).
                return isArray ? "STRING" : "FLOAT64";
            default:
                log.debug("VOTable type " + datatype + " has no BigQuery equivalent, using STRING");
                return "STRING";
        }
    }

    @Override
    public URI getLocation(String filename) throws URISyntaxException {
        return new URI(StorageUtils.getUploadGsUri(filename));
    }
}
