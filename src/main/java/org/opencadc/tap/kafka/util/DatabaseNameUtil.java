package org.opencadc.tap.kafka.util;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.Set;
import javax.security.auth.Subject;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.tap.upload.UploadTable;

/**
 * Utility class for database and table name construction.
 * 
 * @author stvoutsin
 */
public class DatabaseNameUtil {

    /**
     * Get the username.
     * 
     * @return the username of the caller, or null
     */
    public static String getUsername() {
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

    /**
     * Sanitize a string by replacing dashes with underscores.
     * 
     * @param input the input string
     * @return sanitized string
     */
    public static String sanitizeName(String input) {
        return input != null ? input.replace("-", "_") : "";
    }

    /**
     * Constructs the db name with username and jobID.
     * 
     * @param username The username (can be null)
     * @param jobId The job ID
     * @return the database name with sanitized components
     */
    public static String constructDatabaseName(String username, String jobId) {
        StringBuilder sb = new StringBuilder();
        
        String sanitizedJobID = sanitizeName(jobId);
        
        if (username != null && !username.trim().isEmpty()) {
            String sanitizedUsername = sanitizeName(username);
            sb.append("user_").append(sanitizedUsername).append("_").append(sanitizedJobID);
        } else {
            sb.append("job_").append(sanitizedJobID);
        }
        
        return sb.toString();
    }

    /**
     * Constructs the full db table name from schema, upload table name, and jobID.
     * 
     * @param uploadTable the upload table containing table name and job ID
     * @return the full db table name
     */
    public static String getDatabaseTableName(UploadTable uploadTable) {
        return getDatabaseTableName(uploadTable.tableName, uploadTable.jobID);
    }

    /**
     * Constructs the full db table name from table name and jobID.
     * 
     * @param tableName the table name
     * @param jobId the job ID
     * @return the full db table name
     */
    public static String getDatabaseTableName(String tableName, String jobId) {
        StringBuilder sb = new StringBuilder();
        String username = getUsername();
        
        String sanitizedJobID = sanitizeName(jobId);
        
        if (username != null) {
            String sanitizedUsername = sanitizeName(username);
            sb.append("user_").append(sanitizedUsername).append("_").append(sanitizedJobID).append(".");
        } else {
            sb.append("job_").append(sanitizedJobID).append(".");
        }
        sb.append(tableName);
        sb.append("_");
        sb.append(sanitizedJobID);
        return sb.toString();
    }

    /**
     * Format table name to include username and jobID.
     * 
     * @param originalTableName The original table name
     * @param username          The username (can be null)
     * @param jobId            The job ID
     * @return Formatted table name with database prefix
     */
    public static String formatTableName(String originalTableName, String username, String jobId) {
        if (username == null || username.isEmpty()) {
            username = "anonymous";
        }
        
        String databaseName = constructDatabaseName(username, jobId);
        String sanitizedJobID = sanitizeName(jobId);
        
        return databaseName + "." + originalTableName + "_" + sanitizedJobID;
    }
}