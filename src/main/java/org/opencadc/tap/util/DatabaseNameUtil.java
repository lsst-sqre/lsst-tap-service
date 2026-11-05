package org.opencadc.tap.util;

import java.nio.charset.StandardCharsets;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
     * Qserv rejects uploads where the database and table names exceed this.
     */
    public static final int MAX_QSERV_NAME_LENGTH = 56;

    /**
     * Longest database name we build before the username is replaced with a
     * hash.
     */
    public static final int MAX_DATABASE_NAME_LENGTH = 32;

    /**
     * Number of hex characters kept from the SHA-256 hash.
     */
    private static final int HASH_LENGTH = 8;

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
     * Constructs the db name with username and jobID. If the result is longer
     * than {@link #MAX_DATABASE_NAME_LENGTH}, the username is replaced with a
     * short hash.
     *
     * @param username The username (can be null)
     * @param jobId The job ID
     * @return the database name with sanitized components
     */
    public static String constructDatabaseName(String username, String jobId) {
        String sanitizedJobID = sanitizeName(jobId);

        if (username == null || username.trim().isEmpty()) {
            return "job_" + sanitizedJobID;
        }

        String name = "user_" + sanitizeName(username) + "_" + sanitizedJobID;
        if (name.length() > MAX_DATABASE_NAME_LENGTH) {
            name = "user_" + shortHash(username) + "_" + sanitizedJobID;
        }
        return name;
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
        return getDatabaseTableName(tableName, getUsername(), jobId);
    }

    /**
     * Constructs the full db table name from table name, username and jobID.
     * If the database and table names together are longer than
     * {@link #MAX_QSERV_NAME_LENGTH}, the table name is shortened and given a
     * hash.
     * 
     * @param tableName the table name
     * @param username the username (can be null)
     * @param jobId the job ID
     * @return the full db table name
     */
    public static String getDatabaseTableName(String tableName, String username, String jobId) {
        String databaseName = constructDatabaseName(username, jobId);
        return databaseName + "." + shortenTableName(databaseName, tableName);
    }

    /**
     * Shorten a table name so that it fits in Qserv together with its
     * database name.
     *
     * @param databaseName the database the table goes into
     * @param tableName the table name
     * @return the table name, shortened if needed
     */
    static String shortenTableName(String databaseName, String tableName) {
        int available = MAX_QSERV_NAME_LENGTH - databaseName.length();
        if (tableName.length() <= available) {
            return tableName;
        }

        int prefixLength = Math.max(1, available - HASH_LENGTH - 1);
        return tableName.substring(0, prefixLength) + "_" + shortHash(tableName);
    }

    /**
     * @param input the string to hash
     * @return the first {@link #HASH_LENGTH} hex characters of its SHA-256
     */
    static String shortHash(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < HASH_LENGTH / 2; i++) {
                sb.append(String.format("%02x", hash[i]));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
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
