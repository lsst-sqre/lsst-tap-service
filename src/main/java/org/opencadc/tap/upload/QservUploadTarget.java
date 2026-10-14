package org.opencadc.tap.upload;

import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.tap.upload.UploadTable;
import com.google.cloud.storage.HttpMethod;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.log4j.Logger;
import org.opencadc.tap.storage.StorageUtils;
import org.opencadc.tap.util.DatabaseNameUtil;

/**
 * Upload target for Qserv. Each job gets its own database, named after the
 * user and job, with MySQL column types. The worker fetches the files through
 * signed URLs, since it has no access to the bucket itself.
 *
 * @author stvoutsin
 */
public class QservUploadTarget implements UploadTarget {

    private static final Logger log = Logger.getLogger(QservUploadTarget.class);

    /**
     * Expiration time for signed URLs in hours.
     */
    private static final long URL_EXPIRATION_HOURS = 24;

    @Override
    public String getDatabaseTableName(UploadTable uploadTable) {
        return DatabaseNameUtil.getDatabaseTableName(uploadTable);
    }

    @Override
    public String getColumnType(VOTableField field) {
        String datatype = field.getDatatype().toLowerCase();
        String typeString = convertVOTableTypeToMySQL(datatype);

        if (field.getArraysize() != null && !field.getArraysize().trim().isEmpty()) {
            String arraysize = field.getArraysize().trim();
            if ("*".equals(arraysize)) {
                if ("char".equals(datatype) || "unicodechar".equals(datatype)) {
                    typeString = "VARCHAR(255)";
                } else if ("bit".equals(datatype)) {
                    typeString = "BIT(64)";
                } else {
                    log.debug("Array type detected for " + datatype + " with arraysize=*, converting to TEXT");
                    typeString = "TEXT";
                }
            } else {
                if ("char".equals(datatype) || "unicodechar".equals(datatype)) {
                    typeString += "(" + arraysize + ")";
                } else if ("bit".equals(datatype)) {
                    typeString += "(" + arraysize + ")";
                } else {
                    log.debug("Array type detected for " + datatype + " with arraysize=" + arraysize
                            + ", converting to TEXT");
                    typeString = "TEXT";
                }
            }
        }
        return typeString;
    }

    @Override
    public URI getLocation(String filename) throws URISyntaxException {
        return new URI(StorageUtils.getUploadSignedUrl(filename, HttpMethod.GET, URL_EXPIRATION_HOURS));
    }

    /**
     * Convert VOTable data types to MySQL equivalents
     */
    private String convertVOTableTypeToMySQL(String voTableType) {
        switch (voTableType) {
            case "boolean":
                return "BOOLEAN";
            case "unsignedbyte":
                return "TINYINT UNSIGNED";
            case "short":
                return "SMALLINT";
            case "int":
                return "INT";
            case "long":
                return "BIGINT";
            case "float":
                return "FLOAT";
            case "double":
                return "DOUBLE";
            case "char":
            case "unicodechar":
                return "CHAR";
            case "bit":
                return "BIT";
            case "floatcomplex":
            case "doublecomplex":
                log.debug("Complex type " + voTableType + " not supported in MySQL, using VARCHAR");
                return "VARCHAR(255)";
            default:
                log.debug("Unknown VOTable type: " + voTableType + ", keeping as-is");
                return voTableType.toUpperCase();
        }
    }
}
