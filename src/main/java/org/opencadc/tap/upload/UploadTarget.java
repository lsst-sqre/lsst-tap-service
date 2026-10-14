package org.opencadc.tap.upload;

import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.tap.upload.UploadTable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * The parts of a table upload that depend on the backend.
 *
 * @author stvoutsin
 */
public interface UploadTarget {

    /**
     * Name of the uploaded table in the backend. TAP_UPLOAD references in the
     * query are rewritten to this name, and it is sent to the worker as the
     * table to create.
     *
     * @param uploadTable the uploaded table
     * @return the backend table name
     */
    String getDatabaseTableName(UploadTable uploadTable);

    /**
     * Part of the backend table name that must be a valid ADQL table name.
     * cadc-tap checks it before the table is stored.
     *
     * @param databaseTableName name from {@link #getDatabaseTableName}
     * @return the name to validate
     */
    default String getValidatedTableName(String databaseTableName) {
        return databaseTableName;
    }

    /**
     * Backend column type for a VOTable field, as written to the schema file.
     *
     * @param field the VOTable field
     * @return the backend column type
     */
    String getColumnType(VOTableField field);

    /**
     * Location the worker reads an uploaded file from.
     *
     * @param filename name of the file in the upload bucket
     * @return the location to send to the worker
     * @throws URISyntaxException if the location is not a valid URI
     */
    URI getLocation(String filename) throws URISyntaxException;
}
