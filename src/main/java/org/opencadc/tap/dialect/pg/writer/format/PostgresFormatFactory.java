package org.opencadc.tap.dialect.pg.writer.format;

import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.tap.TapSelectItem;
import ca.nrc.cadc.tap.writer.format.PostgreSQLFormatFactory;
import org.apache.log4j.Logger;
import org.opencadc.tap.impl.RubinURLFormat;

/**
 * Format factory for PostgreSQL backend. Delegates to RubinURLFormat for
 * access_url columns in oga.ObsCore.
 */
public class PostgresFormatFactory extends PostgreSQLFormatFactory {
    private static Logger log = Logger.getLogger(PostgresFormatFactory.class);

    public PostgresFormatFactory() {
        super();
    }

    @Override
    public Format<Object> getClobFormat(TapSelectItem columnDesc) {
        if (columnDesc != null) {
            if ("oga.ObsCore".equalsIgnoreCase(columnDesc.tableName)) {
                if ("access_url".equalsIgnoreCase(columnDesc.getColumnName())) {
                    return new RubinURLFormat();
                }
            }
        }
        return super.getClobFormat(columnDesc);
    }
}
