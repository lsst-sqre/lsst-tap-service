package org.opencadc.tap.dialect.qserv.parser.converter;

import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.log4j.Logger;

/**
 * Converts schema prefixes in fully-qualified column references (e.g.
 * schema.table.column).
 */
public class SchemaNameReferenceConverter extends ReferenceNavigator {
    private static final Logger log = Logger.getLogger(SchemaNameReferenceConverter.class);

    private final Map<String, String> map;

    public SchemaNameReferenceConverter(Map<String, String> map) {
        this.map = map;
    }

    @Override
    public void visit(Column column) {
        Table table = column.getTable();
        if (table != null) {
            String schema = table.getSchemaName();
            if (schema != null) {
                String newSchema = map.get(schema);
                if (newSchema != null) {
                    table.setSchemaName(newSchema);
                }
            }
        }
    }
}
