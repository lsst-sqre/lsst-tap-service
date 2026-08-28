package org.opencadc.tap.dialect.qserv.parser.converter;

import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.util.CaseInsensitiveStringComparator;
import java.util.Map;
import java.util.TreeMap;
import net.sf.jsqlparser.schema.Table;
import org.apache.log4j.Logger;

/**
 * Converter that rewrites schema prefixes in table references. Used to allow
 * users to query using a published schema name (e.g. dp1) while the actual
 * data is stored under a different name (e.g. dp1_pilot).
 */
public class SchemaNameConverter extends FromItemNavigator {
    private static final Logger log = Logger.getLogger(SchemaNameConverter.class);

    public final Map<String, String> map;

    public SchemaNameConverter(boolean ignoreCase) {
        if (ignoreCase)
            this.map = new TreeMap<>(new CaseInsensitiveStringComparator());
        else
            this.map = new TreeMap<>();
    }

    public SchemaNameConverter(Map<String, String> map) {
        this.map = map;
    }

    public void put(String fromSchema, String toSchema) {
        map.put(fromSchema, toSchema);
    }

    @Override
    public void visit(Table table) {
        String schema = table.getSchemaName();
        if (schema != null) {
            String newSchema = map.get(schema);
            if (newSchema != null) {
                table.setSchemaName(newSchema);
            }
        }
    }
}
