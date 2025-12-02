package org.opencadc.tap.dialect.bigquery;

import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.parser.QuerySelectDeParser;
import ca.nrc.cadc.tap.parser.converter.TableNameConverter;
import ca.nrc.cadc.tap.parser.converter.TableNameReferenceConverter;
import ca.nrc.cadc.tap.parser.converter.TopConverter;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.apache.log4j.Logger;
import org.opencadc.tap.dialect.bigquery.expression.BigQueryExpressionDeParser;
import org.opencadc.tap.dialect.bigquery.parser.BigQueryQuerySelectDeParser;
import org.opencadc.tap.dialect.bigquery.parser.converter.BigQueryRegionConverter;

/**
 * ADQL Query implementation for BigQuery backend.
 *
 * Table mappings are loaded from bigquery-tables.properties on the classpath,
 * or can be set via system properties with prefix "tap.bigquery.table."
 *
 */
public class BigQueryAdqlQueryImpl extends AdqlQuery {
    private static final Logger log = Logger.getLogger(BigQueryAdqlQueryImpl.class);

    public BigQueryAdqlQueryImpl() {
        super();
    }

    @Override
    protected void init() {
        super.init();

        String project = System.getProperty("tap.bigquery.project");
        String dataset = System.getProperty("tap.bigquery.dataset");
        String schemaName = System.getProperty("tap.bigquery.schema", "ppdb");
        if (project == null || dataset == null) {
            throw new IllegalStateException(
                "tap.bigquery.project and tap.bigquery.dataset must be set");
        }

        log.info("BigQuery configuration: project=" + project + ", dataset=" + dataset + ", schema=" + schemaName);
        
        // TAP-1.1 tap_schema version is encoded in table names
        TableNameConverter tnc = new TableNameConverter(true);

        tnc.put("tap_schema.schemas", "tap_schema.schemas11");
        tnc.put("tap_schema.tables", "tap_schema.tables11");
        tnc.put("tap_schema.columns", "tap_schema.columns11");
        tnc.put("tap_schema.keys", "tap_schema.keys11");
        tnc.put("tap_schema.key_columns", "tap_schema.key_columns11");

        // BigQuery table mappings using configured project/dataset
        String prefix = String.format("`%s.%s.", project, dataset);

        tnc.put(schemaName + ".DiaObject", prefix + "DiaObject`");
        tnc.put(schemaName + ".DiaSource", prefix + "DiaSource`");
        tnc.put(schemaName + ".DiaForcedSource", prefix + "DiaForcedSource`");

        TableNameReferenceConverter tnrc = new TableNameReferenceConverter(tnc.map);

        navigatorList.add(new TopConverter(new ExpressionNavigator(), new ReferenceNavigator(), new FromItemNavigator()));
        navigatorList.add(new BigQueryRegionConverter(new ExpressionNavigator(), tnrc, tnc));
        navigatorList.add(new SelectNavigator(new ExpressionNavigator(), tnrc, tnc));

        // TODO: add more custom query visitors here
    }

    /**
     * Provide implementation of expression deparser if the default (BaseExpressionDeParser)
     * is not sufficient. For example, postgresql+pg_sphere requires the PgsphereDeParser to
     * support spoint and spoly. the default is to return a new BaseExpressionDeParser.
     *
     * @param dep The SelectDeParser used.
     * @param sb  StringBuffer to write to.
     * @return ExpressionDeParser implementation.  Never null.
     */
    @Override
    protected ExpressionDeParser getExpressionDeparser(SelectDeParser dep, StringBuffer sb) {
        return new BigQueryExpressionDeParser(dep, sb);
    }

    /**
     * Provide implementation of select deparser if the default (SelectDeParser) is not sufficient.
     *
     * @return QuerySelectDeParser
     */
    @Override
    protected QuerySelectDeParser getSelectDeParser() {
        return new BigQueryQuerySelectDeParser();
    }
}