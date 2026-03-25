package org.opencadc.tap.dialect.pg;

import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.parser.PgsphereDeParser;
import ca.nrc.cadc.tap.parser.converter.TableNameConverter;
import ca.nrc.cadc.tap.parser.converter.TableNameReferenceConverter;
import ca.nrc.cadc.tap.parser.converter.TopConverter;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import org.apache.log4j.Logger;
import org.opencadc.tap.dialect.pg.parser.converter.ObsCoreRegionConverter;

/**
 * ADQL query implementation for PostgreSQL (pg_sphere) backend.
 */
public class PostgresAdqlQueryImpl extends AdqlQuery {
    private static Logger log = Logger.getLogger(PostgresAdqlQueryImpl.class);

    public PostgresAdqlQueryImpl() {
        super();
        setDeparserImpl(PgsphereDeParser.class);
    }

    @Override
    protected void init() {
        super.init();

        // PostgreSQL uses LIMIT instead of TOP
        super.navigatorList.add(new TopConverter(new ExpressionNavigator(), new ReferenceNavigator(), new FromItemNavigator()));

        // TAP-1.1 tap_schema version is encoded in table names
        TableNameConverter tnc = new TableNameConverter(true);
        tnc.put("tap_schema.schemas", "tap_schema.schemas11");
        tnc.put("tap_schema.tables", "tap_schema.tables11");
        tnc.put("tap_schema.columns", "tap_schema.columns11");
        tnc.put("tap_schema.keys", "tap_schema.keys11");
        tnc.put("tap_schema.key_columns", "tap_schema.key_columns11");
        TableNameReferenceConverter tnrc = new TableNameReferenceConverter(tnc.map);
        super.navigatorList.add(new SelectNavigator(new ExpressionNavigator(), tnrc, tnc));
        super.navigatorList.add(new ObsCoreRegionConverter());
    }
}
