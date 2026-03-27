package org.opencadc.tap.query;

import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.parser.PgsphereDeParser;
import ca.nrc.cadc.tap.parser.converter.TableNameConverter;
import ca.nrc.cadc.tap.parser.converter.TableNameReferenceConverter;
import ca.nrc.cadc.tap.parser.converter.TopConverter;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;

/**
 * Base class for the Rubin ADQL query implementations that target a
 * pg_sphere-compatible SQL dialect (PostgreSQL and QServ). It handles the parts
 * common to both: the pg_sphere deparser, TOP-&gt;LIMIT conversion and the
 * TAP-1.1 tap_schema table-name mapping. Subclasses supply the backend-specific
 * region converter and any extra table-name mappings.
 */
public abstract class AbstractRubinAdqlQuery extends AdqlQuery {

    protected AbstractRubinAdqlQuery() {
        super();
        setDeparserImpl(PgsphereDeParser.class);
    }

    @Override
    protected void init() {
        super.init();

        // pg_sphere / QServ use LIMIT instead of TOP
        navigatorList.add(new TopConverter(new ExpressionNavigator(), new ReferenceNavigator(),
                new FromItemNavigator()));

        TableNameConverter tnc = newTapSchema11Converter();
        configureTableNameConverter(tnc);
        TableNameReferenceConverter tnrc = new TableNameReferenceConverter(tnc.map);

        navigatorList.add(new SelectNavigator(new ExpressionNavigator(), tnrc, tnc));
        navigatorList.add(createRegionConverter());
    }

    /**
     * Create a {@link TableNameConverter} pre-populated with the TAP-1.1
     * tap_schema mappings (schemas-&gt;schemas11, etc). Also used by the BigQuery
     * implementation, which does not extend this class.
     *
     * @return a new converter with the tap_schema11 mappings
     */
    public static TableNameConverter newTapSchema11Converter() {
        TableNameConverter tnc = new TableNameConverter(true);
        tnc.put("tap_schema.schemas", "tap_schema.schemas11");
        tnc.put("tap_schema.tables", "tap_schema.tables11");
        tnc.put("tap_schema.columns", "tap_schema.columns11");
        tnc.put("tap_schema.keys", "tap_schema.keys11");
        tnc.put("tap_schema.key_columns", "tap_schema.key_columns11");
        return tnc;
    }

    /**
     * Hook for subclasses to add backend-specific table-name mappings to the
     * shared converter. The default implementation is a no-op.
     *
     * @param tnc the converter to add mappings to
     */
    protected void configureTableNameConverter(TableNameConverter tnc) {
        // no-op by default
    }

    /**
     * @return the backend-specific ADQL region-predicate converter to append to
     *         the navigator list
     */
    protected abstract SelectNavigator createRegionConverter();
}
