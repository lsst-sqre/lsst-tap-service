package org.opencadc.tap.query.pg;

import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import org.opencadc.tap.query.AbstractRubinAdqlQuery;
import org.opencadc.tap.query.pg.parser.converter.ObsCoreRegionConverter;

/**
 * ADQL query implementation for the PostgreSQL (pg_sphere) backend.
 */
public class PostgresAdqlQuery extends AbstractRubinAdqlQuery {

    public PostgresAdqlQuery() {
        super();
    }

    @Override
    protected SelectNavigator createRegionConverter() {
        return new ObsCoreRegionConverter();
    }
}
