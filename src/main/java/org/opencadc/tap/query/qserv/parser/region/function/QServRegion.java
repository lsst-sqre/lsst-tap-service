package org.opencadc.tap.query.qserv.parser.region.function;

import net.sf.jsqlparser.expression.Expression;

/**
 * QServRegion interface
 * 
 */
public interface QServRegion {
    public Expression pointInRegion(QServPoint point);
}
