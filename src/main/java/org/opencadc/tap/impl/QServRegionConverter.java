package org.opencadc.tap.impl;

import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.RegionFinder;

import net.sf.jsqlparser.expression.Expression;

import org.apache.log4j.Logger;

import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;

/**
 * This class implements the rewriting of all ADQL geometry constructs
 * as QServ specific geometry functions.  This extends the RegionFinder,
 * which by default for functions not overridden in this class throws
 * UnsupportedOperationException.
 *
 * @author cbanek
 */

public class QServRegionConverter extends RegionFinder
{
    private static Logger log = Logger.getLogger(QServRegionConverter.class);

    public QServRegionConverter(ExpressionNavigator en, ReferenceNavigator rn, FromItemNavigator fn)
    {
        super(en, rn, fn);
    }

    /**
     * This method is called when a CIRCLE geometry value is found.
     */
    @Override
    protected Expression handleCircle(Expression coordsys, Expression ra, Expression dec, Expression radius)
    {
        throw new UnsupportedOperationException("Christine needs to write CIRCLE");
    }
}
