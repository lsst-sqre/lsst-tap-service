package org.opencadc.tap.impl;

import java.util.ArrayList;
import java.util.List;

import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.RegionFinder;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import org.apache.log4j.Logger;

import org.opencadc.tap.impl.QServBox;
import org.opencadc.tap.impl.QServCircle;
import org.opencadc.tap.impl.QServPoint;

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
     * This method is called when a REGION PREDICATE function is
     * one of the arguments in a binary expression, and after the
     * direct function conversion (like handleCircle, etc).
     *
     */
    @Override
    protected Expression handleRegionPredicate(BinaryExpression biExpr)
    {
        log.debug("handleRegionPredicate(" + biExpr.getClass().getSimpleName() + "): " + biExpr);
        return biExpr;
    }

    /**
     * This function is called to parse out a CONTAINS, and the expressions are
     * already parsed arguments.
     */
    @Override
    protected Expression handleContains(Expression left, Expression right)
    {
        if(!(left instanceof QServPoint)) {
            throw new UnsupportedOperationException("CONTAINS first argument must be a point");

        }

        if(!(right instanceof QServRegion)) {
            throw new UnsupportedOperationException("CONTAINS second argument must be a region");
        }

        QServPoint point = (QServPoint)left;
        QServRegion region = (QServRegion)right;
        return region.pointInRegion(point);
    }

    @Override
    protected Expression handlePoint(Expression coordsys, Expression ra, Expression dec)
    {
        return new QServPoint(coordsys, ra, dec);
    }

    /**
     * This method is called when a CIRCLE geometry value is found.
     *
     * From CIRCLE(coordinate_system, ra, dec, radius)
     * To qserv_areaspec_circle(long [deg], lat [deg], radius [deg])
     *
     */
    @Override
    protected Expression handleCircle(Expression coordsys, Expression ra, Expression dec, Expression radius)
    {
        return new QServCircle(coordsys, ra, dec, radius);
    }

    /**
     * This method is called whenever BOX geometry is found.
     * We must convert it to a qserv_area_box function.
     *
     * BOX(ra center, dec center, width (len of ra leg), height (len of dec leg))
     * converted to
     * qserv_areaspec_box(min ra, min dec, max ra, max dec)
     */
    @Override
    protected Expression handleBox(Function adqlFunction)
    {
        return new QServBox(adqlFunction.getParameters().getExpressions());
    }

    /**
     * This method is called when a POLYGON geometry value is found.
     **/
    @Override
    protected Expression handlePolygon(List<Expression> expressions)
    {
        return new QServPolygon(expressions);
    }
}
