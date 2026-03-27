package org.opencadc.tap.query.pg.parser.converter;

import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.region.pgsphere.PgsphereRegionConverter;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Interval;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import org.apache.log4j.Logger;

/**
 * Converts ADQL region predicates to pg_sphere equivalents for ObsCore queries.
 * Handles column renaming (s_region -> pgs_region/pgs_center) and COORDSYS.
 */
public class ObsCoreRegionConverter extends PgsphereRegionConverter {
    private static Logger log = Logger.getLogger(ObsCoreRegionConverter.class);

    public ObsCoreRegionConverter() {
        super(new ExpressionNavigator(), new ReferenceNavigator(), new FromItemNavigator());
    }

    @Override
    protected Expression handleContains(Expression left, Expression right) {
        log.debug("handleContains: " + left + " " + right);
        rewriteRegionColumns(left, right);

        if (right instanceof Interval) {
            if (left instanceof Column || left instanceof Interval) {
                // OK
            } else if (left instanceof DoubleValue) {
                DoubleValue dv = (DoubleValue) left;
                double d = dv.getValue();
                double d1 = Double.longBitsToDouble(Double.doubleToLongBits(d) - 1L);
                double d2 = Double.longBitsToDouble(Double.doubleToLongBits(d) + 1L);
                Interval p = new Interval(new DoubleValue(Double.toString(d1)), new DoubleValue(Double.toString(d2)));
                return super.handleIntersects(p, right);
            } else {
                throw new IllegalArgumentException("invalid argument type for contains: " + left.getClass().getSimpleName() + " Interval");
            }
        }
        return super.handleContains(left, right);
    }

    @Override
    protected Expression handleIntersects(Expression left, Expression right) {
        log.debug("handleIntersects: " + left + " " + right);
        rewriteRegionColumns(left, right);
        return super.handleIntersects(left, right);
    }

    @Override
    protected Expression handleInterval(Expression lower, Expression upper) {
        return new Interval(lower, upper);
    }

    /**
     * CENTROID(s_region) is converted to pgs_center column reference.
     */
    @Override
    protected Expression handleCentroid(Function adqlFunction) {
        log.debug("handleCentroid: " + adqlFunction);

        ExpressionList el = adqlFunction.getParameters();
        if (el.getExpressions().size() == 1) {
            Expression e = (Expression) el.getExpressions().get(0);
            if (e instanceof Column) {
                Column c = (Column) e;
                if ("s_region".equalsIgnoreCase(c.getColumnName())) {
                    c.setColumnName("pgs_center");
                    return c;
                }
            }
        }
        throw new UnsupportedOperationException("CENTROID() used with unsupported parameter.");
    }

    @Override
    protected Expression handleCoordSys(Function adqlFunction) {
        log.debug("handleCoordSys: " + adqlFunction);
        return new StringValue("'ICRS'");
    }

    private void rewriteRegionColumns(Expression left, Expression right) {
        rewriteRegionColumn(left);
        rewriteRegionColumn(right);
    }

    private void rewriteRegionColumn(Expression e) {
        if (e instanceof Column) {
            Column c = (Column) e;
            if ("s_region".equalsIgnoreCase(c.getColumnName())) {
                c.setColumnName("pgs_region");
            }
        }
    }
}
