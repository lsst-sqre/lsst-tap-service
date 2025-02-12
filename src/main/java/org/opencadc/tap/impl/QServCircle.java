package org.opencadc.tap.impl;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.apache.log4j.Logger;

/**
 * QServCircle class
 *
 */
public class QServCircle extends Function implements QServRegion
{
    private static final Logger log = Logger.getLogger(QServCircle.class);

    private Expression coordsys;
    private Expression ra;
    private Expression dec;
    private Expression radius;
    
    /**
     * QServCircle constructor
     * @param coordsys Coordsys
     * @param ra ra
     * @param dec dec
     * @param radius radius
     */
    public QServCircle(Expression coordsys, Expression ra, Expression dec, Expression radius)
    {
        this.coordsys = coordsys;
        this.ra = ra;
        this.dec = dec;
        this.radius = radius;
    }
    
    /**
     * Method to get ra
     * @return the ra expression object
     */
    public Expression getRA()
    {
        return ra;
    }

    /**
     * Method to get dec
     * @return the declination expression object
     */
    public Expression getDec()
    {
        return dec;
    }
    
    /**
     * Method to get radius
     * @return the radius expression object
     */
    public Expression getRadius()
    {
        return radius;
    }

    /**
     * Get point in region
     * @return The points expression
     */
    public Expression pointInRegion(QServPoint point)
    {
        List<Expression> params = new ArrayList<Expression>();
        params.add(point.getRA());
        params.add(point.getDec());
        params.add(getRA());
        params.add(getDec());
        params.add(getRadius());

        Function ptInCircle = new Function();
        ptInCircle.setName("scisql_s2PtInCircle");
        ptInCircle.setParameters(new ExpressionList(params));
        return ptInCircle;
    }
}
