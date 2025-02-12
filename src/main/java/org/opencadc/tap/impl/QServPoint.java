package org.opencadc.tap.impl;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import org.apache.log4j.Logger;

/**
 * QServPoint method
 * 
 */
public class QServPoint extends Function
{
    private static final Logger log = Logger.getLogger(QServPoint.class);

    private Expression ra;
    private Expression dec;

    /**
     * QServPoint constructor
     */
    public QServPoint(Expression coordsys, Expression ra, Expression dec)
    {
        this.ra = ra;
        this.dec = dec;
    }

    /**
     * Method to get RA
     * @return The ra expression
     */
    public Expression getRA()
    {
        return ra;
    }

    /**
     * Method to get Declination
     * @return The dec expression
     */
    public Expression getDec()
    {
        return dec;
    }
}
