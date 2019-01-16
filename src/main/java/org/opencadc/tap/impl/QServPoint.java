package org.opencadc.tap.impl;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import org.apache.log4j.Logger;

public class QServPoint extends Function
{
    private static final Logger log = Logger.getLogger(QServPoint.class);

    private Expression ra;
    private Expression dec;

    public QServPoint(Expression coordsys, Expression ra, Expression dec)
    {
        this.ra = ra;
        this.dec = dec;
    }

    public Expression getRA()
    {
        return ra;
    }

    public Expression getDec()
    {
        return dec;
    }
}
