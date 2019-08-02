package org.opencadc.tap.impl;

import net.sf.jsqlparser.expression.Expression;

import org.opencadc.tap.impl.QServPoint;

public interface QServRegion {
    public Expression pointInRegion(QServPoint point);
}
