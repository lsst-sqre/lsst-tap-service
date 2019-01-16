package org.opencadc.tap.impl;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

public class QServParsingHelpers
{
    public static Expression equalsOne(Expression e)
    {
        EqualsTo equalsTo = new EqualsTo();
        equalsTo.setLeftExpression(e);
        equalsTo.setRightExpression(new LongValue("1"));
        return equalsTo;
    }

    public static double extractDoubleParameter(Expression e)
    {
        if (e instanceof DoubleValue) {
            return ((DoubleValue)e).getValue();
        }

        if (e instanceof LongValue) {
            return ((LongValue)e).getValue();
        }

        throw new UnsupportedOperationException("Expected number literal, got " + e);
    }
}
