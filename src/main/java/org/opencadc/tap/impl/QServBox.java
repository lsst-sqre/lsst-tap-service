package org.opencadc.tap.impl;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.apache.log4j.Logger;

import org.opencadc.tap.impl.QServRegion;

public class QServBox extends Function implements QServRegion
{
    private static final Logger log = Logger.getLogger(QServBox.class);

    private Expression coordsys;
    private Expression centerRA;
    private Expression centerDec;
    private Expression width;
    private Expression height;

    public QServBox(List<Expression> params)
    {
        coordsys = params.get(0);
        centerRA = params.get(1);
        centerDec = params.get(2);
        width = params.get(3);
        height = params.get(4);
    }

    public List<Expression> getCorners()
    {
        double cRA = QServParsingHelpers.extractDoubleParameter(centerRA);
        double cDec = QServParsingHelpers.extractDoubleParameter(centerDec);
        double w = QServParsingHelpers.extractDoubleParameter(width);
        double h = QServParsingHelpers.extractDoubleParameter(height);

        double minRA = cRA - w / 2;
        double maxRA = cRA + w / 2;
        double minDec = cDec - h / 2;
        double maxDec = cDec + h / 2;

        List<Expression> params = new ArrayList<Expression>();
        params.add(new DoubleValue(Double.toString(minRA)));
        params.add(new DoubleValue(Double.toString(minDec)));
        params.add(new DoubleValue(Double.toString(maxRA)));
        params.add(new DoubleValue(Double.toString(maxDec)));
        return params;
    }

    public Expression getAreaspec()
    {
        Function boxFunction = new Function();
        boxFunction.setName("qserv_areaspec_box");
        boxFunction.setParameters(new ExpressionList(getCorners()));
        return QServParsingHelpers.equalsOne(boxFunction);
    }

    public Expression pointInRegion(QServPoint point)
    {
        List<Expression> params = getCorners();
        params.add(0, point.getRA());
        params.add(1, point.getDec());

        Function ptInBox = new Function();
        ptInBox.setName("scisql_s2PtInBox");
        ptInBox.setParameters(new ExpressionList(params));
        return new AndExpression(getAreaspec(), ptInBox);
    }
}
