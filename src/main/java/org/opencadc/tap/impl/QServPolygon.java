package org.opencadc.tap.impl;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.apache.log4j.Logger;

import org.opencadc.tap.impl.QServRegion;

public class QServPolygon extends Function implements QServRegion
{
    private static final Logger log = Logger.getLogger(QServPolygon.class);

    private Expression coordsys;
    private List<Expression> points;

    public QServPolygon(List<Expression> params)
    {
        coordsys = params.remove(0);
        points = params;
    }

    public List<Expression> getPoints()
    {
        return points;
    }

    public Expression getAreaspec()
    {
        Function boxFunction = new Function();
        boxFunction.setName("qserv_areaspec_poly");
        boxFunction.setParameters(new ExpressionList(getPoints()));
        return QServParsingHelpers.equalsOne(boxFunction);
    }

    public Expression pointInRegion(QServPoint point)
    {
        List<Expression> params = new ArrayList<Expression>();
        params.add(point.getRA());
        params.add(point.getDec());
        params.addAll(getPoints());

        Function ptInBox = new Function();
        ptInBox.setName("scisql_s2PtInCPoly");
        ptInBox.setParameters(new ExpressionList(params));
        return new AndExpression(getAreaspec(), ptInBox);
    }
}
