package org.opencadc.tap.dialect.qserv.parser.region.function;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.apache.log4j.Logger;

/**
 * QServ polygon class
 * 
 */
public class QServPolygon extends Function implements QServRegion
{
    private static final Logger log = Logger.getLogger(QServPolygon.class);

    private Expression coordsys;
    private List<Expression> points;

    /**
     * QServPolygon constructor
     */
    public QServPolygon(List<Expression> params)
    {
        coordsys = params.remove(0);
        points = params;
    }

    /**
     * Get points
     * @return Points expression
     */
    public List<Expression> getPoints()
    {
        return points;
    }
    
    /**
     * Get points in region
     * @return The point expression
     */
    public Expression pointInRegion(QServPoint point)
    {
        List<Expression> params = new ArrayList<Expression>();
        params.add(point.getRA());
        params.add(point.getDec());
        params.addAll(getPoints());

        Function ptInBox = new Function();
        ptInBox.setName("scisql_s2PtInCPoly");
        ptInBox.setParameters(new ExpressionList(params));
        return ptInBox;
    }
}
