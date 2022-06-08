package org.opencadc.tap.impl;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import org.apache.log4j.Logger;

public class QServRegionColumn
{
    private static final Logger log = Logger.getLogger(QServRegionColumn.class);

    public static Expression pointInRegion(QServPoint point)
    {
        Function scisql = new Function();
        scisql.setName("scisql_s2PtInCPoly");
        List<Expression> scisqlParams = new ArrayList<Expression>();
        scisqlParams.add(point.getRA());
        scisqlParams.add(point.getDec());
        scisqlParams.add(new Column(new Table(), "s_region_scisql"));
        scisql.setParameters(new ExpressionList(scisqlParams));

        Function pt = new Function();
        pt.setName("POINT");
        List<Expression> ptParams = new ArrayList<Expression>();
        ptParams.add(point.getRA());
        ptParams.add(point.getDec());
        pt.setParameters(new ExpressionList(ptParams));

        Function mbr = new Function();
        mbr.setName("MBRWITHIN");
        List<Expression> mbrParams = new ArrayList<Expression>();
        mbrParams.add(pt);
        mbrParams.add(new Column(new Table(), "s_region_bounds"));
        mbr.setParameters(new ExpressionList(mbrParams));

        AndExpression and = new AndExpression(mbr, scisql);
        return new AndExpression(and, new Column(new Table(), "1"));
    }
}
