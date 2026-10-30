/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2019.                            (c) 2019.
 *  Government of Canada                 Gouvernement du Canada
 *  National Research Council            Conseil national de recherches
 *  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 *  All rights reserved                  Tous droits réservés
 *
 *  NRC disclaims any warranties,        Le CNRC dénie toute garantie
 *  expressed, implied, or               énoncée, implicite ou légale,
 *  statutory, of any kind with          de quelque nature que ce
 *  respect to the software,             soit, concernant le logiciel,
 *  including without limitation         y compris sans restriction
 *  any warranty of merchantability      toute garantie de valeur
 *  or fitness for a particular          marchande ou de pertinence
 *  purpose. NRC shall not be            pour un usage particulier.
 *  liable in any event for any          Le CNRC ne pourra en aucun cas
 *  damages, whether direct or           être tenu responsable de tout
 *  indirect, special or general,        dommage, direct ou indirect,
 *  consequential or incidental,         particulier ou général,
 *  arising from the use of the          accessoire ou fortuit, résultant
 *  software.  Neither the name          de l'utilisation du logiciel. Ni
 *  of the National Research             le nom du Conseil National de
 *  Council of Canada nor the            Recherches du Canada ni les noms
 *  names of its contributors may        de ses  participants ne peuvent
 *  be used to endorse or promote        être utilisés pour approuver ou
 *  products derived from this           promouvoir les produits dérivés
 *  software without specific prior      de ce logiciel sans autorisation
 *  written permission.                  préalable et particulière
 *                                       par écrit.
 *
 *  This file is part of the             Ce fichier fait partie du projet
 *  OpenCADC project.                    OpenCADC.
 *
 *  OpenCADC is free software:           OpenCADC est un logiciel libre ;
 *  you can redistribute it and/or       vous pouvez le redistribuer ou le
 *  modify it under the terms of         modifier suivant les termes de
 *  the GNU Affero General Public        la “GNU Affero General Public
 *  License as published by the          License” telle que publiée
 *  Free Software Foundation,            par la Free Software Foundation
 *  either version 3 of the              : soit la version 3 de cette
 *  License, or (at your option)         licence, soit (à votre gré)
 *  any later version.                   toute version ultérieure.
 *
 *  OpenCADC is distributed in the       OpenCADC est distribué
 *  hope that it will be useful,         dans l’espoir qu’il vous
 *  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
 *  without even the implied             GARANTIE : sans même la garantie
 *  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
 *  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
 *  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
 *  General Public License for           Générale Publique GNU Affero
 *  more details.                        pour plus de détails.
 *
 *  You should have received             Vous devriez avoir reçu une
 *  a copy of the GNU Affero             copie de la Licence Générale
 *  General Public License along         Publique GNU Affero avec
 *  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
 *  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
 *                                       <http://www.gnu.org/licenses/>.
 *
 *
 ************************************************************************
 */

package org.opencadc.tap.query.bigquery.parser.converter;

import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.RegionFinder;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.log4j.Logger;
import org.opencadc.tap.query.bigquery.parser.region.function.BigQueryCircle;
import org.opencadc.tap.query.bigquery.parser.region.function.BigQueryPoint;
import org.opencadc.tap.query.bigquery.parser.region.function.BigQueryPolygon;

import java.util.*;
 
 public class BigQueryRegionConverter extends RegionFinder {
 
     private static final Logger LOGGER = Logger.getLogger(BigQueryRegionConverter.class);
 
     private static final String CONTAINS_FUNCTION_NAME = "ST_CONTAINS";
     private static final String NOT_FUNCTION_NAME = "NOT";
     private static final String DWITHIN_FUNCTION_NAME = "ST_DWITHIN";
     private static final String INTERSECTS_FUNCTION_NAME = "ST_INTERSECTS";
     private static final String ST_AREA_FUNCTION_NAME = "ST_AREA";
     private static final String ST_DISTANCE_FUNCTION_NAME = "ST_DISTANCE";
     private static final String ST_MAXDISTANCE_FUNCTION_NAME = "ST_MAXDISTANCE";
     private static final List<String> MEASURE_FUNCTIONS = Arrays.asList(ST_DISTANCE_FUNCTION_NAME, ST_MAXDISTANCE_FUNCTION_NAME, ST_AREA_FUNCTION_NAME);
     private static final double DEGREE_ARC_SECONDS = 3600;
     private static final double ARC_SECOND_METERS = 30.8874796235;
     private static final String SAFE_DIVIDE = "SAFE_DIVIDE";
     private static final String SAFE_MULTIPLY = "SAFE_MULTIPLY";
     private static final String GEO_POINT_COLUMN = "geo_point";

     /**
      * TAP names of the tables that have a geo_point column.
      */
     private final Set<String> geoPointTables;

     /**
      * @param geoPointTables TAP names of the tables that have a geo_point
      *                       column, we use geo_point so that BigQuery
      *                       can use the clustering on it. Every other
      *                       table, including uploaded tables, uses
      *                       ST_GEOGPOINT on the columns in the query.
      */
     public BigQueryRegionConverter(ExpressionNavigator en, ReferenceNavigator rn,
                              FromItemNavigator fn, Collection<String> geoPointTables) {
         super(en, rn, fn);
         this.geoPointTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
         this.geoPointTables.addAll(geoPointTables);
     }
 
     @Override
     public Expression convertToImplementation(Function func) {
         return super.convertToImplementation(func);
     }
 
     /**
      * This method is called when a REGION PREDICATE function is one of the arguments in a binary expression,
      * and after the direct function conversion.
      *
      * <p>Supported functions: CONTAINS
      *
      * <p>Examples:
      *
      * <p>CONTAINS() = 0
      * CONTAINS() = 1
      * 1 = CONTAINS()
      * 0 = CONTAINS()
      */
     @Override
     protected Expression handleRegionPredicate(BinaryExpression binaryExpression) {
         LOGGER.debug("handleRegionPredicate(" + binaryExpression.getClass().getSimpleName() + "): " + binaryExpression);

         Function dwithinFunction = distanceToDWithin(binaryExpression);
         if (dwithinFunction != null) {
             return dwithinFunction;
         }

         binaryExpression = convertToMeters(binaryExpression);
 
         Expression left = binaryExpression.getLeftExpression();
         Expression right = binaryExpression.getRightExpression();
 
         if (!(binaryExpression instanceof EqualsTo)) {
             return binaryExpression;
         }
 
         Expression binaryValueExpression = null;
         Expression containsExpression = null;
 
         if ((isFunction(left) || isBinaryExpression(left)) && ParserUtil.isBinaryValue(right)) {
             containsExpression = left;
             binaryValueExpression = right;
         }
 
         if (ParserUtil.isBinaryValue(left) && (isFunction(right) || isBinaryExpression(right))) {
             containsExpression = right;
             binaryValueExpression = left;
         }
 
         if (containsExpression == null) {
             return binaryExpression;
         }
 
         boolean isTrueExpression = binaryValueExpression.toString().equals("1");
         if (!isTrueExpression) {
             Expression notBinaryExpression = handleNotExpression(containsExpression);
             return notBinaryExpression;
         }
 
         return containsExpression;
     }
 
     /**
      * This method is called when a CONTAINS is found outside of a predicate.
      * This could occur if the query had CONTAINS(...) in the select list or as
      * part of an arithmetic expression or aggregate function (since CONTAINS
      * returns a numeric value).
      * In BigQuery, we need to switch the arguments.
      */
     @Override
     protected Expression handleContains(Expression left, Expression right) {
         
         if (isCircle(left)) {
             if (isContainsPolygon(right)) {
                 return polygonContainsCircle(right, left);
             }
 
             return circleIntersects(left, right, true);
         }
 
         if (isCircle(right)) {
             if (isContainsPolygon(left)) {
                 return circleContainsPolygon(left, right);
             }
 
             return circleIntersects(left, right, true);
         }
 
         Function containsFunction = new Function();
         containsFunction.setName(CONTAINS_FUNCTION_NAME);
         List paramList = Arrays.asList(right, left);
 
         ExpressionList parameters = new ExpressionList(paramList);
         containsFunction.setParameters(parameters);
 
         return containsFunction;
     }
 
     private Function handleNotExpression(Expression containsFunction) {
         Function notFunction = new Function();
         ExpressionList parameters = new ExpressionList(Arrays.asList(containsFunction));
 
         notFunction.setName(NOT_FUNCTION_NAME);
         notFunction.setParameters(parameters);
 
         return notFunction;
     }
 
     /**
      * This method is called when a POINT geometry value is found.
      */
     @Override
     protected Expression handlePoint(Expression coordsys, Expression ra, Expression dec) {
         return new BigQueryPoint(ra, dec);
     }
 
     /**
      * This method is called when a POLYGON geometry value is found.
      */
     @Override
     protected Expression handlePolygon(List<Expression> expressions) {
         return new BigQueryPolygon(expressions);
     }
 
     /**
      * This method is called when a CIRCLE geometry value is found.
      */
     @Override
     protected Expression handleCircle(Expression coordsys, Expression ra, Expression dec, Expression radiusExpression) {
 
         if (isNumberValue((radiusExpression))) {
             double radius = getDoubleValue(radiusExpression).getValue();
 
             if (radius > 0.0D) {
                 return new BigQueryCircle(ra, dec, radiusExpression);
             }
         } else if (radiusExpression != null && !(radiusExpression instanceof NullValue)) {
             // A radius from a column or expression, e.g. CIRCLE(u.ra, u.dec, u.radius)
             return new BigQueryCircle(ra, dec, radiusExpression);
         }
 
         LOGGER.debug("Radius is missing or is 0.0.  Returning a POINT instead.");
         return handlePoint(coordsys, ra, dec);
     }
 
     /**
      * This method is called when a INTERSECTS is found outside of a predicate.
      * This could occur if the query had INTERSECTS(...) in the select list or as
      * part of an arithmetic expression or aggregate function (since INTERSECTS
      * returns a numeric value).
      */
     @Override
     protected Expression handleIntersects(Expression left, Expression right) {
         if (isContainsCircle(left, right)) {
             return circleIntersects(left, right, false);
         }
 
         Function intersectsFunction = new Function();
         ExpressionList parameters = new ExpressionList(Arrays.asList(right, left));
 
         intersectsFunction.setName(INTERSECTS_FUNCTION_NAME);
         intersectsFunction.setParameters(parameters);
 
         return intersectsFunction;
     }
 
     @Override
     protected Expression handleArea(Function areaFunction) {
         areaFunction.setName(ST_AREA_FUNCTION_NAME);
 
         return metersToDegreeFunction(areaFunction);
     }
 
     private BinaryExpression convertToMeters(BinaryExpression binaryExpression) {
         Expression left = binaryExpression.getLeftExpression();
         Expression right = binaryExpression.getRightExpression();

         if (isFunction(right) && isNumberValue(left)) {
             Expression measureFunction = getInternalMeasureFunction(right);
             if (measureFunction != null) {
                 binaryExpression.setRightExpression(measureFunction);
                 DoubleValue leftInMeters = getArchDistanceInMetersFromExpression(getDoubleValue(left));
                 binaryExpression.setLeftExpression(leftInMeters);
             }
         }

         if (isFunction(left) && isNumberValue(right)) {
             Expression measureFunction = getInternalMeasureFunction(left);
             if (measureFunction != null) {
                 binaryExpression.setLeftExpression(measureFunction);
                 DoubleValue rightInMeters = getArchDistanceInMetersFromExpression(getDoubleValue(right));
                 binaryExpression.setRightExpression(rightInMeters);
             }
         }

         return binaryExpression;
     }

     /**
      * Rewrite an upper bound on a DISTANCE as ST_DWITHIN. BigQuery can
      * run ST_DWITHIN as a spatial join, while comparing ST_DISTANCE against a
      * value means computing the distance for every pair of rows.
      */
     private Function distanceToDWithin(BinaryExpression binaryExpression) {
         Function distanceFunction;
         Expression radius;
         if (binaryExpression instanceof MinorThan || binaryExpression instanceof MinorThanEquals) {
             distanceFunction = getDistanceFunction(binaryExpression.getLeftExpression());
             radius = binaryExpression.getRightExpression();
         } else if (binaryExpression instanceof GreaterThan || binaryExpression instanceof GreaterThanEquals) {
             distanceFunction = getDistanceFunction(binaryExpression.getRightExpression());
             radius = binaryExpression.getLeftExpression();
         } else {
             return null;
         }

         if (distanceFunction == null || getDistanceFunction(radius) != null) {
             return null;
         }

         List<Expression> paramList = new ArrayList<>(distanceFunction.getParameters().getExpressions());
         paramList.add(degreesToMeters(radius));

         Function dwithinFunction = new Function();
         dwithinFunction.setName(DWITHIN_FUNCTION_NAME);
         dwithinFunction.setParameters(new ExpressionList(paramList));
         return dwithinFunction;
     }

     private Function getDistanceFunction(Expression expression) {
         if (!isFunction(expression) || !SAFE_DIVIDE.equals(((Function) expression).getName())) {
             return null;
         }
         ExpressionList parameters = ((Function) expression).getParameters();
         if (parameters == null || parameters.getExpressions().isEmpty()) {
             return null;
         }
         Expression measureFunction = (Expression) parameters.getExpressions().get(0);
         if (isFunction(measureFunction)
                 && ST_DISTANCE_FUNCTION_NAME.equals(((Function) measureFunction).getName())) {
             return (Function) measureFunction;
         }
         return null;
     }
 
     private Expression getInternalMeasureFunction(Expression expression) {
         ExpressionList parameters = ((Function) expression).getParameters();
         if (parameters == null) {
             return null;   // e.g. COUNT(*) has no parameter list to inspect
         }
         List<Expression> measureFunctionParams = parameters.getExpressions();

         return measureFunctionParams
                 .stream()
                 .filter(exp -> isFunction(exp) && MEASURE_FUNCTIONS.contains(((Function) exp).getName()))
                 .findAny()
                 .orElse(null);
     }
 
     private Function handleDistance(Expression left, Expression right, String distanceFunctionName) {
         Function distanceFunction = new Function();
         ExpressionList parameters = new ExpressionList(Arrays.asList(right, left));
 
         distanceFunction.setName(distanceFunctionName);
         distanceFunction.setParameters(parameters);
 
         return distanceFunction;
     }
 
     private Expression metersToDegreeFunction(Function measuresFunction) {
         Function multiplyFunction = new Function();
         multiplyFunction.setName(SAFE_MULTIPLY);
         ExpressionList multiplyFunctionParameters = new ExpressionList(
                 Arrays.asList(
                         new DoubleValue(Double.toString(DEGREE_ARC_SECONDS)),
                         new DoubleValue(Double.toString(ARC_SECOND_METERS))
                 )
         );
         multiplyFunction.setParameters(multiplyFunctionParameters);
 
         Function divideFunction = new Function();
         divideFunction.setName(SAFE_DIVIDE);
         ExpressionList divideFunctionParameters = new ExpressionList(Arrays.asList(measuresFunction, multiplyFunction));
         divideFunction.setParameters(divideFunctionParameters);
 
         return divideFunction;
     }
 
     /**
      * This method is called when DISTANCE function is found.
      */
     @Override
     protected Expression handleDistance(Expression left, Expression right) {
         Function distanceFunction = handleDistance(toGeoPoint(left), toGeoPoint(right), ST_DISTANCE_FUNCTION_NAME);
         return metersToDegreeFunction(distanceFunction);
     }
 
     private DoubleValue getDoubleValue(Expression expression) {
         return isDoubleValue(expression) ?
                 (DoubleValue) expression :
                 new DoubleValue(((Long) ((LongValue) expression).getValue()).toString());
     }
 
     private boolean isDoubleValue(Expression expression) {
         return (expression instanceof DoubleValue);
     }
 
     private boolean isLongValue(Expression expression) {
         return (expression instanceof LongValue);
     }
 
     private boolean isNumberValue(Expression expression) {
         return isDoubleValue(expression) || isLongValue(expression);
     }
 
     private boolean isFunction(Expression expression) {
         return (expression instanceof Function);
     }
 
     private boolean isBinaryExpression(Expression expression) {
         return (expression instanceof BinaryExpression);
     }
 
     private boolean isCircle(Expression expression) {
         return expression instanceof BigQueryCircle;
     }
 
     private boolean isContainsCircle(Expression left, Expression right) {
         return isCircle(left) || isCircle(right);
     }
 
     private Expression getRadius(Expression expression) {
         Expression radiusExpression = (Expression) ((Function) expression).getParameters().getExpressions().get(1);
         return degreesToMeters(radiusExpression);
     }

     private Expression degreesToMeters(Expression expression) {
         if (isNumberValue(expression)) {
             double degrees = getDoubleValue(expression).getValue();
             double archDistanceInMeters = getArchDistanceInMeters(degrees);
             return new DoubleValue(Double.toString(archDistanceInMeters));
         }

         // Convert a column or expression to meters in the query.
         Multiplication inMeters = new Multiplication();
         inMeters.setLeftExpression(new Parenthesis(expression));
         inMeters.setRightExpression(new DoubleValue(Double.toString(getArchDistanceInMeters(1.0))));
         return inMeters;
     }
 
     private DoubleValue getArchDistanceInMetersFromExpression(Expression expression) {
         double measurement = ((DoubleValue) expression).getValue();
         double measurementInMeters = getArchDistanceInMeters(measurement);
         return new DoubleValue(Double.toString(measurementInMeters));
     }
 
     private double getArchDistanceInMeters(double measurement) {
         return measurement * DEGREE_ARC_SECONDS * ARC_SECOND_METERS;
     }
 
     private Expression getCircleCenterPoint(Expression expression) {
         return (Expression) ((Function) expression).getParameters().getExpressions().get(0);
     }
 
     private List<Expression> circleDataParams(Expression left, Expression right, boolean isContains) {
         LOGGER.debug("circleDataParams(" + left + ", " + right + ")");

         if (isCircle(left)) {
             Expression radius = right instanceof BigQueryPoint && isContains ?
                     new DoubleValue("0") :
                     getRadius(left);

             return new ArrayList<>(Arrays.asList(toGeoPoint(right), getCircleCenterPoint(left), radius));
         }

         return new ArrayList<>(Arrays.asList(getCircleCenterPoint(right), toGeoPoint(left), getRadius(right)));
     }

     /**
      * Replace a point on the columns of a table that has a geo_point column
      * with a reference to geo_point, so that BigQuery can use the clustering
      * on it. Any other expression is returned unchanged.
      *
      * @param expression the expression to check
      * @return geo_point, qualified as in the query, or the original expression
      */
     private Expression toGeoPoint(Expression expression) {
         if (geoPointTables.isEmpty() || !(expression instanceof BigQueryPoint)) {
             return expression;
         }

         List<Expression> params = ((Function) expression).getParameters().getExpressions();
         if (!(params.get(0) instanceof Column) || !(params.get(1) instanceof Column)) {
             return expression;
         }
         Column ra = (Column) params.get(0);
         Column dec = (Column) params.get(1);
         String raTable = getTableReference(ra);
         String decTable = getTableReference(dec);
         if (raTable == null ? decTable != null : !raTable.equalsIgnoreCase(decTable)) {
             return expression;
         }

         Table fromTable = findFromTable(raTable);
         if (fromTable == null || !geoPointTables.contains(fromTable.getWholeTableName())) {
             return expression;
         }

         LOGGER.debug("Using " + GEO_POINT_COLUMN + " of " + fromTable.getWholeTableName());
         return new Column(raTable == null ? new Table() : ra.getTable(), GEO_POINT_COLUMN);
     }

     /**
      * @return the table name or alias a column is qualified with, or null if
      *         it is not qualified
      */
     private String getTableReference(Column column) {
         Table table = column.getTable();
         if (table == null || table.getName() == null) {
             return null;
         }
         return table.getWholeTableName();
     }

     /**
      * Find the table in the FROM clause that a table name or alias refers to.
      * An unqualified column can only be resolved if there is one table.
      *
      * @param reference table name or alias, or null for an unqualified column
      * @return the table, or null if it can't be resolved
      */
     private Table findFromTable(String reference) {
         List<Table> fromTables = ParserUtil.getFromTableList(plainSelect);
         if (reference == null) {
             return fromTables.size() == 1 ? fromTables.get(0) : null;
         }
         for (Table table : fromTables) {
             if (reference.equalsIgnoreCase(table.getAlias())
                     || reference.equalsIgnoreCase(table.getWholeTableName())
                     || (table.getAlias() == null && reference.equalsIgnoreCase(table.getName()))) {
                 return table;
             }
         }
         return null;
     }

     private Expression shapeContainsShape(Expression left, Expression right, String distanceFunctionName, BinaryExpression binaryExpression) {
 
         List<Expression> paramList = circleDataParams(left, right, true);
         int lastIndex = paramList.size() - 1;
         Expression radius = paramList.get(lastIndex);
         paramList.remove(lastIndex);
 
         Expression containsFunction = handleDistance(paramList.get(0), paramList.get(1), distanceFunctionName);
         binaryExpression.setLeftExpression(containsFunction);
         binaryExpression.setRightExpression(radius);
 
         return binaryExpression;
     }
 
 
     private Expression circleContainsPolygon(Expression left, Expression right) {
         MinorThanEquals minorThanEquals = new MinorThanEquals();
         Expression containsExpression = shapeContainsShape(left, right, ST_MAXDISTANCE_FUNCTION_NAME, minorThanEquals);
 
         return containsExpression;
     }
 
     private Expression polygonContainsCircle(Expression left, Expression right) {
         GreaterThanEquals greaterThanEquals = new GreaterThanEquals();
         Expression containsExpression = shapeContainsShape(left, right, ST_DISTANCE_FUNCTION_NAME, greaterThanEquals);
 
         return containsExpression;
     }
 
     private Function circleIntersects(Expression left, Expression right, boolean isContains) {
         Function intersectsFunction = new Function();
 
         intersectsFunction.setName(DWITHIN_FUNCTION_NAME);
 
         List paramList = circleDataParams(left, right, isContains);
         ExpressionList parameters = new ExpressionList(paramList);
         intersectsFunction.setParameters(parameters);
         return intersectsFunction;
 
     }
 
     private boolean isContainsPolygon(Expression expression) {
         return expression instanceof BigQueryPolygon;
     }
 }
