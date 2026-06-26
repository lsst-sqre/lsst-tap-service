package org.opencadc.tap.kafka.util;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

import org.apache.log4j.Logger;

/**
 * Detects whether TAP_UPLOAD tables should be ingested as partitioned tables
 * in Qserv, based on how they are used in the translated SQL.
 *
 */
public class UploadPartitionDetector {

    private static final Logger log = Logger.getLogger(UploadPartitionDetector.class);
    private final Map<String, DirectorConfig> directors;

    static class DirectorConfig {
        final String database;
        final String table;
        final String idCol;

        DirectorConfig(String database, String table, String idCol) {
            this.database = database;
            this.table = table;
            this.idCol = idCol;
        }
    }

    public static class PartitionInfo {
        /* Holds information on how to partition the uploaded table */
        public enum Type {
            DIRECTOR, DEPENDENT
        }

        public final Type type;
        public final String longitudeColName;
        public final String latitudeColName;
        public final String idColName;
        public final String refDirectorDatabase;
        public final String refDirectorTable;
        public final String refDirectorIdColName;

        private PartitionInfo(Type type, String lon, String lat,
                String id, String refDb, String refTable, String refIdCol) {
            this.type = type;
            this.longitudeColName = lon;
            this.latitudeColName = lat;
            this.idColName = id;
            this.refDirectorDatabase = refDb;
            this.refDirectorTable = refTable;
            this.refDirectorIdColName = refIdCol;
        }

        public static PartitionInfo director(String lonCol, String latCol) {
            return new PartitionInfo(Type.DIRECTOR, lonCol, latCol, null, null, null, null);
        }

        public static PartitionInfo dependent(String idCol, String refDb, String refTable, String refIdCol) {
            return new PartitionInfo(Type.DEPENDENT, null, null, idCol, refDb, refTable, refIdCol);
        }

        @Override
        public String toString() {
            if (type == Type.DIRECTOR) {
                return "DIRECTOR(lon=" + longitudeColName + ", lat=" + latitudeColName + ")";
            }
            return "DEPENDENT(id=" + idColName + ", ref=" + refDirectorDatabase + "." + refDirectorTable + ")";
        }
    }

    /**
     * @param directorsConfig comma-separated database.table:idCol pairs,
     *                        e.g. "dp1.Object:objectId"
     */
    public UploadPartitionDetector(String directorsConfig) {
        this.directors = parseDirectorsConfig(directorsConfig);
    }

    /**
     * Detect partition type for each upload table based on how they appear in the
     * SQL.
     *
     * @param sql              translated QServ SQL from qRunner.internalSQL
     * @param uploadTableNames set of translated table names (e.g.
     *                         user_job123.mytable)
     * @return map from upload table name to its detected partition info (absent =
     *         fully replicated)
     */
    public Map<String, PartitionInfo> detect(String sql, Set<String> uploadTableNames) {
        Map<String, PartitionInfo> result = new HashMap<>();
        if (sql == null || sql.isBlank() || uploadTableNames == null || uploadTableNames.isEmpty()) {
            return result;
        }
        try {
            Statement stmt = new CCJSqlParserManager().parse(new StringReader(sql));
            if (!(stmt instanceof Select)) {
                return result;
            }
            SelectBody body = ((Select) stmt).getSelectBody();
            if (!(body instanceof PlainSelect)) {
                return result;
            }
            PlainSelect ps = (PlainSelect) body;

            Map<String, String> uploadAliases = new HashMap<>();
            Map<String, String[]> otherAliases = new HashMap<>();

            buildAliasMaps(ps, uploadTableNames, uploadAliases, otherAliases);

            if (uploadAliases.isEmpty()) {
                return result;
            }

            // Director detection: spatial function in WHERE or JOIN ON
            detectDirector(ps, uploadAliases, otherAliases, result);

            // Dependent detection: join to a known director table
            if (!directors.isEmpty()) {
                detectDependent(ps, uploadAliases, otherAliases, result);
            }

        } catch (JSQLParserException e) {
            log.warn("Could not parse SQL for upload partition detection: " + e.getMessage());
        } catch (Exception e) {
            log.warn("Unexpected error during upload partition detection: " + e.getMessage());
        }
        return result;
    }

    private void buildAliasMaps(PlainSelect ps, Set<String> uploadTableNames,
            Map<String, String> uploadAliases, Map<String, String[]> otherAliases) {
        categorize(ps.getFromItem(), uploadTableNames, uploadAliases, otherAliases);
        if (ps.getJoins() != null) {
            for (Object o : ps.getJoins()) {
                Join join = (Join) o;
                categorize(join.getRightItem(), uploadTableNames, uploadAliases, otherAliases);
            }
        }
    }

    /**
     * Categorize a FromItem as either an upload table or another table, and record
     * its alias.
     * It does this by checking the schema name of the table. If the schema name
     * starts with "user_" or "job_",
     * it is considered an upload table.
     * 
     */
    private void categorize(FromItem item, Set<String> uploadTableNames,
            Map<String, String> uploadAliases, Map<String, String[]> otherAliases) {

        if (!(item instanceof Table)) {
            return;
        }
        Table t = (Table) item;
        String schema = t.getSchemaName();
        String name = t.getName();
        if (schema == null || name == null) {
            return;
        }
        String alias = (t.getAlias() != null) ? t.getAlias() : name;
        String fullName = schema + "." + name;
        String schemaLower = schema.toLowerCase();

        if (schemaLower.startsWith("user_") || schemaLower.startsWith("job_")) {
            if (uploadTableNames.contains(fullName)) {
                uploadAliases.put(alias.toLowerCase(), fullName);
            }
        } else {
            otherAliases.put(alias.toLowerCase(), new String[] { schema, name });
        }
    }

    /**
     * Detect director partitioning by scanning for spatial functions in WHERE and
     * JOIN ON clauses.
     * 
     * @param ps
     * @param uploadAliases
     * @param otherAliases
     * @param result
     */
    private void detectDirector(PlainSelect ps, Map<String, String> uploadAliases,
            Map<String, String[]> otherAliases,
            Map<String, PartitionInfo> result) {

        if (ps.getWhere() != null) {
            scanForSpatial(ps.getWhere(), uploadAliases, otherAliases, result);
        }

        if (ps.getJoins() != null) {
            for (Object o : ps.getJoins()) {
                Join join = (Join) o;
                Expression onExpr = join.getOnExpression();
                if (onExpr != null) {
                    scanForSpatial(onExpr, uploadAliases, otherAliases, result);
                }
            }
        }
    }

    /**
     * Scan an expression for spatial functions (scisql_s2pt* or scisql_angsep) and
     * check if
     * they reference upload table columns.
     * 
     */
    @SuppressWarnings("unchecked")
    private void scanForSpatial(Expression expr, Map<String, String> uploadAliases,
            Map<String, String[]> otherAliases,
            Map<String, PartitionInfo> result) {

        if (expr instanceof Function) {
            Function func = (Function) expr;
            String name = func.getName().toLowerCase();
            boolean isSpatial = name.startsWith("scisql_s2pt") || name.equals("scisql_angsep");
            if (isSpatial && func.getParameters() != null) {
                List<Expression> args = func.getParameters().getExpressions();
                if (args.size() >= 4 && (name.equals("scisql_s2ptincircle") || name.equals("scisql_angsep"))) {
                    checkSpatialColumns(args.get(0), args.get(1), args.get(2), args.get(3),
                            uploadAliases, otherAliases, result);
                } else if (args.size() >= 2) {
                    checkSpatialColumns(args.get(0), args.get(1), null, null,
                            uploadAliases, otherAliases, result);
                }
            }

        } else if (expr instanceof BinaryExpression) {
            BinaryExpression be = (BinaryExpression) expr;
            scanForSpatial(be.getLeftExpression(), uploadAliases, otherAliases, result);
            scanForSpatial(be.getRightExpression(), uploadAliases, otherAliases, result);

        } else if (expr instanceof Parenthesis) {
            scanForSpatial(((Parenthesis) expr).getExpression(), uploadAliases, otherAliases, result);
        }
    }

    private void tryPair(Expression lon1, Expression lat1, Expression lon2, Expression lat2,
            Map<String, String> uploadAliases, Map<String, String[]> otherAliases,
            Map<String, PartitionInfo> result) {

        if (!(lon1 instanceof Column) || !(lat1 instanceof Column)) {
            return;
        }

        Column lonCol = (Column) lon1;
        Column latCol = (Column) lat1;

        if (lonCol.getTable() == null || latCol.getTable() == null) {
            return;
        }

        String lonAlias = lonCol.getTable().getName().toLowerCase();
        String latAlias = latCol.getTable().getName().toLowerCase();

        if (!lonAlias.equals(latAlias)) {
            return;
        }

        if (lon2 != null) {

            if (lon2 instanceof Column && lat2 instanceof Column) {

                Column lonCol2 = (Column) lon2;
                Column latCol2 = (Column) lat2;

                if (lonCol2.getTable() == null || latCol2.getTable() == null) {
                    return;
                }

                String lonAlias2 = lonCol2.getTable().getName().toLowerCase();
                String latAlias2 = latCol2.getTable().getName().toLowerCase();

                if (!lonAlias2.equals(latAlias2)) {
                    return;
                }

                if (!otherAliases.containsKey(lonAlias2)) {
                    return;
                }

                String key = otherAliases.get(lonAlias2)[0].toLowerCase() + "."
                        + otherAliases.get(lonAlias2)[1].toLowerCase();
                if (!directors.containsKey(key)) {
                    return;
                }

            }
        }

        String uploadTable = uploadAliases.get(lonAlias);
        if (uploadTable != null && !result.containsKey(uploadTable)) {
            log.debug("Detected director partition for " + uploadTable
                    + ": lon=" + lonCol.getColumnName() + ", lat=" + latCol.getColumnName());
            result.put(uploadTable, PartitionInfo.director(lonCol.getColumnName(), latCol.getColumnName()));
        }

    }

    private void checkSpatialColumns(Expression lon1, Expression lat1,
            Expression lon2, Expression lat2,
            Map<String, String> uploadAliases, Map<String, String[]> otherAliases,
            Map<String, PartitionInfo> result) {

        tryPair(lon1, lat1, lon2, lat2, uploadAliases, otherAliases, result);
        if (lon2 != null) {
            tryPair(lon2, lat2, lon1, lat1, uploadAliases, otherAliases, result);
        }
    }

    private void detectDependent(PlainSelect ps, Map<String, String> uploadAliases,
            Map<String, String[]> otherAliases, Map<String, PartitionInfo> result) {

        if (ps.getJoins() != null) {
            for (Object o : ps.getJoins()) {
                Join join = (Join) o;
                Expression onExpr = join.getOnExpression();
                if (onExpr != null) {
                    scanForEquiJoin(onExpr, uploadAliases, otherAliases, result);
                }
            }
        }

        if (ps.getWhere() != null) {
            scanForEquiJoin(ps.getWhere(), uploadAliases, otherAliases, result);
        }
    }

    private void scanForEquiJoin(Expression expr, Map<String, String> uploadAliases,
            Map<String, String[]> otherAliases, Map<String, PartitionInfo> result) {

        if (expr instanceof EqualsTo) {
            EqualsTo eq = (EqualsTo) expr;
            checkEquiJoinPair(eq.getLeftExpression(), eq.getRightExpression(),
                    uploadAliases, otherAliases, result);
            checkEquiJoinPair(eq.getRightExpression(), eq.getLeftExpression(),
                    uploadAliases, otherAliases, result);
        } else if (expr instanceof BinaryExpression) {
            BinaryExpression be = (BinaryExpression) expr;
            scanForEquiJoin(be.getLeftExpression(), uploadAliases, otherAliases, result);
            scanForEquiJoin(be.getRightExpression(), uploadAliases, otherAliases, result);
        } else if (expr instanceof Parenthesis) {
            scanForEquiJoin(((Parenthesis) expr).getExpression(), uploadAliases, otherAliases, result);
        }
    }

    private void checkEquiJoinPair(Expression uploadSide, Expression directorSide,
            Map<String, String> uploadAliases, Map<String, String[]> otherAliases,
            Map<String, PartitionInfo> result) {

        if (!(uploadSide instanceof Column) || !(directorSide instanceof Column)) {
            return;
        }
        Column uploadCol = (Column) uploadSide;
        Column dirCol = (Column) directorSide;
        if (uploadCol.getTable() == null || dirCol.getTable() == null) {
            return;
        }
        String uploadAlias = uploadCol.getTable().getName().toLowerCase();
        String uploadTable = uploadAliases.get(uploadAlias);
        if (uploadTable == null || result.containsKey(uploadTable)) {
            return;
        }
        String dirAlias = dirCol.getTable().getName().toLowerCase();
        String[] dirTableInfo = otherAliases.get(dirAlias);
        if (dirTableInfo == null) {
            return;
        }

        String dirKey = dirTableInfo[0].toLowerCase() + "." + dirTableInfo[1].toLowerCase();
        DirectorConfig dc = directors.get(dirKey);
        if (dc != null && dc.idCol.equalsIgnoreCase(dirCol.getColumnName())) {
            log.debug("Detected dependent partition for " + uploadTable
                    + ": id=" + uploadCol.getColumnName()
                    + ", ref=" + dc.database + "." + dc.table);
            result.put(uploadTable, PartitionInfo.dependent(
                    uploadCol.getColumnName(), dc.database, dc.table, dc.idCol));
        }
    }

    static Map<String, DirectorConfig> parseDirectorsConfig(String config) {
        Map<String, DirectorConfig> result = new HashMap<>();
        if (config == null || config.isBlank()) {
            return result;
        }

        for (String entry : config.split(",")) {
            entry = entry.trim();
            int colon = entry.lastIndexOf(':');
            if (colon < 0) {
                continue;
            }
            String tableRef = entry.substring(0, colon).trim();
            String idCol = entry.substring(colon + 1).trim();
            int dot = tableRef.indexOf('.');
            if (dot < 0) {
                continue;
            }
            String db = tableRef.substring(0, dot).trim();
            String table = tableRef.substring(dot + 1).trim();
            if (db.isEmpty() || table.isEmpty() || idCol.isEmpty()) {
                continue;
            }
            String key = db.toLowerCase() + "." + table.toLowerCase();
            result.put(key, new DirectorConfig(db, table, idCol));
        }
        return result;
    }
}
