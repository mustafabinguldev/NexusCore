package network.darkland.db.sql.mssql;

import network.darkland.db.StorageColumn;
import network.darkland.db.StorageType;
import network.darkland.db.sql.SqlDialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Microsoft SQL Server specific SQL fragments. T-SQL is the most different dialect Nexus
 * supports, which is exactly why the {@link SqlDialect} abstraction exists:
 * <ul>
 *   <li>no {@code CREATE TABLE IF NOT EXISTS} — wrapped in an {@code IF NOT EXISTS (...)} check</li>
 *   <li>no {@code ON DUPLICATE KEY} / {@code ON CONFLICT} — uses a {@code MERGE} statement</li>
 *   <li>no {@code LIMIT} — uses {@code SELECT TOP (?)}, see {@link #rankingQuery}</li>
 *   <li>no {@code ALTER TABLE ... RENAME} — uses {@code sp_rename}</li>
 * </ul>
 */
public final class MsSqlDialect implements SqlDialect {

    @Override
    public String driverClassName() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    public String buildJdbcUrl(String host, int port, String database) {
        return "jdbc:sqlserver://" + host + ":" + port
                + ";databaseName=" + database
                + ";encrypt=false;trustServerCertificate=true;loginTimeout=2";
    }

    @Override
    public String quote(String identifier) {
        return "[" + identifier + "]";
    }

    @Override
    public String idColumnType() {
        return "NVARCHAR(255)";
    }

    @Override
    public String columnType(StorageType type) {
        return switch (type) {
            case TEXT, JSON -> "NVARCHAR(MAX)";
            case INT        -> "INT";
            case LONG       -> "BIGINT";
            case DOUBLE     -> "FLOAT";
            case BOOLEAN    -> "BIT";
        };
    }

    @Override
    public String createTableSql(String table, StorageColumn id, List<StorageColumn> columns) {
        StringBuilder sql = new StringBuilder("IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '")
                .append(table).append("') CREATE TABLE ").append(quote(table)).append(" (")
                .append(quote(id.name())).append(' ').append(idColumnType()).append(" NOT NULL PRIMARY KEY");
        for (StorageColumn column : columns) {
            sql.append(", ").append(quote(column.name())).append(' ').append(columnType(column.type()));
        }
        return sql.append(')').toString();
    }

    @Override
    public String addColumnSql(String table, StorageColumn column) {
        return "IF COL_LENGTH('" + table + "', '" + column.name() + "') IS NULL "
                + "ALTER TABLE " + quote(table) + " ADD " + quote(column.name()) + " " + columnType(column.type());
    }

    @Override
    public String renameTableSql(String from, String to) {
        return "EXEC sp_rename '" + from + "', '" + to + "'";
    }

    @Override
    public String upsertSql(String table, StorageColumn id, List<StorageColumn> columns) {
        String sourceColumns = java.util.stream.Stream.concat(java.util.stream.Stream.of(id), columns.stream())
                .map(c -> "? AS " + quote(c.name()))
                .collect(Collectors.joining(", "));
        String insertValues = java.util.stream.Stream.concat(java.util.stream.Stream.of(id), columns.stream())
                .map(c -> "source." + quote(c.name()))
                .collect(Collectors.joining(", "));

        StringBuilder sql = new StringBuilder("MERGE INTO ").append(quote(table)).append(" AS target ")
                .append("USING (SELECT ").append(sourceColumns).append(") AS source ")
                .append("ON target.").append(quote(id.name())).append(" = source.").append(quote(id.name())).append(' ');
        if (!columns.isEmpty()) {
            sql.append("WHEN MATCHED THEN UPDATE SET ").append(columns.stream()
                    .map(c -> quote(c.name()) + " = source." + quote(c.name()))
                    .collect(Collectors.joining(", "))).append(' ');
        }
        return sql.append("WHEN NOT MATCHED THEN INSERT (").append(columnList(id, columns))
                .append(") VALUES (").append(insertValues).append(");").toString();
    }

    @Override
    public String rankingQuery(String table, String column, String direction) {
        return "SELECT TOP (?) * FROM " + quote(table) + " WHERE " + quote(column) + " IS NOT NULL"
                + " ORDER BY " + quote(column) + " " + direction;
    }

    @Override
    public List<String> createIndexStatements(String table, String indexName, String column) {
        return List.of("IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '" + indexName
                + "' AND object_id = OBJECT_ID('" + table + "')) "
                + "CREATE INDEX " + quote(indexName) + " ON " + quote(table) + " (" + quote(column) + ")");
    }
}
