package network.darkland.db.sql.sqlite;

import network.darkland.db.StorageColumn;
import network.darkland.db.StorageType;
import network.darkland.db.sql.SqlDialect;

import java.util.List;
import java.util.stream.Collectors;

public final class SqliteDialect implements SqlDialect {

    @Override
    public String driverClassName() {
        return "org.sqlite.JDBC";
    }

    @Override
    public String buildJdbcUrl(String host, int port, String database) {
        throw new UnsupportedOperationException("SQLite is file-based; use buildJdbcUrlForFile(filePath) instead");
    }

    @Override
    public String buildJdbcUrlForFile(String filePath) {
        return "jdbc:sqlite:" + filePath;
    }

    @Override
    public String quote(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String idColumnType() {
        return "TEXT";
    }

    @Override
    public String columnType(StorageType type) {
        return switch (type) {
            case TEXT, JSON     -> "TEXT";
            case INT, LONG      -> "INTEGER";
            case DOUBLE         -> "REAL";
            case BOOLEAN        -> "BOOLEAN";
        };
    }

    @Override
    public String upsertSql(String table, StorageColumn id, List<StorageColumn> columns) {
        String conflict = columns.isEmpty()
                ? "DO NOTHING"
                : "DO UPDATE SET " + columns.stream()
                        .map(c -> quote(c.name()) + " = excluded." + quote(c.name()))
                        .collect(Collectors.joining(", "));
        return "INSERT INTO " + quote(table) + " (" + columnList(id, columns) + ") VALUES ("
                + placeholders(columns.size() + 1) + ") ON CONFLICT(" + quote(id.name()) + ") " + conflict;
    }
}
