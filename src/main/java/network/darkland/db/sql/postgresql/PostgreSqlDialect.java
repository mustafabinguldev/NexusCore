package network.darkland.db.sql.postgresql;

import network.darkland.db.StorageColumn;
import network.darkland.db.StorageType;
import network.darkland.db.sql.SqlDialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.stream.Collectors;

public final class PostgreSqlDialect implements SqlDialect {

    @Override
    public String driverClassName() {
        return "org.postgresql.Driver";
    }

    @Override
    public String buildJdbcUrl(String host, int port, String database) {
        return "jdbc:postgresql://" + host + ":" + port + "/" + database
                + "?connectTimeout=2&socketTimeout=5";
    }

    @Override
    public String quote(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String idColumnType() {
        return "VARCHAR(255)";
    }

    @Override
    public String columnType(StorageType type) {
        return switch (type) {
            case TEXT    -> "TEXT";
            case INT     -> "INTEGER";
            case LONG    -> "BIGINT";
            case DOUBLE  -> "DOUBLE PRECISION";
            case BOOLEAN -> "BOOLEAN";
            case JSON    -> "JSONB";
        };
    }

    @Override
    public String addColumnSql(String table, StorageColumn column) {
        return "ALTER TABLE " + quote(table) + " ADD COLUMN IF NOT EXISTS " + quote(column.name())
                + " " + columnType(column.type());
    }

    @Override
    public String upsertSql(String table, StorageColumn id, List<StorageColumn> columns) {
        String conflict = columns.isEmpty()
                ? "DO NOTHING"
                : "DO UPDATE SET " + columns.stream()
                        .map(c -> quote(c.name()) + " = EXCLUDED." + quote(c.name()))
                        .collect(Collectors.joining(", "));
        return "INSERT INTO " + quote(table) + " (" + columnList(id, columns) + ") VALUES ("
                + placeholders(columns.size() + 1) + ") ON CONFLICT (" + quote(id.name()) + ") " + conflict;
    }

    @Override
    public void bindJson(PreparedStatement ps, int index, String json) throws SQLException {
        if (json == null) ps.setNull(index, Types.OTHER);
        else ps.setObject(index, json, Types.OTHER);
    }
}
