package network.darkland.db.sql;

import network.darkland.db.StorageColumn;
import network.darkland.db.StorageType;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Engine-specific SQL fragments. Every model is stored as a regular table: the id field is
 * the primary key and every other top-level field has its own typed column (see
 * {@link StorageType}). Identifiers passed in are already validated by {@link SqlRepository};
 * dialects only have to quote them.
 */
public interface SqlDialect {

    String driverClassName();

    String buildJdbcUrl(String host, int port, String database);

    default String buildJdbcUrlForFile(String filePath) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " is not a file-based engine");
    }

    String quote(String identifier);

    /** Column type of the primary key; keys always travel as strings. */
    String idColumnType();

    String columnType(StorageType type);

    default String tableOptions() {
        return "";
    }

    default String createTableSql(String table, StorageColumn id, List<StorageColumn> columns) {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(quote(table)).append(" (")
                .append(quote(id.name())).append(' ').append(idColumnType()).append(" NOT NULL PRIMARY KEY");
        for (StorageColumn column : columns) {
            sql.append(", ").append(quote(column.name())).append(' ').append(columnType(column.type()));
        }
        return sql.append(')').append(tableOptions()).toString();
    }

    default String addColumnSql(String table, StorageColumn column) {
        return "ALTER TABLE " + quote(table) + " ADD COLUMN " + quote(column.name()) + " " + columnType(column.type());
    }

    default String renameTableSql(String from, String to) {
        return "ALTER TABLE " + quote(from) + " RENAME TO " + quote(to);
    }

    /** Insert-or-replace of one row; parameters are the id followed by {@code columns} in order. */
    String upsertSql(String table, StorageColumn id, List<StorageColumn> columns);

    /** Rows ordered by a numeric column; the single parameter is the row limit. */
    default String rankingQuery(String table, String column, String direction) {
        return "SELECT * FROM " + quote(table) + " WHERE " + quote(column) + " IS NOT NULL"
                + " ORDER BY " + quote(column) + " " + direction + " LIMIT ?";
    }

    default List<String> createIndexStatements(String table, String indexName, String column) {
        return List.of("CREATE INDEX IF NOT EXISTS " + quote(indexName) + " ON " + quote(table)
                + " (" + quote(column) + ")");
    }

    default void bindJson(PreparedStatement ps, int index, String json) throws SQLException {
        if (json == null) ps.setNull(index, Types.VARCHAR);
        else ps.setString(index, json);
    }

    default String columnList(StorageColumn id, List<StorageColumn> columns) {
        return java.util.stream.Stream.concat(java.util.stream.Stream.of(id), columns.stream())
                .map(column -> quote(column.name()))
                .collect(Collectors.joining(", "));
    }

    default String placeholders(int count) {
        return String.join(", ", java.util.Collections.nCopies(count, "?"));
    }
}
