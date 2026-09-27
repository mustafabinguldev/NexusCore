package network.darkland.db.sql.mysql;

import network.darkland.db.StorageColumn;
import network.darkland.db.StorageType;
import network.darkland.db.sql.SqlDialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * MySQL specific SQL fragments. This is the only place MySQL syntax appears —
 * {@link network.darkland.db.sql.SqlRepository} contains all the actual query orchestration.
 */
public final class MySqlDialect implements SqlDialect {

    @Override
    public String driverClassName() {
        return "com.mysql.cj.jdbc.Driver";
    }

    @Override
    public String buildJdbcUrl(String host, int port, String database) {
        return "jdbc:mysql://" + host + ":" + port + "/" + database
                + "?useUnicode=true&characterEncoding=UTF-8"
                + "&useSSL=false&allowPublicKeyRetrieval=true"
                + "&serverTimezone=UTC&connectTimeout=2000&socketTimeout=5000";
    }

    @Override
    public String quote(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public String idColumnType() {
        return "VARCHAR(191)";
    }

    @Override
    public String columnType(StorageType type) {
        return switch (type) {
            case TEXT    -> "TEXT";
            case INT     -> "INT";
            case LONG    -> "BIGINT";
            case DOUBLE  -> "DOUBLE";
            case BOOLEAN -> "BOOLEAN";
            case JSON    -> "JSON";
        };
    }

    @Override
    public String tableOptions() {
        return " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    }

    @Override
    public String upsertSql(String table, StorageColumn id, List<StorageColumn> columns) {
        String updates = columns.isEmpty()
                ? quote(id.name()) + " = " + quote(id.name())
                : columns.stream()
                        .map(c -> quote(c.name()) + " = VALUES(" + quote(c.name()) + ")")
                        .collect(Collectors.joining(", "));
        return "INSERT INTO " + quote(table) + " (" + columnList(id, columns) + ") VALUES ("
                + placeholders(columns.size() + 1) + ") ON DUPLICATE KEY UPDATE " + updates;
    }

    @Override
    public List<String> createIndexStatements(String table, String indexName, String column) {
        // MySQL has no CREATE INDEX IF NOT EXISTS; a duplicate index is reported and ignored.
        return List.of("CREATE INDEX " + quote(indexName) + " ON " + quote(table) + " (" + quote(column) + ")");
    }
}
