package network.darkland.db.sql.mariadb;

import network.darkland.db.StorageColumn;
import network.darkland.db.StorageType;
import network.darkland.db.sql.SqlDialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * MariaDB specific SQL fragments. Kept as its own dialect (rather than reusing
 * {@link network.darkland.db.sql.mysql.MySqlDialect}) even though the SQL is very similar,
 * because it uses a different JDBC driver/URL scheme and the two engines are free to diverge
 * (MariaDB's {@code JSON} type is a {@code LONGTEXT} alias with a validity check, not a real
 * native JSON type like MySQL's).
 */
public final class MariaDbDialect implements SqlDialect {

    @Override
    public String driverClassName() {
        return "org.mariadb.jdbc.Driver";
    }

    @Override
    public String buildJdbcUrl(String host, int port, String database) {
        return "jdbc:mariadb://" + host + ":" + port + "/" + database
                + "?useUnicode=true&characterEncoding=UTF-8"
                + "&connectTimeout=2000&socketTimeout=5000";
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
            case JSON    -> "LONGTEXT";
        };
    }

    @Override
    public String tableOptions() {
        return " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    }

    @Override
    public String addColumnSql(String table, StorageColumn column) {
        return "ALTER TABLE " + quote(table) + " ADD COLUMN IF NOT EXISTS " + quote(column.name())
                + " " + columnType(column.type());
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
}
