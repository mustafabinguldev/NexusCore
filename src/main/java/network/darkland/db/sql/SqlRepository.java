package network.darkland.db.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import network.darkland.db.StorageColumn;
import network.darkland.db.StorageType;
import network.darkland.protocol.DataAddon;
import network.darkland.resilience.ResilienceConfig;
import network.darkland.resilience.ResilienceExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stores every model as a regular relational table: the model's id field is the primary
 * key and each other top-level field has its own typed column. Nested objects, lists and
 * maps have no single-column equivalent and are kept in JSON columns.
 *
 * <p>Tables written by older versions ({@code id_key} + a single JSON {@code data} column)
 * are migrated on first use: the old table is renamed to {@code <table>_json_backup} and
 * its rows are copied into the new layout.</p>
 */
public final class SqlRepository {

    private static final Logger LOGGER = Logger.getLogger(SqlRepository.class.getName());
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

    static final String LEGACY_ID_COLUMN = "id_key";
    static final String LEGACY_DATA_COLUMN = "data";
    private static final int MIGRATION_BATCH_SIZE = 500;

    private final SqlConnectionManager connectionManager;
    private final SqlDialect dialect;
    private final ExecutorService executor;
    private final ResilienceConfig resilience;

    private final ConcurrentHashMap<String, TableLayout> layouts = new ConcurrentHashMap<>();

    /**
     * Resolved column layout of one addon's table.
     *
     * @param id      primary key column
     * @param columns every other column, in declaration order
     */
    record TableLayout(String table, StorageColumn id, List<StorageColumn> columns, String upsertSql) {

        StorageColumn column(String name) {
            for (StorageColumn column : columns) {
                if (column.name().equalsIgnoreCase(name)) return column;
            }
            return null;
        }
    }

    public SqlRepository(SqlConnectionManager connectionManager, SqlDialect dialect,
                          ExecutorService executor, ResilienceConfig resilience) {
        this.connectionManager = connectionManager;
        this.dialect = dialect;
        this.executor = executor;
        this.resilience = resilience;
    }

    private String tableName(DataAddon addon) {
        String raw = "nexus_" + addon.getNamespace() + "_" + addon.getDataset();
        return sanitizeIdentifier(raw);
    }

    static String sanitizeIdentifier(String raw) {
        String cleaned = raw.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
        if (cleaned.equals(raw) && cleaned.length() <= 63) return cleaned;

        String hash = sha256(raw).substring(0, 12);
        int prefixLength = Math.min(cleaned.length(), 63 - hash.length() - 1);
        return cleaned.substring(0, prefixLength) + "_" + hash;
    }

    static String validateFieldName(String fieldName) {
        if (fieldName == null || !fieldName.matches("[A-Za-z_][A-Za-z0-9_]{0,63}")) {
            throw new IllegalArgumentException("Invalid ranking field name");
        }
        return fieldName;
    }

    static int validateRankingLimit(int limit) {
        if (limit < 1 || limit > 1_000) {
            throw new IllegalArgumentException("Ranking limit must be between 1 and 1000");
        }
        return limit;
    }

    private static String sha256(String value) {
        try {
            return java.util.HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    // ---------------------------------------------------------------- layout

    /**
     * Builds the column layout for an addon. Field names become column names verbatim, so
     * they must be plain identifiers and unique regardless of case (MySQL, MariaDB, SQL Server
     * and SQLite compare column names case-insensitively).
     */
    static TableLayout buildLayout(String table, List<StorageColumn> declared, SqlDialect dialect) {
        StorageColumn id = null;
        List<StorageColumn> columns = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (StorageColumn column : declared) {
            String name = column.name();
            if (name == null || !name.matches("[A-Za-z_][A-Za-z0-9_]{0,62}")) {
                throw new IllegalStateException("[SQL] Field '" + name + "' of table " + table
                        + " cannot be used as a column name (letters, digits and '_' only, at most 63 characters)");
            }
            if (!seen.add(name.toLowerCase(Locale.ROOT))) {
                throw new IllegalStateException("[SQL] Fields of table " + table
                        + " differ only by case and would share the column '" + name + "'");
            }
            if (column.id() && id == null) {
                id = new StorageColumn(name, column.type(), true);
            } else {
                columns.add(new StorageColumn(name, column.type(), false));
            }
        }

        if (id == null) {
            if (!seen.add(LEGACY_ID_COLUMN)) {
                throw new IllegalStateException("[SQL] Table " + table + " has no id field and already uses '"
                        + LEGACY_ID_COLUMN + "'");
            }
            id = new StorageColumn(LEGACY_ID_COLUMN, StorageType.TEXT, true);
        }

        List<StorageColumn> frozen = List.copyOf(columns);
        return new TableLayout(table, id, frozen, dialect.upsertSql(table, id, frozen));
    }

    private TableLayout ensureTable(DataAddon addon) {
        String table = tableName(addon);
        TableLayout layout = layouts.get(table);
        if (layout != null) return layout;

        synchronized (layouts) {
            layout = layouts.get(table);
            if (layout != null) return layout;

            layout = buildLayout(table, addon.storageColumns(), dialect);
            try (Connection conn = connectionManager.getConnection()) {
                Set<String> existing = existingColumns(conn, table);
                if (existing == null) {
                    execute(conn, dialect.createTableSql(table, layout.id(), layout.columns()));
                    existing = existingColumns(conn, table);
                    if (existing == null) throw new SQLException("table " + table + " is still missing after CREATE TABLE");
                }
                if (isLegacyLayout(existing, layout)) {
                    migrateLegacyTable(conn, layout);
                } else {
                    addMissingColumns(conn, layout, existing);
                }
            } catch (SQLException e) {
                throw new IllegalStateException("[SQL] Could not create/verify table " + table, e);
            }
            layouts.put(table, layout);
            return layout;
        }
    }

    /** Lower-cased column names of a table, or {@code null} when the table does not exist. */
    private Set<String> existingColumns(Connection conn, String table) {
        String sql = "SELECT * FROM " + dialect.quote(table) + " WHERE 1 = 0";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            Set<String> names = new HashSet<>();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                names.add(meta.getColumnName(i).toLowerCase(Locale.ROOT));
            }
            return names;
        } catch (SQLException e) {
            return null;
        }
    }

    static boolean isLegacyLayout(Set<String> existing, TableLayout layout) {
        return existing.contains(LEGACY_ID_COLUMN)
                && existing.contains(LEGACY_DATA_COLUMN)
                && !layout.id().name().equalsIgnoreCase(LEGACY_ID_COLUMN)
                && layout.column(LEGACY_DATA_COLUMN) == null;
    }

    private void addMissingColumns(Connection conn, TableLayout layout, Set<String> existing) throws SQLException {
        for (StorageColumn column : layout.columns()) {
            if (existing.contains(column.name().toLowerCase(Locale.ROOT))) continue;
            execute(conn, dialect.addColumnSql(layout.table(), column));
            LOGGER.info("[SQL] Added column " + column.name() + " to " + layout.table());
        }
    }

    /**
     * Moves an {@code id_key}/{@code data} JSON table aside and copies its rows into a table
     * with one column per field. The old table is kept as a backup.
     */
    private void migrateLegacyTable(Connection conn, TableLayout layout) throws SQLException {
        String table = layout.table();
        String backup = sanitizeIdentifier(table + "_json_backup");
        if (existingColumns(conn, backup) != null) {
            backup = sanitizeIdentifier(table + "_json_backup_" + System.currentTimeMillis());
        }

        LOGGER.info("[SQL] Migrating JSON table " + table + " to one column per field (backup: " + backup + ")");
        execute(conn, dialect.renameTableSql(table, backup));

        try {
            execute(conn, dialect.createTableSql(table, layout.id(), layout.columns()));

            List<String[]> rows = new ArrayList<>();
            String select = "SELECT " + dialect.quote(LEGACY_ID_COLUMN) + ", " + dialect.quote(LEGACY_DATA_COLUMN)
                    + " FROM " + dialect.quote(backup);
            try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(select)) {
                while (rs.next()) rows.add(new String[]{rs.getString(1), rs.getString(2)});
            }

            boolean autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            int copied = 0;
            try (PreparedStatement ps = conn.prepareStatement(layout.upsertSql())) {
                for (String[] row : rows) {
                    JsonNode document;
                    try {
                        document = row[1] == null ? null : JSON.readTree(row[1]);
                    } catch (JsonProcessingException e) {
                        LOGGER.warning("[SQL] Skipping row " + row[0] + " of " + backup + ": stored data is not valid JSON");
                        continue;
                    }
                    bindRow(ps, layout, row[0], document);
                    ps.addBatch();
                    if (++copied % MIGRATION_BATCH_SIZE == 0) ps.executeBatch();
                }
                ps.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(autoCommit);
            }
            LOGGER.info("[SQL] Migrated " + copied + " rows from " + backup + " into " + table);
        } catch (SQLException | RuntimeException e) {
            restoreLegacyTable(conn, table, backup);
            throw e;
        }
    }

    private void restoreLegacyTable(Connection conn, String table, String backup) {
        try {
            execute(conn, "DROP TABLE " + dialect.quote(table));
        } catch (SQLException ignored) {
            // the new table may never have been created
        }
        try {
            execute(conn, dialect.renameTableSql(backup, table));
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "[SQL] Migration of " + table + " failed and the original table could not be "
                    + "renamed back; its data is intact in " + backup, e);
        }
    }

    private static void execute(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    // ---------------------------------------------------------------- row mapping

    private void bindRow(PreparedStatement ps, TableLayout layout, String key, JsonNode document) throws SQLException {
        ps.setString(1, key);
        int index = 2;
        for (StorageColumn column : layout.columns()) {
            JsonNode value = document == null ? null : document.get(column.name());
            bindValue(ps, index++, column.type(), value == null || value.isNull() ? null : value);
        }
    }

    private void bindValue(PreparedStatement ps, int index, StorageType type, JsonNode value) throws SQLException {
        if (type == StorageType.JSON) {
            dialect.bindJson(ps, index, value == null ? null : value.toString());
            return;
        }
        if (value == null) {
            ps.setNull(index, sqlType(type));
            return;
        }
        switch (type) {
            case TEXT    -> ps.setString(index, value.isValueNode() ? value.asText() : value.toString());
            case INT     -> ps.setInt(index, value.asInt());
            case LONG    -> ps.setLong(index, value.asLong());
            case DOUBLE  -> ps.setDouble(index, value.asDouble());
            case BOOLEAN -> ps.setBoolean(index, value.asBoolean());
            default      -> throw new IllegalStateException("unreachable");
        }
    }

    private static int sqlType(StorageType type) {
        return switch (type) {
            case TEXT, JSON -> Types.VARCHAR;
            case INT        -> Types.INTEGER;
            case LONG       -> Types.BIGINT;
            case DOUBLE     -> Types.DOUBLE;
            case BOOLEAN    -> Types.BOOLEAN;
        };
    }

    /** Rebuilds the JSON document of one row. NULL columns are left out so the model's defaults apply. */
    private ObjectNode readRow(ResultSet rs, TableLayout layout) throws SQLException {
        ObjectNode document = NODES.objectNode();
        document.set(layout.id().name(), idValue(rs.getString(layout.id().name()), layout.id().type()));

        for (StorageColumn column : layout.columns()) {
            String name = column.name();
            JsonNode value = switch (column.type()) {
                case TEXT    -> NODES.textNode(rs.getString(name));
                case INT     -> NODES.numberNode(rs.getInt(name));
                case LONG    -> NODES.numberNode(rs.getLong(name));
                case DOUBLE  -> NODES.numberNode(rs.getDouble(name));
                case BOOLEAN -> NODES.booleanNode(rs.getBoolean(name));
                case JSON    -> parseJsonColumn(rs.getString(name), layout.table(), name);
            };
            if (!rs.wasNull() && value != null) document.set(name, value);
        }
        return document;
    }

    /** Keys are stored as text; give them back their declared type so the model sees e.g. a number. */
    static JsonNode idValue(String key, StorageType type) {
        if (key == null) return NODES.nullNode();
        try {
            return switch (type) {
                case INT, LONG -> NODES.numberNode(Long.parseLong(key));
                case DOUBLE    -> NODES.numberNode(Double.parseDouble(key));
                case BOOLEAN   -> NODES.booleanNode(Boolean.parseBoolean(key));
                case TEXT, JSON -> NODES.textNode(key);
            };
        } catch (NumberFormatException e) {
            return NODES.textNode(key);
        }
    }

    private static JsonNode parseJsonColumn(String json, String table, String column) {
        if (json == null) return null;
        try {
            return JSON.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("[SQL] Column " + table + "." + column + " does not hold valid JSON", e);
        }
    }

    private static StorageColumn numericColumn(TableLayout layout, String fieldName) {
        StorageColumn column = layout.column(fieldName);
        if (column == null || !column.type().isNumeric()) {
            throw new IllegalArgumentException("[SQL] '" + fieldName + "' is not a numeric field of " + layout.table());
        }
        return column;
    }

    // ---------------------------------------------------------------- operations

    public CompletableFuture<Boolean> exists(DataAddon addon, String key) {
        return ResilienceExecutor.decorateAsync(
                resilience.sqlCircuitBreaker(),
                resilience.sqlRetry(),
                resilience.retryScheduler(),
                () -> CompletableFuture.supplyAsync(() -> {
                    TableLayout layout = ensureTable(addon);
                    String sql = "SELECT 1 FROM " + dialect.quote(layout.table())
                            + " WHERE " + dialect.quote(layout.id().name()) + " = ?";
                    try (Connection conn = connectionManager.getConnection();
                         PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, key);
                        try (ResultSet rs = ps.executeQuery()) {
                            return rs.next();
                        }
                    } catch (SQLException e) {
                        throw new IllegalStateException("[SQL] exists() failed for key=" + key, e);
                    }
                }, executor)
        );
    }

    public CompletableFuture<String> getValue(DataAddon addon, String key) {
        return ResilienceExecutor.decorateAsync(
                resilience.sqlCircuitBreaker(),
                resilience.sqlRetry(),
                resilience.retryScheduler(),
                () -> CompletableFuture.supplyAsync(() -> {
                    TableLayout layout = ensureTable(addon);
                    String sql = "SELECT * FROM " + dialect.quote(layout.table())
                            + " WHERE " + dialect.quote(layout.id().name()) + " = ?";
                    try (Connection conn = connectionManager.getConnection();
                         PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, key);
                        try (ResultSet rs = ps.executeQuery()) {
                            return rs.next() ? JSON.writeValueAsString(readRow(rs, layout)) : null;
                        }
                    } catch (SQLException | JsonProcessingException e) {
                        throw new IllegalStateException("[SQL] getValue() failed for key=" + key, e);
                    }
                }, executor)
        );
    }

    public CompletableFuture<Void> removeValue(DataAddon addon, String key) {
        return ResilienceExecutor.decorateAsync(
                resilience.sqlCircuitBreaker(),
                resilience.sqlRetry(),
                resilience.retryScheduler(),
                () -> CompletableFuture.runAsync(() -> {
                    TableLayout layout = ensureTable(addon);
                    String sql = "DELETE FROM " + dialect.quote(layout.table())
                            + " WHERE " + dialect.quote(layout.id().name()) + " = ?";
                    try (Connection conn = connectionManager.getConnection();
                         PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, key);
                        ps.executeUpdate();
                    } catch (SQLException e) {
                        throw new IllegalStateException("[SQL] removeValue() failed for key=" + key, e);
                    }
                }, executor)
        );
    }

    public CompletableFuture<Void> setValue(DataAddon addon, String key, String jsonValue) {
        return ResilienceExecutor.decorateAsync(
                resilience.sqlCircuitBreaker(),
                resilience.sqlRetry(),
                resilience.retryScheduler(),
                () -> CompletableFuture.runAsync(() -> {
                    TableLayout layout = ensureTable(addon);
                    JsonNode document;
                    try {
                        document = JSON.readTree(jsonValue);
                    } catch (JsonProcessingException e) {
                        throw new IllegalArgumentException("[SQL] setValue() received invalid JSON for key=" + key, e);
                    }
                    try (Connection conn = connectionManager.getConnection();
                         PreparedStatement ps = conn.prepareStatement(layout.upsertSql())) {
                        bindRow(ps, layout, key, document);
                        ps.executeUpdate();
                    } catch (SQLException e) {
                        throw new IllegalStateException("[SQL] setValue() failed for key=" + key, e);
                    }
                }, executor)
        );
    }

    public CompletableFuture<Map<Integer, Object>> getRanking(DataAddon addon, String fieldName, String orderType, int limitCount) {
        String validatedFieldName = validateFieldName(fieldName);
        int validatedLimitCount = validateRankingLimit(limitCount);
        return ResilienceExecutor.decorateAsync(
                resilience.sqlCircuitBreaker(),
                resilience.sqlRetry(),
                resilience.retryScheduler(),
                () -> CompletableFuture.supplyAsync(() -> {
                    TableLayout layout = ensureTable(addon);
                    StorageColumn column = numericColumn(layout, validatedFieldName);
                    String direction = "DESC".equalsIgnoreCase(orderType) ? "DESC" : "ASC";
                    String sql = dialect.rankingQuery(layout.table(), column.name(), direction);

                    Map<Integer, Object> rankingMap = new LinkedHashMap<>();
                    try (Connection conn = connectionManager.getConnection();
                         PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, validatedLimitCount);
                        int rank = 1;
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                rankingMap.put(rank++, JSON.convertValue(readRow(rs, layout), Map.class));
                            }
                        }
                    } catch (SQLException e) {
                        throw new IllegalStateException("[SQL] getRanking() failed for field=" + validatedFieldName, e);
                    }
                    return rankingMap;
                }, executor)
        );
    }

    public CompletableFuture<Integer> getPosition(DataAddon addon, String key, String fieldName, String orderType) {
        String validatedFieldName = validateFieldName(fieldName);
        return ResilienceExecutor.decorateAsync(
                resilience.sqlCircuitBreaker(),
                resilience.sqlRetry(),
                resilience.retryScheduler(),
                () -> CompletableFuture.supplyAsync(() -> {
                    TableLayout layout = ensureTable(addon);
                    String table = dialect.quote(layout.table());
                    String column = dialect.quote(numericColumn(layout, validatedFieldName).name());

                    Double value = null;
                    String selectSql = "SELECT " + column + " FROM " + table
                            + " WHERE " + dialect.quote(layout.id().name()) + " = ?";
                    try (Connection conn = connectionManager.getConnection();
                         PreparedStatement ps = conn.prepareStatement(selectSql)) {
                        ps.setString(1, key);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                double v = rs.getDouble(1);
                                if (!rs.wasNull()) value = v;
                            }
                        }
                    } catch (SQLException e) {
                        throw new IllegalStateException("[SQL] getPosition() lookup failed for key=" + key, e);
                    }

                    if (value == null) return -1;

                    String comparator = "DESC".equalsIgnoreCase(orderType) ? ">" : "<";
                    String countSql = "SELECT COUNT(*) FROM " + table + " WHERE " + column + " " + comparator + " ?";

                    try (Connection conn = connectionManager.getConnection();
                         PreparedStatement ps = conn.prepareStatement(countSql)) {
                        ps.setDouble(1, value);
                        try (ResultSet rs = ps.executeQuery()) {
                            long ahead = rs.next() ? rs.getLong(1) : 0L;
                            return (int) (ahead + 1);
                        }
                    } catch (SQLException e) {
                        throw new IllegalStateException("[SQL] getPosition() count failed for key=" + key, e);
                    }
                }, executor)
        );
    }

    public void ensureIndex(DataAddon addon, String fieldName) {
        fieldName = validateFieldName(fieldName);
        TableLayout layout = ensureTable(addon);
        StorageColumn column = layout.column(fieldName);
        if (column == null || column.type() == StorageType.JSON || column.type() == StorageType.TEXT) {
            LOGGER.warning("[SQL] ensureIndex() skipped for " + layout.table() + "." + fieldName
                    + ": only numeric and boolean columns are indexed");
            return;
        }
        String indexName = sanitizeIdentifier("ix_" + layout.table() + "_" + column.name().toLowerCase(Locale.ROOT));
        for (String statement : dialect.createIndexStatements(layout.table(), indexName, column.name())) {
            try (Connection conn = connectionManager.getConnection();
                 PreparedStatement ps = conn.prepareStatement(statement)) {
                ps.executeUpdate();
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "[SQL] ensureIndex() statement failed for "
                        + layout.table() + "." + fieldName + " (likely already applied): " + e.getMessage());
            }
        }
    }
}
