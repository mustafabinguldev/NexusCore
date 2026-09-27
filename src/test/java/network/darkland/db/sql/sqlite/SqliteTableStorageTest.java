package network.darkland.db.sql.sqlite;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import network.darkland.model.schema.JsonModelAddon;
import network.darkland.model.schema.ModelSchemaParser;
import network.darkland.resilience.ResilienceConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end check that SQL stores lay models out as ordinary tables, one column per field. */
class SqliteTableStorageTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TABLE = "nexus_game_players";

    private static final String PLAYERS = """
            {
              "addonId": 910,
              "name": "Players",
              "namespace": "game",
              "dataset": "players",
              "fields": {
                "uuid":   { "type": "string", "id": true },
                "name":   { "type": "string" },
                "coins":  { "type": "long" },
                "level":  { "type": "int" },
                "ratio":  { "type": "double" },
                "banned": { "type": "boolean" },
                "tags":   { "type": "list", "of": "string" },
                "stats":  { "type": "object", "fields": { "kills": { "type": "int" } } }
              }
            }
            """;

    @TempDir
    Path dir;

    private String file;
    private ExecutorService executor;
    private SqliteDataStore store;
    private final JsonModelAddon players = new JsonModelAddon(ModelSchemaParser.parse("players.json", PLAYERS));

    @BeforeEach
    void setUp() {
        file = dir.resolve("nexus.db").toString();
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        if (store != null) store.close();
        executor.shutdownNow();
    }

    private SqliteDataStore open() {
        store = new SqliteDataStore(file, executor, new ResilienceConfig());
        return store;
    }

    private Connection jdbc() throws Exception {
        return DriverManager.getConnection("jdbc:sqlite:" + file);
    }

    private Set<String> columns(String table) throws Exception {
        Set<String> names = new HashSet<>();
        try (Connection conn = jdbc(); Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(\"" + table + "\")")) {
            while (rs.next()) names.add(rs.getString("name"));
        }
        return names;
    }

    @Test
    void storesEveryFieldInItsOwnColumn() throws Exception {
        open().setValue(players, "p1", """
                {"uuid":"p1","name":"Alex","coins":1500,"level":7,"ratio":1.25,"banned":false,
                 "tags":["vip"],"stats":{"kills":3}}
                """).join();

        assertEquals(Set.of("uuid", "name", "coins", "level", "ratio", "banned", "tags", "stats"), columns(TABLE));

        try (Connection conn = jdbc(); Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT name, coins, level FROM " + TABLE + " WHERE uuid = 'p1'")) {
            assertTrue(rs.next());
            assertEquals("Alex", rs.getString("name"));
            assertEquals(1500L, rs.getLong("coins"));
            assertEquals(7, rs.getInt("level"));
        }

        JsonNode back = MAPPER.readTree(store.getValue(players, "p1").join());
        assertEquals("p1", back.get("uuid").asText());
        assertEquals(1500L, back.get("coins").asLong());
        assertEquals(1.25, back.get("ratio").asDouble());
        assertFalse(back.get("banned").asBoolean());
        assertEquals("vip", back.get("tags").get(0).asText());
        assertEquals(3, back.get("stats").get("kills").asInt());

        assertTrue(store.exists(players, "p1").join());
        store.removeValue(players, "p1").join();
        assertNull(store.getValue(players, "p1").join());
    }

    @Test
    void ranksOnTheNumericColumn() {
        open();
        store.setValue(players, "a", "{\"uuid\":\"a\",\"coins\":10}").join();
        store.setValue(players, "b", "{\"uuid\":\"b\",\"coins\":30}").join();
        store.setValue(players, "c", "{\"uuid\":\"c\",\"coins\":20}").join();
        store.ensureIndex(players, "coins");

        Map<Integer, Object> ranking = store.getRanking(players, "coins", "DESC", 2).join();
        assertEquals(2, ranking.size());
        assertInstanceOf(Map.class, ranking.get(1));
        assertEquals("b", ((Map<?, ?>) ranking.get(1)).get("uuid"));
        assertEquals("c", ((Map<?, ?>) ranking.get(2)).get("uuid"));

        assertEquals(3, store.getPosition(players, "a", "coins", "DESC").join());
        assertEquals(1, store.getPosition(players, "a", "coins", "ASC").join());
    }

    @Test
    void migratesLegacyJsonTables() throws Exception {
        try (Connection conn = jdbc(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE \"" + TABLE + "\" (id_key TEXT PRIMARY KEY, data TEXT NOT NULL)");
            st.execute("INSERT INTO \"" + TABLE + "\" VALUES ('old', '{\"uuid\":\"old\",\"name\":\"Legacy\",\"coins\":99,\"stats\":{\"kills\":5}}')");
        }

        JsonNode back = MAPPER.readTree(open().getValue(players, "old").join());
        assertEquals("Legacy", back.get("name").asText());
        assertEquals(99, back.get("coins").asLong());
        assertEquals(5, back.get("stats").get("kills").asInt());

        assertTrue(columns(TABLE).contains("coins"));
        assertFalse(columns(TABLE).contains("data"));
        assertEquals(Set.of("id_key", "data"), columns(TABLE + "_json_backup"));
    }

    @Test
    void addsColumnsForNewlyDeclaredFields() throws Exception {
        try (Connection conn = jdbc(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE \"" + TABLE + "\" (uuid TEXT PRIMARY KEY, name TEXT)");
            st.execute("INSERT INTO \"" + TABLE + "\" VALUES ('p', 'Sam')");
        }

        JsonNode back = MAPPER.readTree(open().getValue(players, "p").join());
        assertEquals("Sam", back.get("name").asText());
        assertFalse(back.has("coins"), "missing values are left out so the model defaults apply");
        assertTrue(columns(TABLE).containsAll(Set.of("coins", "level", "tags", "stats")));
    }
}
