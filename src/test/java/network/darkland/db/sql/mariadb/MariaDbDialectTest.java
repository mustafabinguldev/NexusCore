package network.darkland.db.sql.mariadb;

import network.darkland.db.StorageColumn;
import network.darkland.db.StorageType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MariaDbDialectTest {

    private final MariaDbDialect dialect = new MariaDbDialect();

    @Test
    void createsOneTypedColumnPerField() {
        String sql = dialect.createTableSql("nexus_players",
                new StorageColumn("uuid", StorageType.TEXT, true),
                List.of(new StorageColumn("coins", StorageType.LONG, false),
                        new StorageColumn("stats", StorageType.JSON, false)));

        assertEquals("CREATE TABLE IF NOT EXISTS `nexus_players` (`uuid` VARCHAR(191) NOT NULL PRIMARY KEY, "
                + "`coins` BIGINT, `stats` LONGTEXT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", sql);
    }

    @Test
    void upsertUpdatesEveryNonKeyColumn() {
        String sql = dialect.upsertSql("nexus_players",
                new StorageColumn("uuid", StorageType.TEXT, true),
                List.of(new StorageColumn("coins", StorageType.LONG, false),
                        new StorageColumn("name", StorageType.TEXT, false)));

        assertEquals("INSERT INTO `nexus_players` (`uuid`, `coins`, `name`) VALUES (?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE `coins` = VALUES(`coins`), `name` = VALUES(`name`)", sql);
    }
}
