package network.darkland.db.sql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SqlRepositoryInputTest {

    @Test
    void rejectsSqlInjectedJsonField() {
        assertThrows(IllegalArgumentException.class,
                () -> SqlRepository.validateFieldName("score') DESC; DROP TABLE users;--"));
    }

    @Test
    void acceptsSimpleJsonFields() {
        assertEquals("score_2", SqlRepository.validateFieldName("score_2"));
    }

    @Test
    void rejectsUnboundedRankingLimits() {
        assertThrows(IllegalArgumentException.class, () -> SqlRepository.validateRankingLimit(0));
        assertThrows(IllegalArgumentException.class, () -> SqlRepository.validateRankingLimit(1_001));
    }

    @Test
    void keepsSafeShortTableNamesStable() {
        assertEquals("nexus_game_players", SqlRepository.sanitizeIdentifier("nexus_game_players"));
    }

    @Test
    void preventsNormalizedAndTruncatedTableNameCollisions() {
        String punctuationA = SqlRepository.sanitizeIdentifier("nexus_foo-bar_players");
        String punctuationB = SqlRepository.sanitizeIdentifier("nexus_foo.bar_players");
        String longA = SqlRepository.sanitizeIdentifier("nexus_" + "a".repeat(80) + "x");
        String longB = SqlRepository.sanitizeIdentifier("nexus_" + "a".repeat(80) + "y");

        assertNotEquals(punctuationA, punctuationB);
        assertNotEquals(longA, longB);
        assertTrue(punctuationA.length() <= 63);
        assertTrue(longA.length() <= 63);
    }

    @Test
    void keysGetTheirDeclaredTypeBack() {
        assertTrue(SqlRepository.idValue("42", network.darkland.db.StorageType.LONG).isNumber());
        assertEquals("abc", SqlRepository.idValue("abc", network.darkland.db.StorageType.TEXT).asText());
        assertTrue(SqlRepository.idValue("not-a-number", network.darkland.db.StorageType.INT).isTextual());
    }
}
