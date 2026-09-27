package network.darkland.db;

/**
 * One top-level field of a data model as a relational store sees it.
 *
 * @param name field name, used verbatim as the column name
 * @param type column type
 * @param id   whether this field is the model's key (becomes the primary key)
 */
public record StorageColumn(String name, StorageType type, boolean id) {
}
