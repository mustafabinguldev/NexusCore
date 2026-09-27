package network.darkland.db;

/**
 * Column type a model field is stored as in a relational database. Scalar fields get a
 * native column type; nested objects, lists and maps are kept as a JSON column because
 * they have no single-column relational equivalent.
 */
public enum StorageType {

    TEXT,
    INT,
    LONG,
    DOUBLE,
    BOOLEAN,
    JSON;

    public boolean isNumeric() {
        return this == INT || this == LONG || this == DOUBLE;
    }

    /** Maps a Java field type (as declared on a {@code @DbDataModels} field) to a column type. */
    public static StorageType fromJavaType(Class<?> type) {
        if (type == String.class || type == char.class || type == Character.class || type.isEnum()) return TEXT;
        if (type == int.class || type == Integer.class
                || type == short.class || type == Short.class
                || type == byte.class || type == Byte.class) return INT;
        if (type == long.class || type == Long.class) return LONG;
        if (type == double.class || type == Double.class
                || type == float.class || type == Float.class) return DOUBLE;
        if (type == boolean.class || type == Boolean.class) return BOOLEAN;
        return JSON;
    }
}
