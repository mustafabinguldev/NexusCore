package network.darkland.model.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import network.darkland.NexusApplication;
import network.darkland.db.StorageColumn;
import network.darkland.db.StorageType;
import network.darkland.protocol.DataAddon;
import network.darkland.protocol.NexusJsonDataContainer;
import network.darkland.protocol.RequestType;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JsonModelAddon extends DataAddon {

    private static final Logger LOGGER = Logger.getLogger(JsonModelAddon.class.getName());

    private final ModelDefinition definition;
    private final SchemaNormalizer normalizer;

    public JsonModelAddon(ModelDefinition definition) {
        this.definition = Objects.requireNonNull(definition, "definition");
        this.normalizer = new SchemaNormalizer(definition.addonName());
    }

    public ModelDefinition definition() {
        return definition;
    }

    // ---------------------------------------------------------------- identity

    @Override
    public int addonId() {
        return definition.addonId();
    }

    @Override
    public String addonName() {
        return definition.addonName();
    }

    @Override
    public String cacheKeyHeaderTag() {
        return definition.cacheKeyHeaderTag();
    }

    @Override
    public String getNamespace() {
        return definition.namespace();
    }

    @Override
    public String getDataset() {
        return definition.dataset();
    }

    @Override
    public int getCacheTTL() {
        return definition.cacheTTL();
    }

    @Override
    public boolean l1CacheEnabled() {
        return definition.l1Cache();
    }

    @Override
    public String getIdFieldName() {
        return definition.idField().name();
    }

    @Override
    public Class<?> getIdClassName() {
        return definition.idField().kind().javaType();
    }

    @Override
    public List<StorageColumn> storageColumns() {
        return definition.root().fields().values().stream()
                .map(field -> new StorageColumn(field.name(), storageType(field.kind()), field.isId()))
                .toList();
    }

    private static StorageType storageType(FieldKind kind) {
        return switch (kind) {
            case STRING  -> StorageType.TEXT;
            case INT     -> StorageType.INT;
            case LONG    -> StorageType.LONG;
            case DOUBLE  -> StorageType.DOUBLE;
            case BOOLEAN -> StorageType.BOOLEAN;
            case OBJECT, LIST, MAP, ANY -> StorageType.JSON;
        };
    }

    // ---------------------------------------------------------------- requests

    @Override
    public boolean handleRequest(String source, RequestType type, NexusJsonDataContainer json) {
        boolean permitted = definition.access().permits(source, type);
        if (!permitted) {
            LOGGER.fine("[JsonModel/" + addonName() + "] request rejected by access rules, type="
                    + type + ", source=" + source);
        }
        return permitted;
    }

    // ---------------------------------------------------------------- schema

    @Override
    public String modelInit(String json) {
        try {
            return MAPPER.writeValueAsString(normalize(json));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[JsonModel/" + addonName() + "] modelInit error, json=" + json, e);
            return "{}";
        }
    }

    @Override
    public String modelInitComp(String json) {
        try {
            return MAPPER.writeValueAsString(normalize(json));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[JsonModel/" + addonName() + "] modelInitComp error, json=" + json, e);
            return json;
        }
    }

    private JsonNode normalize(String json) throws com.fasterxml.jackson.core.JsonProcessingException {
        JsonNode input = json == null || json.isBlank() ? null : MAPPER.readTree(json);
        return normalizer.apply(definition.root(), input);
    }

    // ---------------------------------------------------------------- metrics

    @Override
    public void pushMetrics(NexusJsonDataContainer currentData) {
        ModelDefinition.MetricsConfig metrics = definition.metrics();
        if (!metrics.enabled()) return;

        Point point = Point
                .measurement(metrics.resolveMeasurement(addonName()))
                .addTag(getIdFieldName(), getSpecificDbKeyFromJsonKeyToValue(currentData))
                .time(Instant.now(), WritePrecision.NS);

        Map<String, Object> fields = new HashMap<>();

        for (SchemaField field : definition.root().fields().values()) {
            if (field.isId() || field.metric() == null) continue;
            if (!currentData.containsKey(field.name())) continue;

            Object value = currentData.get(field.name(), Object.class);
            if (value == null) continue;

            String dataKey = field.metric().resolveName(field.name());
            if (field.metric().tag()) point.addTag(dataKey, value.toString());
            fields.put(dataKey, value);
        }

        if (fields.isEmpty()) return;

        point.addFields(fields);

        NexusApplication app = NexusApplication.getApplication();
        CompletableFuture.runAsync(() ->
                app.getInfluxDBManager().ifPresent(db -> db.write(point))
        );
    }

    @Override
    public String toString() {
        return "JsonModelAddon{" + definition.addonName() + " #" + definition.addonId()
                + " <- " + definition.sourceFile() + "}";
    }
}
