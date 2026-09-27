package network.darkland.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import network.darkland.cache.CacheMetrics;
import network.darkland.Influxdb.annotations.NexusMetric;
import network.darkland.Influxdb.annotations.NexusMetricConfig;
import network.darkland.NexusApplication;
import network.darkland.db.StorageColumn;
import network.darkland.db.StorageType;
import network.darkland.model.DataModel;
import network.darkland.protocol.backup.annotations.DbDataModels;
import network.darkland.protocol.handlers.GetDataHandler;
import network.darkland.protocol.handlers.IncrementDataHandler;
import network.darkland.protocol.handlers.RankFinderHandler;
import network.darkland.protocol.handlers.RankingHandler;
import network.darkland.protocol.handlers.RemoveDataHandler;
import network.darkland.protocol.handlers.SetDataHandler;
import network.darkland.redis.security.MessageValidationChain;
import network.darkland.redis.security.MessageValidator;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class DataAddon {

    protected static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private static final Logger LOGGER = Logger.getLogger(DataAddon.class.getName());

    private volatile String  cachedIdFieldName = null;
    private volatile Class<?> cachedIdClass    = null;
    private final Object idCacheLock = new Object();

    private volatile Field[] cachedAnnotatedFields = null;
    private final Object fieldCacheLock = new Object();

    private static final class KeyLock {
        int users;
    }
    private final ConcurrentHashMap<String, KeyLock> keyLocks = new ConcurrentHashMap<>();

    private volatile MessageValidationChain additionalValidationChain = null;
    private final Object validationChainLock = new Object();

    private final ConcurrentHashMap<RequestType, RequestHandler> handlers = new ConcurrentHashMap<>();

    protected DataAddon() {
        registerDefaultHandlers();
    }

    protected void registerDefaultHandlers() {
        registerHandler(RequestType.GET_DATA,       new GetDataHandler());
        registerHandler(RequestType.SET_DATA,       new SetDataHandler());
        registerHandler(RequestType.REMOVE_DATA,    new RemoveDataHandler());
        registerHandler(RequestType.INCREMENT_DATA, new IncrementDataHandler());
        registerHandler(RequestType.RANKING,        new RankingHandler());
        registerHandler(RequestType.RANK_FINDER,    new RankFinderHandler());
    }

    protected final void registerHandler(RequestType type, RequestHandler handler) {
        if (type == null || handler == null) {
            throw new IllegalArgumentException("type and handler cannot be null");
        }
        handlers.put(type, handler);
    }

    public final void dispatch(String source, RequestType type, NexusJsonDataContainer json) {
        RequestHandler handler = handlers.get(type);
        if (handler == null) {
            LOGGER.warning("[DataAddon/" + addonName() + "] No registered handler found, type=" + type);
            return;
        }
        try {
            handler.handle(this, source, json);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[DataAddon/" + addonName() + "] The handler threw an error; type=" + type, e);
            throw new IllegalStateException("Request handler failed", e);
        }
    }

    public final Set<RequestType> supportedRequestTypes() {
        return Collections.unmodifiableSet(handlers.keySet());
    }

    public abstract boolean handleRequest(String source, RequestType type, NexusJsonDataContainer json);
    public abstract int     addonId();
    public abstract String  addonName();
    public abstract String  cacheKeyHeaderTag();

    public abstract String  getNamespace();

    public abstract String  getDataset();

    public abstract int     getCacheTTL();

    public boolean l1CacheEnabled() {
        return true;
    }

    protected List<MessageValidator> additionalValidators() {
        return List.of();
    }

    public final MessageValidationChain getAdditionalValidationChain() {
        MessageValidationChain chain = additionalValidationChain;
        if (chain != null) return chain;

        synchronized (validationChainLock) {
            if (additionalValidationChain == null) {
                additionalValidationChain = new MessageValidationChain(additionalValidators());
            }
            return additionalValidationChain;
        }
    }

    public final Object acquireKeyLock(String keyValue) {
        return keyLocks.compute(keyValue, (key, current) -> {
            KeyLock lock = current == null ? new KeyLock() : current;
            lock.users++;
            return lock;
        });
    }

    public final void releaseKeyLock(String keyValue, Object lock) {
        keyLocks.computeIfPresent(keyValue, (key, current) -> {
            if (current != lock) return current;
            return --current.users == 0 ? null : current;
        });
    }

    public final void withKeyLock(NexusJsonDataContainer json, Runnable task) {
        NexusJsonDataContainer data = json;
        try {
            if (json.containsKey("data")) {
                data = new NexusJsonDataContainer(network.darkland.util.JsonUtils.toJson(json.get("data", Object.class)));
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) { throw new IllegalArgumentException(e); }
        String key = getSpecificDbKeyFromJsonKeyToValue(data);
        Object lock = acquireKeyLock(key);
        try { synchronized (lock) { task.run(); } }
        finally { releaseKeyLock(key, lock); }
    }

    public void pushMetrics(NexusJsonDataContainer currentData) {
        NexusMetricConfig metricConfig = getClass().getAnnotation(NexusMetricConfig.class);
        if (metricConfig == null || !metricConfig.enabled()) return;

        String measurement = metricConfig.customMeasurement().isEmpty()
                ? getClass().getName()
                : metricConfig.customMeasurement();

        Point point = Point
                .measurement(measurement)
                .addTag(getIdFieldName(), getSpecificDbKeyFromJsonKeyToValue(currentData))
                .time(Instant.now(), WritePrecision.NS);

        HashMap<String, Object> fields = new HashMap<>();
        String idFieldName = getIdFieldName();

        for (Field field : getClass().getDeclaredFields()) {
            if (field.getName().equals(idFieldName)) continue;
            if (!field.isAnnotationPresent(NexusMetric.class)) continue;

            String fieldName = field.getName();
            if (!currentData.containsKey(fieldName)) continue;

            Object value       = currentData.get(fieldName, Object.class);
            NexusMetric metric = field.getAnnotation(NexusMetric.class);
            String dataKey     = metric.value().isEmpty() ? fieldName : metric.value();

            if (metric.isTag()) point.addTag(dataKey, value.toString());
            fields.put(dataKey, value);
        }

        point.addFields(fields);

        NexusApplication app = NexusApplication.getApplication();
        CompletableFuture.runAsync(() ->
                app.getInfluxDBManager().ifPresent(db -> db.write(point))
        );
    }

    public void loadIntoCache(Object key) {
        Class<?> expectedType = getIdClassName();
        if (expectedType == null || !expectedType.isInstance(key)) return;

        String keyTag        = cacheKeyHeaderTag() + "_" + key;
        NexusApplication app = NexusApplication.getApplication();

        if (app.getDataContainer().getDataModelFromKey(keyTag).isPresent()) return;

        NexusJsonDataContainer trigger = new NexusJsonDataContainer();
        trigger.set(getIdFieldName(), key);
        getData(trigger);
    }

    public String modelInit(String json) {
        com.fasterxml.jackson.databind.node.ObjectNode outputNode = MAPPER.createObjectNode();
        try {
            JsonNode inputNode = MAPPER.readTree(json);
            for (Field field : getAnnotatedFields()) {
                String       fieldName   = field.getName();
                DbDataModels anno        = field.getAnnotation(DbDataModels.class);
                Object       targetValue = convertToType(anno.defaultValue(), field.getType());

                if (inputNode.has(fieldName) && !inputNode.get(fieldName).isNull()) {
                    targetValue = MAPPER.readerForUpdating(targetValue).readValue(inputNode.get(fieldName));
                }

                outputNode.set(fieldName,
                        targetValue != null ? MAPPER.valueToTree(targetValue) : MAPPER.createObjectNode());
            }
            return MAPPER.writeValueAsString(outputNode);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[DataAddon/" + addonName() + "] modelInit error, json=" + json, e);
            return "{}";
        }
    }

    public String modelInitComp(String json) {
        try {
            JsonNode rootNode = MAPPER.readTree(json);
            com.fasterxml.jackson.databind.node.ObjectNode updatedNode = MAPPER.createObjectNode();

            for (Field field : getAnnotatedFields()) {
                String       fieldName = field.getName();
                DbDataModels anno      = field.getAnnotation(DbDataModels.class);
                Object       baseObj   = convertToType(anno.defaultValue(), field.getType());

                if (rootNode.has(fieldName) && !rootNode.get(fieldName).isNull()) {
                    baseObj = MAPPER.readerForUpdating(baseObj).readValue(rootNode.get(fieldName));
                }
                updatedNode.set(fieldName, MAPPER.valueToTree(baseObj));
            }
            return MAPPER.writeValueAsString(updatedNode);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[DataAddon/" + addonName() + "] modelInitComp error, json=" + json, e);
            return json;
        }
    }

    private Field[] getAnnotatedFields() {
        if (cachedAnnotatedFields != null) return cachedAnnotatedFields;
        synchronized (fieldCacheLock) {
            if (cachedAnnotatedFields != null) return cachedAnnotatedFields;
            cachedAnnotatedFields = Arrays.stream(getClass().getDeclaredFields())
                    .filter(f -> f.isAnnotationPresent(DbDataModels.class))
                    .peek(f -> f.setAccessible(true))
                    .toArray(Field[]::new);
        }
        return cachedAnnotatedFields;
    }

    private Object convertToType(String value, Class<?> type) {
        boolean blank = value == null || value.isEmpty() || value.equals("{}");

        if (blank) {
            if (type == String.class)                            return "";
            if (type == int.class     || type == Integer.class)  return 0;
            if (type == long.class    || type == Long.class)     return 0L;
            if (type == double.class  || type == Double.class)   return 0.0;
            if (type == boolean.class || type == Boolean.class)  return false;
            try {
                return type.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                try   { return MAPPER.readValue("{}", type); }
                catch (Exception ex) {
                    LOGGER.warning("[DataAddon/" + addonName() + "] convertToType: default üretilemedi -> " + type.getSimpleName());
                    return null;
                }
            }
        }

        if (type == String.class)                            return value;
        if (type == int.class     || type == Integer.class)  return Integer.parseInt(value);
        if (type == long.class    || type == Long.class)     return Long.parseLong(value);
        if (type == double.class  || type == Double.class)   return Double.parseDouble(value);
        if (type == boolean.class || type == Boolean.class)  return Boolean.parseBoolean(value);

        try {
            return MAPPER.readValue(value, type);
        } catch (Exception e) {
            LOGGER.warning("[DataAddon/" + addonName() + "] convertToType parse error: "
                    + type.getSimpleName() + " value=" + value);
            return null;
        }
    }

    /**
     * Top-level fields of this model, used by relational stores to lay the data out as
     * a regular table (one column per field) instead of a single JSON document.
     */
    public List<StorageColumn> storageColumns() {
        return Arrays.stream(getAnnotatedFields())
                .map(f -> new StorageColumn(f.getName(), StorageType.fromJavaType(f.getType()),
                        f.getAnnotation(DbDataModels.class).isId()))
                .toList();
    }

    public String getIdFieldName() {
        if (cachedIdFieldName != null) return cachedIdFieldName;
        synchronized (idCacheLock) {
            if (cachedIdFieldName != null) return cachedIdFieldName;
            for (Field f : getClass().getDeclaredFields()) {
                if (f.isAnnotationPresent(DbDataModels.class) && f.getAnnotation(DbDataModels.class).isId()) {
                    cachedIdFieldName = f.getName();
                    cachedIdClass     = f.getType();
                    return cachedIdFieldName;
                }
            }
            cachedIdFieldName = "";
        }
        return cachedIdFieldName;
    }

    public Class<?> getIdClassName() {
        if (cachedIdClass != null) return cachedIdClass;
        getIdFieldName();
        return cachedIdClass;
    }

    public Optional<DataModel> getData(NexusJsonDataContainer dataContainer) {
        try {
            NexusJsonDataContainer work = dataContainer.containsKey("data")
                    ? new NexusJsonDataContainer(network.darkland.util.JsonUtils.toJson(dataContainer.get("data", Object.class)))
                    : dataContainer;

            String specificValue = getSpecificDbKeyFromJsonKeyToValue(work);
            if (specificValue.isEmpty() || "null".equals(specificValue)) return Optional.empty();

            String           keyTag = cacheKeyHeaderTag() + "_" + specificValue;
            NexusApplication app    = NexusApplication.getApplication();
            var              redis  = app.getRedisManager();

            Optional<DataModel> pending = app.getDataContainer().getPendingModel(keyTag);
            if (pending.isPresent()) return pending;

            Optional<DataModel> l1 = app.getDataContainer().getDataModelFromKey(keyTag);
            if (l1.isPresent()) {
                CacheMetrics.get().recordL1Hit(cacheKeyHeaderTag());
                pushMetrics(new NexusJsonDataContainer(l1.get().getValueJson()));
                return l1;
            }

            Optional<String> redisOpt = redis.getData(keyTag);
            if (redisOpt.isPresent()) {
                String redisJson = redisOpt.get();

                if (!redisJson.trim().startsWith("{")) {
                    LOGGER.warning("[DataAddon/" + addonName() + "] Corrupt data detected in Redis; deleting key.=" + keyTag);
                    redis.deleteData(keyTag);
                } else {
                    CacheMetrics.get().recordL2Hit(cacheKeyHeaderTag());
                    DataModel m = new DataModel(keyTag, UUID.randomUUID().toString(),
                            modelInitComp(redisJson), this, specificValue);
                    app.getDataContainer().cacheModel(keyTag, m);
                    pushMetrics(new NexusJsonDataContainer(m.getValueJson()));
                    return Optional.of(m);
                }
            }

            String dbJson = app.getDataStore().getValue(this, specificValue).join();
            if (dbJson != null) {
                CacheMetrics.get().recordL3Hit(cacheKeyHeaderTag());
                DataModel m = new DataModel(keyTag, UUID.randomUUID().toString(),
                        modelInitComp(dbJson), this, specificValue);
                app.getDataContainer().cacheModel(keyTag, m);
                pushMetrics(new NexusJsonDataContainer(m.getValueJson()));
                return Optional.of(m);
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[DataAddon/" + addonName() + "] getData error", e);
            throw new IllegalStateException("Data lookup failed; the record may still exist", e);
        }
        return Optional.empty();
    }

    public String getSpecificDbKeyFromJsonKeyToValue(NexusJsonDataContainer dataContainer) {
        String idName = getIdFieldName();
        if (idName.isEmpty()) return "";
        Object val = dataContainer.get(idName, Object.class);
        return val != null ? val.toString() : "";
    }

    public String generateRawJson(String idValue) {
        String idName = getIdFieldName();
        return idName.isEmpty() ? "{}" : modelInit(network.darkland.util.NexusJsonBuilder.create().add(idName, idValue).build());
    }

    public DataModel createModel(String json) throws com.fasterxml.jackson.core.JsonProcessingException {
        NexusJsonDataContainer container = new NexusJsonDataContainer(json);
        String specificKey = getSpecificDbKeyFromJsonKeyToValue(container);
        String keyTag      = cacheKeyHeaderTag() + "_" + specificKey;
        return new DataModel(keyTag, UUID.randomUUID().toString(), json, this, specificKey);
    }
}
