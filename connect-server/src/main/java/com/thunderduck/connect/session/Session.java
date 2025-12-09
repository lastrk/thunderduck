package com.thunderduck.connect.session;

import com.thunderduck.logical.LogicalPlan;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a single Spark Connect session.
 *
 * Each session maintains:
 * - Unique session ID
 * - Session-scoped configuration
 * - Creation timestamp
 * - Temporary view registry
 */
public class Session {
    private final String sessionId;
    private final long createdAt;
    private final Map<String, String> config;
    private final Map<String, LogicalPlan> tempViews;

    /**
     * Create a new session with the given ID.
     *
     * @param sessionId Unique session identifier (typically UUID from client)
     */
    public Session(String sessionId) {
        this.sessionId = sessionId;
        this.createdAt = System.currentTimeMillis();
        this.config = new HashMap<>();
        this.tempViews = new ConcurrentHashMap<>();

        // Set default configuration
        config.putAll(SparkDefaults.getDefaults());
        config.put("spark.app.name", "thunderduck-connect");
        config.put("spark.sql.dialect", "spark");
    }

    /**
     * Get session ID.
     *
     * @return Session identifier
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Get session creation timestamp.
     *
     * @return Timestamp in milliseconds since epoch
     */
    public long getCreatedAt() {
        return createdAt;
    }

    /**
     * Get configuration value.
     *
     * @param key Configuration key
     * @return Configuration value, or null if not set
     */
    public String getConfig(String key) {
        return config.get(key);
    }

    /**
     * Set configuration value.
     *
     * @param key Configuration key
     * @param value Configuration value
     */
    public void setConfig(String key, String value) {
        config.put(key, value);
    }

    /**
     * Get all configuration entries.
     *
     * @return Immutable copy of configuration map
     */
    public Map<String, String> getAllConfig() {
        return new HashMap<>(config);
    }

    /**
     * Register a temporary view.
     *
     * @param name View name
     * @param plan Logical plan representing the view
     */
    public void registerTempView(String name, LogicalPlan plan) {
        tempViews.put(name, plan);
    }

    /**
     * Get a temporary view by name.
     *
     * @param name View name
     * @return Logical plan if view exists, empty otherwise
     */
    public Optional<LogicalPlan> getTempView(String name) {
        return Optional.ofNullable(tempViews.get(name));
    }

    /**
     * Drop a temporary view.
     *
     * @param name View name
     * @return true if view existed and was dropped, false otherwise
     */
    public boolean dropTempView(String name) {
        return tempViews.remove(name) != null;
    }

    /**
     * Get all temporary view names.
     *
     * @return Set of view names
     */
    public Set<String> getTempViewNames() {
        return tempViews.keySet();
    }

    /**
     * Clear all temporary views.
     */
    public void clearTempViews() {
        tempViews.clear();
    }

    @Override
    public String toString() {
        return String.format("Session[id=%s, created=%d, views=%d]",
            sessionId, createdAt, tempViews.size());
    }
}
