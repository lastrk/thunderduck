package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.MapType;
import com.thunderduck.types.StringType;
import com.thunderduck.types.TypeInferenceEngine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Expression representing a map literal.
 *
 * <p>Map literals contain key-value pairs with unified key and value types.
 * This class preserves the key and value lists to enable proper type inference
 * by examining and unifying the types of all keys and values.
 *
 * <p>SQL form: MAP([key1, key2], [val1, val2])
 * <p>Spark form: map_from_arrays(array(k1, k2), array(v1, v2))
 *
 * <p>Type inference:
 * <ul>
 *   <li>Empty map: MapType(StringType, StringType, true)</li>
 *   <li>Non-empty: MapType(unifiedKeyType, unifiedValueType, true)</li>
 * </ul>
 */
public final class MapLiteralExpression implements Expression {

    private final List<Expression> keys;
    private final List<Expression> values;

    /**
     * Creates a map literal expression.
     *
     * @param keys the map keys (must match values size)
     * @param values the map values
     * @throws IllegalArgumentException if keys and values have different sizes
     */
    public MapLiteralExpression(List<Expression> keys, List<Expression> values) {
        Objects.requireNonNull(keys, "keys must not be null");
        Objects.requireNonNull(values, "values must not be null");

        if (keys.size() != values.size()) {
            throw new IllegalArgumentException(
                "keys and values must have the same size: " +
                keys.size() + " vs " + values.size());
        }

        this.keys = new ArrayList<>(keys);
        this.values = new ArrayList<>(values);
    }

    /**
     * Returns the map keys.
     *
     * @return an unmodifiable list of key expressions
     */
    public List<Expression> keys() {
        return Collections.unmodifiableList(keys);
    }

    /**
     * Returns the map values.
     *
     * @return an unmodifiable list of value expressions
     */
    public List<Expression> values() {
        return Collections.unmodifiableList(values);
    }

    /**
     * Returns the number of key-value pairs.
     *
     * @return entry count
     */
    public int size() {
        return keys.size();
    }

    /**
     * Returns whether the map is empty.
     *
     * @return true if no entries
     */
    public boolean isEmpty() {
        return keys.isEmpty();
    }

    /**
     * Returns the data type of this map literal.
     *
     * <p>Infers the key and value types by unifying all key and value types
     * respectively. Empty maps default to MapType(StringType, StringType, true).
     * The valueContainsNull flag is computed based on actual value nullability.
     *
     * @return MapType with inferred key and value types and computed valueContainsNull
     */
    @Override
    public DataType dataType() {
        if (keys.isEmpty()) {
            // Empty map defaults to StringType for both key and value
            return new MapType(StringType.get(), StringType.get(), true);
        }

        // Unify key types
        DataType keyType = keys.get(0).dataType();
        for (int i = 1; i < keys.size(); i++) {
            DataType nextType = keys.get(i).dataType();
            keyType = TypeInferenceEngine.unifyTypes(keyType, nextType);
        }

        // Unify value types
        DataType valueType = values.get(0).dataType();
        for (int i = 1; i < values.size(); i++) {
            DataType nextType = values.get(i).dataType();
            valueType = TypeInferenceEngine.unifyTypes(valueType, nextType);
        }

        return new MapType(keyType, valueType, valueContainsNull());
    }

    /**
     * Returns whether this map can be null.
     *
     * <p>Map literals themselves are not null (they're constant values),
     * but they may contain null values (keys cannot be null in Spark/DuckDB).
     *
     * @return false (map literals are not null)
     */
    @Override
    public boolean nullable() {
        return false;
    }

    /**
     * Returns whether any value in the map is nullable.
     *
     * @return true if any value is nullable
     */
    public boolean valueContainsNull() {
        return values.stream().anyMatch(Expression::nullable);
    }

    /**
     * Generates the SQL representation of this map literal.
     *
     * @return SQL string in the form "MAP([key1, key2], [val1, val2])"
     */
    @Override
    public String toSQL() {
        String keysSQL = keys.stream()
            .map(Expression::toSQL)
            .collect(Collectors.joining(", "));
        String valuesSQL = values.stream()
            .map(Expression::toSQL)
            .collect(Collectors.joining(", "));
        return "MAP([" + keysSQL + "], [" + valuesSQL + "])";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof MapLiteralExpression)) return false;
        MapLiteralExpression that = (MapLiteralExpression) obj;
        return Objects.equals(keys, that.keys) &&
               Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keys, values);
    }

    @Override
    public String toString() {
        return "MapLiteral(" + keys.size() + " entries)";
    }
}
