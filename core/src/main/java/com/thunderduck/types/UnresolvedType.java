package com.thunderduck.types;

import java.util.Objects;

/**
 * Represents an unresolved data type that needs resolution during schema inference.
 *
 * <p>This type serves as a placeholder when the actual type cannot be determined
 * at expression construction time. It is used instead of StringType as a sentinel
 * to clearly distinguish between:
 * <ul>
 *   <li>Actual string types (StringType)</li>
 *   <li>Types that need resolution from context (UnresolvedType)</li>
 * </ul>
 *
 * <p>Common use cases:
 * <ul>
 *   <li>Function return types that depend on argument types</li>
 *   <li>Array/Map element types that need resolution from column schema</li>
 *   <li>Nested types within complex structures</li>
 * </ul>
 *
 * <p>UnresolvedType should be resolved to a concrete type during
 * {@code Project.inferSchema()} or similar schema inference phases.
 *
 * @see DataType
 */
public final class UnresolvedType implements DataType {

    private final String hint;

    /**
     * Creates an UnresolvedType with an optional hint describing what type is expected.
     *
     * @param hint a descriptive hint about the expected type (e.g., "array_element", "map_value")
     */
    public UnresolvedType(String hint) {
        this.hint = Objects.requireNonNull(hint, "hint must not be null");
    }

    /**
     * Creates an UnresolvedType without a hint.
     */
    public UnresolvedType() {
        this.hint = "unknown";
    }

    /**
     * Returns the hint describing what type is expected.
     *
     * @return the type hint
     */
    public String hint() {
        return hint;
    }

    @Override
    public String typeName() {
        return "unresolved(" + hint + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof UnresolvedType)) return false;
        UnresolvedType that = (UnresolvedType) other;
        return Objects.equals(hint, that.hint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hint);
    }

    @Override
    public String toString() {
        return typeName();
    }

    // ==================== Factory Methods ====================

    /**
     * Creates an UnresolvedType for an array element.
     */
    public static UnresolvedType arrayElement() {
        return new UnresolvedType("array_element");
    }

    /**
     * Creates an UnresolvedType for a map key.
     */
    public static UnresolvedType mapKey() {
        return new UnresolvedType("map_key");
    }

    /**
     * Creates an UnresolvedType for a map value.
     */
    public static UnresolvedType mapValue() {
        return new UnresolvedType("map_value");
    }

    /**
     * Creates an UnresolvedType for a function return value.
     */
    public static UnresolvedType functionReturn() {
        return new UnresolvedType("function_return");
    }

    /**
     * Creates an UnresolvedType for a struct field.
     */
    public static UnresolvedType structField() {
        return new UnresolvedType("struct_field");
    }

    /**
     * Creates an UnresolvedType for a raw SQL expression string.
     */
    public static UnresolvedType expressionString() {
        return new UnresolvedType("expression_string");
    }

    /**
     * Checks if a DataType is unresolved (needs resolution).
     *
     * @param type the type to check
     * @return true if the type is UnresolvedType
     */
    public static boolean isUnresolved(DataType type) {
        return type instanceof UnresolvedType;
    }

    /**
     * Checks if a DataType contains any unresolved types (including nested).
     *
     * @param type the type to check
     * @return true if the type contains any UnresolvedType
     */
    public static boolean containsUnresolved(DataType type) {
        return switch (type) {
            case UnresolvedType u -> true;
            case ArrayType a      -> containsUnresolved(a.elementType());
            case MapType m        -> containsUnresolved(m.keyType()) || containsUnresolved(m.valueType());
            default               -> false;
        };
    }
}
