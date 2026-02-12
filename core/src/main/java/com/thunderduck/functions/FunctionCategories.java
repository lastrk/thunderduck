package com.thunderduck.functions;

import java.util.Set;

/**
 * Centralized function categorization to eliminate duplicate hardcoded patterns.
 *
 * <p>This class provides shared constants for function categories that need
 * consistent handling across ExpressionConverter, Project, and FunctionRegistry.
 *
 * <p>Categories are based on how functions behave with respect to:
 * <ul>
 *   <li>Return type inference (type-preserving, boolean-returning, etc.)</li>
 *   <li>Nullable inference (null-coalescing behavior)</li>
 *   <li>Array/Map type handling</li>
 * </ul>
 */
public final class FunctionCategories {

    private FunctionCategories() {
        // Utility class - prevent instantiation
    }

    /**
     * Array functions that preserve the input array's element type.
     * These return ArrayType with the same element type as their first argument.
     */
    public static final Set<String> ARRAY_TYPE_PRESERVING = Set.of(
        "array_sort", "sort_array", "array_distinct", "reverse", "slice", "shuffle"
    );

    /**
     * Array set operations that return ArrayType.
     * These combine two arrays and return an array with the same element type.
     */
    public static final Set<String> ARRAY_SET_OPERATIONS = Set.of(
        "array_union", "array_intersect", "array_except"
    );

    /**
     * Functions with null-coalescing behavior.
     * Result is non-null if ANY argument is non-null.
     * (Only nullable if ALL arguments are nullable)
     *
     * <p>Note: nvl2, nullif, and if have different nullable semantics and are NOT included:
     * - nvl2(a,b,c): returns b if a non-null, else c - nullable depends on both b AND c
     * - nullif(a,b): returns null if a==b - can be null even with non-null inputs
     * - if(cond,a,b): returns a or b - nullable depends on both branches
     */
    public static final Set<String> NULL_COALESCING = Set.of(
        "greatest", "least", "coalesce", "nvl", "ifnull"
    );

    /**
     * Scalar functions that preserve their first argument's type.
     * The return type matches the type of the first argument.
     *
     * <p>Includes:
     * - Math functions: abs, negative, positive, unary_minus, unary_plus, mod, pmod
     * - Comparison functions: greatest, least
     * - Null-handling functions: coalesce, nvl, nvl2, ifnull, nullif, if
     */
    public static final Set<String> TYPE_PRESERVING = Set.of(
        "abs", "negative", "positive", "unary_minus", "unary_plus",
        "greatest", "least", "mod", "pmod",
        "coalesce", "nvl", "nvl2", "ifnull", "nullif", "if"
    );

    /**
     * Functions that always return BooleanType.
     */
    public static final Set<String> BOOLEAN_RETURNING = Set.of(
        "array_contains", "arrays_overlap", "isnull", "isnotnull", "isnan",
        "contains", "startswith", "endswith", "like", "rlike", "regexp", "regexp_like"
    );

    /**
     * Functions that return IntegerType (not LongType).
     * Position functions in Spark return Int, not Long.
     */
    public static final Set<String> INTEGER_RETURNING = Set.of(
        "instr", "locate", "position"
    );

    /**
     * Map key/value extraction functions.
     * map_keys returns ArrayType of key type, map_values returns ArrayType of value type.
     */
    public static final Set<String> MAP_EXTRACTION = Set.of(
        "map_keys", "map_values"
    );

    /**
     * Element extraction functions (from arrays or maps).
     */
    public static final Set<String> ELEMENT_EXTRACTION = Set.of(
        "element_at"
    );

    /**
     * Explode functions that unnest arrays.
     */
    public static final Set<String> EXPLODE_FUNCTIONS = Set.of(
        "explode", "explode_outer"
    );

    /**
     * Check if a function belongs to a category.
     *
     * @param funcName the function name (case-insensitive)
     * @param category the category set to check against
     * @return true if the function is in the category
     */
    public static boolean isInCategory(String funcName, Set<String> category) {
        return category.contains(funcName.toLowerCase());
    }

    /**
     * Check if a function is an array type-preserving function.
     */
    public static boolean isArrayTypePreserving(String funcName) {
        return isInCategory(funcName, ARRAY_TYPE_PRESERVING);
    }

    /**
     * Check if a function is an array set operation.
     */
    public static boolean isArraySetOperation(String funcName) {
        return isInCategory(funcName, ARRAY_SET_OPERATIONS);
    }

    /**
     * Check if a function has null-coalescing behavior.
     */
    public static boolean isNullCoalescing(String funcName) {
        return isInCategory(funcName, NULL_COALESCING);
    }

    /**
     * Check if a function preserves its first argument's type.
     */
    public static boolean isTypePreserving(String funcName) {
        return isInCategory(funcName, TYPE_PRESERVING);
    }

    /**
     * Check if a function returns BooleanType.
     */
    public static boolean isBooleanReturning(String funcName) {
        return isInCategory(funcName, BOOLEAN_RETURNING);
    }

    /**
     * Check if a function returns IntegerType.
     */
    public static boolean isIntegerReturning(String funcName) {
        return isInCategory(funcName, INTEGER_RETURNING);
    }

    /**
     * Check if a function is a map extraction function.
     */
    public static boolean isMapExtraction(String funcName) {
        return isInCategory(funcName, MAP_EXTRACTION);
    }

    /**
     * Check if a function is an element extraction function.
     */
    public static boolean isElementExtraction(String funcName) {
        return isInCategory(funcName, ELEMENT_EXTRACTION);
    }

    /**
     * Check if a function is an explode function.
     */
    public static boolean isExplodeFunction(String funcName) {
        return isInCategory(funcName, EXPLODE_FUNCTIONS);
    }
}
