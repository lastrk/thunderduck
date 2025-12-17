package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;
import java.util.Objects;

/**
 * Expression that extracts a value from a complex type (struct, array, or map).
 *
 * <p>Handles three extraction modes:
 * <ul>
 *   <li>Struct field: {@code struct.field} or {@code struct['field']}</li>
 *   <li>Array element: {@code list[index]} (1-based in DuckDB)</li>
 *   <li>Map key: {@code element_at(map, key)}</li>
 * </ul>
 *
 * <p>Examples:
 * <pre>
 *   person.name           -- struct field access
 *   arr[1]                -- array element (1-based in DuckDB)
 *   element_at(map, 'k')  -- map key access
 * </pre>
 *
 * <p>Note: PySpark uses 0-based indexing for arrays, DuckDB uses 1-based.
 * The converter handles this translation before creating this expression.
 */
public class ExtractValueExpression extends Expression {

    /**
     * The type of extraction being performed.
     */
    public enum ExtractionType {
        /** Extract a field from a struct by name */
        STRUCT_FIELD,
        /** Extract an element from an array by index */
        ARRAY_INDEX,
        /** Extract a value from a map by key */
        MAP_KEY
    }

    private final Expression child;
    private final Expression extraction;
    private final ExtractionType extractionType;
    private final DataType resultType;

    /**
     * Creates an ExtractValueExpression.
     *
     * @param child the expression to extract from (struct/array/map)
     * @param extraction the extraction specifier (field name/index/key)
     * @param extractionType the type of extraction
     * @param resultType the resulting data type
     */
    public ExtractValueExpression(Expression child, Expression extraction,
                                   ExtractionType extractionType, DataType resultType) {
        this.child = Objects.requireNonNull(child, "child must not be null");
        this.extraction = Objects.requireNonNull(extraction, "extraction must not be null");
        this.extractionType = Objects.requireNonNull(extractionType, "extractionType must not be null");
        this.resultType = Objects.requireNonNull(resultType, "resultType must not be null");
    }

    /**
     * Creates an ExtractValueExpression with inferred string type.
     *
     * @param child the expression to extract from
     * @param extraction the extraction specifier
     * @param extractionType the type of extraction
     */
    public ExtractValueExpression(Expression child, Expression extraction, ExtractionType extractionType) {
        this(child, extraction, extractionType, StringType.get());
    }

    /**
     * Returns the child expression (the collection being extracted from).
     *
     * @return the child expression
     */
    public Expression child() {
        return child;
    }

    /**
     * Returns the extraction expression (index/key/field name).
     *
     * @return the extraction expression
     */
    public Expression extraction() {
        return extraction;
    }

    /**
     * Returns the type of extraction.
     *
     * @return the extraction type
     */
    public ExtractionType extractionType() {
        return extractionType;
    }

    @Override
    public DataType dataType() {
        return resultType;
    }

    @Override
    public boolean nullable() {
        // Extraction can always produce null (missing key, out of bounds, etc.)
        return true;
    }

    @Override
    public String toSQL() {
        String childSql = child.toSQL();
        String extractionSql = extraction.toSQL();

        switch (extractionType) {
            case STRUCT_FIELD:
                // Use bracket notation which works for both structs and maps
                // DuckDB supports struct['field'] syntax which is equivalent to struct.field
                // This also works for maps, making it a universal solution
                if (extraction instanceof Literal) {
                    Object value = ((Literal) extraction).value();
                    if (value instanceof String) {
                        // Use bracket notation with quoted string key
                        return String.format("%s['%s']", childSql, value);
                    }
                }
                // Dynamic field access - use bracket notation
                return String.format("%s[%s]", childSql, extractionSql);

            case ARRAY_INDEX:
                // Array indexing uses bracket notation
                // Note: Index should already be 1-based (converted by ExpressionConverter)
                return String.format("%s[%s]", childSql, extractionSql);

            case MAP_KEY:
                // Map key access also uses bracket notation
                // element_at can be used but bracket notation is simpler and works well
                if (extraction instanceof Literal) {
                    Object value = ((Literal) extraction).value();
                    if (value instanceof String) {
                        return String.format("%s['%s']", childSql, value);
                    }
                }
                return String.format("%s[%s]", childSql, extractionSql);

            default:
                throw new IllegalStateException("Unknown extraction type: " + extractionType);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ExtractValueExpression)) return false;
        ExtractValueExpression that = (ExtractValueExpression) obj;
        return Objects.equals(child, that.child) &&
               Objects.equals(extraction, that.extraction) &&
               extractionType == that.extractionType &&
               Objects.equals(resultType, that.resultType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child, extraction, extractionType, resultType);
    }

    // ==================== Factory Methods ====================

    /**
     * Creates an expression for struct field access.
     *
     * @param struct the struct expression
     * @param fieldName the field name to extract
     * @return the extract value expression
     */
    public static ExtractValueExpression structField(Expression struct, String fieldName) {
        return new ExtractValueExpression(
            struct,
            Literal.of(fieldName),
            ExtractionType.STRUCT_FIELD
        );
    }

    /**
     * Creates an expression for struct field access with dynamic field.
     *
     * @param struct the struct expression
     * @param fieldExpr the field name expression
     * @return the extract value expression
     */
    public static ExtractValueExpression structField(Expression struct, Expression fieldExpr) {
        return new ExtractValueExpression(struct, fieldExpr, ExtractionType.STRUCT_FIELD);
    }

    /**
     * Creates an expression for array element access.
     * Note: The index should already be 1-based for DuckDB.
     *
     * @param array the array expression
     * @param index the 1-based index expression
     * @return the extract value expression
     */
    public static ExtractValueExpression arrayElement(Expression array, Expression index) {
        return new ExtractValueExpression(array, index, ExtractionType.ARRAY_INDEX);
    }

    /**
     * Creates an expression for array element access with literal index.
     * Note: The index should already be 1-based for DuckDB.
     *
     * @param array the array expression
     * @param index the 1-based index
     * @return the extract value expression
     */
    public static ExtractValueExpression arrayElement(Expression array, int index) {
        return new ExtractValueExpression(
            array,
            Literal.of(index),
            ExtractionType.ARRAY_INDEX
        );
    }

    /**
     * Creates an expression for map key access.
     *
     * @param map the map expression
     * @param key the key expression
     * @return the extract value expression
     */
    public static ExtractValueExpression mapKey(Expression map, Expression key) {
        return new ExtractValueExpression(map, key, ExtractionType.MAP_KEY);
    }

    /**
     * Creates an expression for map key access with literal string key.
     *
     * @param map the map expression
     * @param key the string key
     * @return the extract value expression
     */
    public static ExtractValueExpression mapKey(Expression map, String key) {
        return new ExtractValueExpression(
            map,
            Literal.of(key),
            ExtractionType.MAP_KEY
        );
    }
}
