package com.thunderduck.expression;

import com.thunderduck.types.ArrayType;
import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;
import com.thunderduck.types.TypeInferenceEngine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Expression representing an array literal.
 *
 * <p>Array literals contain zero or more elements of a common type. This class
 * preserves the element list to enable proper type inference by examining
 * and unifying the types of all elements.
 *
 * <p>SQL form: [elem1, elem2, elem3]
 * <p>Spark form: array(1, 2, 3) or lit([1, 2, 3])
 *
 * <p>Type inference:
 * <ul>
 *   <li>Empty array: ArrayType(StringType, true) - follows Spark convention</li>
 *   <li>Non-empty: ArrayType(unifiedElementType, true)</li>
 * </ul>
 */
public final class ArrayLiteralExpression implements Expression {

    private final List<Expression> elements;

    /**
     * Creates an array literal expression.
     *
     * @param elements the array elements (may be empty)
     */
    public ArrayLiteralExpression(List<Expression> elements) {
        Objects.requireNonNull(elements, "elements must not be null");
        this.elements = new ArrayList<>(elements);
    }

    /**
     * Returns the array elements.
     *
     * @return an unmodifiable list of element expressions
     */
    public List<Expression> elements() {
        return Collections.unmodifiableList(elements);
    }

    /**
     * Returns the number of elements in the array.
     *
     * @return element count
     */
    public int size() {
        return elements.size();
    }

    /**
     * Returns whether the array is empty.
     *
     * @return true if no elements
     */
    public boolean isEmpty() {
        return elements.isEmpty();
    }

    /**
     * Returns the data type of this array literal.
     *
     * <p>Infers the element type by unifying all element types. Empty arrays
     * default to ArrayType(StringType, true) following Spark conventions.
     * The containsNull flag is computed based on actual element nullability.
     *
     * @return ArrayType with inferred element type and computed containsNull
     */
    @Override
    public DataType dataType() {
        if (elements.isEmpty()) {
            // Empty array defaults to StringType element type (Spark convention)
            return new ArrayType(StringType.get(), true);
        }

        // Unify element types
        DataType elementType = elements.get(0).dataType();
        for (int i = 1; i < elements.size(); i++) {
            DataType nextType = elements.get(i).dataType();
            elementType = TypeInferenceEngine.unifyTypes(elementType, nextType);
        }

        return new ArrayType(elementType, containsNullableElements());
    }

    /**
     * Returns whether this array can be null.
     *
     * <p>Array literals themselves are not null (they're constant values),
     * but they may contain null elements.
     *
     * @return false (array literals are not null)
     */
    @Override
    public boolean nullable() {
        return false;
    }

    /**
     * Returns whether any element in the array is nullable.
     *
     * @return true if any element is nullable
     */
    public boolean containsNullableElements() {
        return elements.stream().anyMatch(Expression::nullable);
    }

    /**
     * Generates the SQL representation of this array literal.
     *
     * @return SQL string in the form "[elem1, elem2, ...]"
     */
    @Override
    public String toSQL() {
        String elementsSQL = elements.stream()
            .map(Expression::toSQL)
            .collect(Collectors.joining(", "));
        return "[" + elementsSQL + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ArrayLiteralExpression)) return false;
        ArrayLiteralExpression that = (ArrayLiteralExpression) obj;
        return Objects.equals(elements, that.elements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elements);
    }

    @Override
    public String toString() {
        return "ArrayLiteral(" + elements.size() + " elements)";
    }
}
