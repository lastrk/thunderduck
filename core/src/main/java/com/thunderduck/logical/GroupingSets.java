package com.thunderduck.logical;

import com.thunderduck.expression.Expression;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents grouping sets for multi-dimensional aggregation.
 *
 * <p>This class encapsulates the different types of grouping operations
 * supported in SQL (ROLLUP, CUBE, GROUPING SETS) and provides methods
 * to generate appropriate SQL syntax.
 *
 * <p><b>ROLLUP</b> generates hierarchical grouping sets:
 * <pre>
 * ROLLUP(a, b, c) generates:
 * - (a, b, c)
 * - (a, b)
 * - (a)
 * - ()
 * </pre>
 *
 * <p><b>CUBE</b> generates all possible combinations:
 * <pre>
 * CUBE(a, b) generates:
 * - (a, b)
 * - (a)
 * - (b)
 * - ()
 * </pre>
 *
 * <p><b>GROUPING SETS</b> allows explicit specification:
 * <pre>
 * GROUPING SETS((a, b), (a), (c)) generates exactly those 3 sets
 * </pre>
 *
 * <p><b>Example Usage:</b>
 * <pre>
 * // Create a ROLLUP grouping
 * GroupingSets rollup = GroupingSets.rollup(
 *     Arrays.asList(yearCol, monthCol, dayCol)
 * );
 *
 * // Create a CUBE grouping
 * GroupingSets cube = GroupingSets.cube(
 *     Arrays.asList(regionCol, productCol)
 * );
 *
 * // Create custom GROUPING SETS
 * List&lt;List&lt;Expression&gt;&gt; sets = Arrays.asList(
 *     Arrays.asList(regionCol, productCol),
 *     Arrays.asList(regionCol),
 *     Collections.emptyList()  // grand total
 * );
 * GroupingSets custom = GroupingSets.groupingSets(sets);
 * </pre>
 *
 * @see GroupingType
 * @see Aggregate
 * @since 1.0
 */
public class GroupingSets {

    /**
     * Maximum recommended dimensions for CUBE operations.
     * 2^10 = 1,024 grouping sets.
     */
    public static final int MAX_CUBE_DIMENSIONS = 10;

    private final GroupingType type;
    private final List<Expression> columns;
    private final List<List<Expression>> sets;

    /**
     * Creates a GroupingSets instance.
     *
     * @param type the grouping type
     * @param columns the grouping columns (for ROLLUP/CUBE)
     * @param sets the explicit grouping sets (for GROUPING SETS)
     */
    private GroupingSets(GroupingType type,
                        List<Expression> columns,
                        List<List<Expression>> sets) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.columns = columns != null
            ? Collections.unmodifiableList(new ArrayList<>(columns))
            : Collections.emptyList();
        this.sets = sets != null
            ? Collections.unmodifiableList(
                sets.stream()
                    .map(set -> Collections.unmodifiableList(new ArrayList<>(set)))
                    .toList())
            : Collections.emptyList();

        validate();
    }

    /**
     * Creates a ROLLUP grouping over the specified columns.
     *
     * <p>ROLLUP generates N+1 grouping sets for N columns, representing
     * a hierarchy from the most detailed level to the grand total.
     *
     * <p><b>Example:</b>
     * <pre>
     * GroupingSets.rollup(Arrays.asList(year, month, day))
     * // SQL: GROUP BY ROLLUP(year, month, day)
     * // Generates: (year, month, day), (year, month), (year), ()
     * </pre>
     *
     * @param columns the grouping columns in hierarchical order
     * @return a ROLLUP grouping sets instance
     * @throws NullPointerException if columns is null
     * @throws IllegalArgumentException if columns is empty
     */
    public static GroupingSets rollup(List<Expression> columns) {
        Objects.requireNonNull(columns, "columns must not be null");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("ROLLUP requires at least one column");
        }
        return new GroupingSets(GroupingType.ROLLUP, columns, null);
    }

    /**
     * Creates a CUBE grouping over the specified columns.
     *
     * <p>CUBE generates 2^N grouping sets for N columns, representing
     * all possible combinations of the grouping dimensions.
     *
     * <p><b>Warning:</b> Due to exponential growth, it is strongly
     * recommended to limit CUBE to 10 or fewer dimensions.
     *
     * <p><b>Example:</b>
     * <pre>
     * GroupingSets.cube(Arrays.asList(region, product))
     * // SQL: GROUP BY CUBE(region, product)
     * // Generates: (region, product), (region), (product), ()
     * </pre>
     *
     * @param columns the grouping columns
     * @return a CUBE grouping sets instance
     * @throws NullPointerException if columns is null
     * @throws IllegalArgumentException if columns is empty or exceeds max dimensions
     */
    public static GroupingSets cube(List<Expression> columns) {
        Objects.requireNonNull(columns, "columns must not be null");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("CUBE requires at least one column");
        }
        if (columns.size() > MAX_CUBE_DIMENSIONS) {
            throw new IllegalArgumentException(
                String.format("CUBE with %d dimensions exceeds maximum of %d " +
                            "(would generate %d grouping sets)",
                            columns.size(), MAX_CUBE_DIMENSIONS,
                            1 << columns.size()));
        }
        return new GroupingSets(GroupingType.CUBE, columns, null);
    }

    /**
     * Creates a GROUPING SETS with explicit grouping set specifications.
     *
     * <p>GROUPING SETS allows precise control over which combinations
     * of columns to group by. Each set can contain any subset of columns,
     * including an empty set for the grand total.
     *
     * <p><b>Example:</b>
     * <pre>
     * List&lt;List&lt;Expression&gt;&gt; sets = Arrays.asList(
     *     Arrays.asList(region, product),  // regional product totals
     *     Arrays.asList(region),           // regional totals only
     *     Collections.emptyList()          // grand total
     * );
     * GroupingSets.groupingSets(sets)
     * // SQL: GROUP BY GROUPING SETS((region, product), (region), ())
     * </pre>
     *
     * @param sets the explicit grouping sets
     * @return a GROUPING SETS instance
     * @throws NullPointerException if sets is null
     * @throws IllegalArgumentException if sets is empty
     */
    public static GroupingSets groupingSets(List<List<Expression>> sets) {
        Objects.requireNonNull(sets, "sets must not be null");
        if (sets.isEmpty()) {
            throw new IllegalArgumentException("GROUPING SETS requires at least one set");
        }
        return new GroupingSets(GroupingType.GROUPING_SETS, null, sets);
    }

    /**
     * Returns the grouping type.
     *
     * @return the grouping type
     */
    public GroupingType type() {
        return type;
    }

    /**
     * Returns the grouping columns (for ROLLUP/CUBE).
     *
     * @return an unmodifiable list of grouping columns
     */
    public List<Expression> columns() {
        return columns;
    }

    /**
     * Returns the explicit grouping sets (for GROUPING SETS).
     *
     * @return an unmodifiable list of grouping sets
     */
    public List<List<Expression>> sets() {
        return sets;
    }

    /**
     * Generates SQL for this grouping specification.
     *
     * <p>The SQL format depends on the grouping type:
     * <ul>
     *   <li>ROLLUP: {@code ROLLUP(col1, col2, ...)}</li>
     *   <li>CUBE: {@code CUBE(col1, col2, ...)}</li>
     *   <li>GROUPING SETS: {@code GROUPING SETS((set1), (set2), ...)}</li>
     * </ul>
     *
     * @return the SQL representation
     */
    public String toSQL() {
        switch (type) {
            case ROLLUP:
                return "ROLLUP(" + columnsToSQL(columns) + ")";

            case CUBE:
                return "CUBE(" + columnsToSQL(columns) + ")";

            case GROUPING_SETS:
                return "GROUPING SETS(" + setsToSQL() + ")";

            default:
                throw new IllegalStateException("Unknown grouping type: " + type);
        }
    }

    /**
     * Computes the number of grouping sets that will be generated.
     *
     * <p>For ROLLUP with N columns: N+1 sets
     * <p>For CUBE with N columns: 2^N sets
     * <p>For GROUPING SETS: the explicit number of sets
     *
     * @return the number of grouping sets
     */
    public int getGroupingSetCount() {
        switch (type) {
            case ROLLUP:
                return columns.size() + 1;

            case CUBE:
                return 1 << columns.size();  // 2^N

            case GROUPING_SETS:
                return sets.size();

            default:
                throw new IllegalStateException("Unknown grouping type: " + type);
        }
    }

    /**
     * Validates the grouping sets configuration.
     *
     * @throws IllegalArgumentException if the configuration is invalid
     */
    private void validate() {
        switch (type) {
            case ROLLUP:
            case CUBE:
                if (columns.isEmpty()) {
                    throw new IllegalArgumentException(
                        type + " requires at least one column");
                }
                if (columns.stream().anyMatch(Objects::isNull)) {
                    throw new IllegalArgumentException(
                        type + " columns must not contain null values");
                }
                break;

            case GROUPING_SETS:
                if (sets.isEmpty()) {
                    throw new IllegalArgumentException(
                        "GROUPING SETS requires at least one set");
                }
                for (int i = 0; i < sets.size(); i++) {
                    List<Expression> set = sets.get(i);
                    if (set == null) {
                        throw new IllegalArgumentException(
                            "GROUPING SETS must not contain null sets");
                    }
                    if (set.stream().anyMatch(Objects::isNull)) {
                        throw new IllegalArgumentException(
                            String.format("GROUPING SETS set %d contains null expressions", i));
                    }
                }
                break;

            default:
                throw new IllegalStateException("Unknown grouping type: " + type);
        }
    }

    /**
     * Converts a list of columns to SQL.
     *
     * @param columns the columns to convert
     * @return comma-separated SQL column expressions
     */
    private String columnsToSQL(List<Expression> columns) {
        return columns.stream()
            .map(Expression::toSQL)
            .collect(Collectors.joining(", "));
    }

    /**
     * Converts grouping sets to SQL.
     *
     * @return SQL representation of all grouping sets
     */
    private String setsToSQL() {
        return sets.stream()
            .map(this::setToSQL)
            .collect(Collectors.joining(", "));
    }

    /**
     * Converts a single grouping set to SQL.
     *
     * @param set the grouping set to convert
     * @return SQL representation like "(col1, col2)" or "()"
     */
    private String setToSQL(List<Expression> set) {
        if (set.isEmpty()) {
            return "()";
        }
        return "(" + columnsToSQL(set) + ")";
    }

    @Override
    public String toString() {
        return String.format("GroupingSets[type=%s, sets=%d]",
                           type, getGroupingSetCount());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupingSets that = (GroupingSets) o;
        return type == that.type &&
               Objects.equals(columns, that.columns) &&
               Objects.equals(sets, that.sets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, columns, sets);
    }
}
