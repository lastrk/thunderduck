package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Expression representing an interval literal.
 *
 * <p>Interval expressions represent time spans. Spark has three interval types:
 * <ul>
 *   <li>YEAR_MONTH: months only (stored as total months)</li>
 *   <li>DAY_TIME: days, hours, minutes, seconds, microseconds (stored as total microseconds)</li>
 *   <li>CALENDAR: combination of months, days, and microseconds</li>
 * </ul>
 *
 * <p>This class preserves the interval components for proper SQL generation.
 * DuckDB represents intervals using composite expressions like:
 * "INTERVAL '1' DAY + INTERVAL '2' HOUR".
 *
 * <p>Note: Currently returns StringType for dataType() as the type system
 * doesn't have a proper IntervalType. This is acceptable since intervals
 * are primarily used in date arithmetic expressions.
 */
public final class IntervalExpression implements Expression {

    /**
     * The type of interval.
     */
    public enum IntervalType {
        /** Year-month interval (stored as total months) */
        YEAR_MONTH,
        /** Day-time interval (stored as total microseconds) */
        DAY_TIME,
        /** Calendar interval (months, days, and microseconds) */
        CALENDAR
    }

    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long MICROS_PER_MINUTE = 60L * MICROS_PER_SECOND;
    private static final long MICROS_PER_HOUR = 60L * MICROS_PER_MINUTE;
    private static final long MICROS_PER_DAY = 24L * MICROS_PER_HOUR;

    private final IntervalType intervalType;
    private final int months;
    private final int days;
    private final long microseconds;

    /**
     * Creates a YEAR_MONTH interval.
     *
     * @param months total months
     * @return YearMonth interval expression
     */
    public static IntervalExpression yearMonth(int months) {
        return new IntervalExpression(IntervalType.YEAR_MONTH, months, 0, 0);
    }

    /**
     * Creates a DAY_TIME interval.
     *
     * @param microseconds total microseconds
     * @return DayTime interval expression
     */
    public static IntervalExpression dayTime(long microseconds) {
        return new IntervalExpression(IntervalType.DAY_TIME, 0, 0, microseconds);
    }

    /**
     * Creates a CALENDAR interval.
     *
     * @param months total months
     * @param days total days
     * @param microseconds total microseconds
     * @return Calendar interval expression
     */
    public static IntervalExpression calendar(int months, int days, long microseconds) {
        return new IntervalExpression(IntervalType.CALENDAR, months, days, microseconds);
    }

    /**
     * Creates an interval expression.
     *
     * @param intervalType the type of interval
     * @param months months component
     * @param days days component
     * @param microseconds microseconds component
     */
    private IntervalExpression(IntervalType intervalType, int months, int days, long microseconds) {
        this.intervalType = Objects.requireNonNull(intervalType, "intervalType must not be null");
        this.months = months;
        this.days = days;
        this.microseconds = microseconds;
    }

    /**
     * Returns the interval type.
     *
     * @return the interval type
     */
    public IntervalType intervalType() {
        return intervalType;
    }

    /**
     * Returns the months component.
     *
     * @return months
     */
    public int months() {
        return months;
    }

    /**
     * Returns the days component.
     *
     * @return days
     */
    public int days() {
        return days;
    }

    /**
     * Returns the microseconds component.
     *
     * @return microseconds
     */
    public long microseconds() {
        return microseconds;
    }

    /**
     * Returns the data type of this interval expression.
     *
     * <p>Currently returns StringType as the type system doesn't have
     * a proper IntervalType. Intervals are typically used in date/time
     * arithmetic where the result type depends on the operand types.
     *
     * @return StringType (placeholder for interval type)
     */
    @Override
    public DataType dataType() {
        // TODO: Add proper IntervalType to the type system
        return StringType.get();
    }

    /**
     * Returns whether this interval can be null.
     *
     * <p>Interval literals are not null.
     *
     * @return false
     */
    @Override
    public boolean nullable() {
        return false;
    }

    /**
     * Generates the SQL representation of this interval.
     *
     * @return SQL string like "INTERVAL '12' MONTH" or composite expressions
     */
    @Override
    public String toSQL() {
        switch (intervalType) {
            case YEAR_MONTH:
                return buildYearMonthSQL();
            case DAY_TIME:
                return buildDayTimeSQL();
            case CALENDAR:
                return buildCalendarSQL();
            default:
                throw new IllegalStateException("Unknown interval type: " + intervalType);
        }
    }

    /**
     * Builds SQL for year-month interval.
     */
    private String buildYearMonthSQL() {
        return String.format("INTERVAL '%d' MONTH", months);
    }

    /**
     * Builds SQL for day-time interval from total microseconds.
     */
    private String buildDayTimeSQL() {
        boolean negative = microseconds < 0;
        long absMicros = Math.abs(microseconds);

        long daysPart = absMicros / MICROS_PER_DAY;
        long remainingMicros = absMicros % MICROS_PER_DAY;

        long hoursPart = remainingMicros / MICROS_PER_HOUR;
        remainingMicros = remainingMicros % MICROS_PER_HOUR;

        long minutesPart = remainingMicros / MICROS_PER_MINUTE;
        remainingMicros = remainingMicros % MICROS_PER_MINUTE;

        double secondsPart = remainingMicros / (double) MICROS_PER_SECOND;

        List<String> parts = new ArrayList<>();

        if (daysPart != 0) {
            parts.add(String.format("INTERVAL '%s%d' DAY", negative ? "-" : "", daysPart));
        }
        if (hoursPart != 0) {
            parts.add(String.format("INTERVAL '%s%d' HOUR", negative && parts.isEmpty() ? "-" : "", hoursPart));
        }
        if (minutesPart != 0) {
            parts.add(String.format("INTERVAL '%s%d' MINUTE", negative && parts.isEmpty() ? "-" : "", minutesPart));
        }
        if (secondsPart != 0 || parts.isEmpty()) {
            parts.add(String.format("INTERVAL '%s%.6f' SECOND",
                negative && parts.isEmpty() ? "-" : "", secondsPart));
        }

        return String.join(" + ", parts);
    }

    /**
     * Builds SQL for calendar interval.
     */
    private String buildCalendarSQL() {
        List<String> parts = new ArrayList<>();

        if (months != 0) {
            parts.add(String.format("INTERVAL '%d' MONTH", months));
        }
        if (days != 0) {
            parts.add(String.format("INTERVAL '%d' DAY", days));
        }
        if (microseconds != 0) {
            double seconds = microseconds / (double) MICROS_PER_SECOND;
            parts.add(String.format("INTERVAL '%.6f' SECOND", seconds));
        }

        if (parts.isEmpty()) {
            return "INTERVAL '0' SECOND";
        }

        return String.join(" + ", parts);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof IntervalExpression)) return false;
        IntervalExpression that = (IntervalExpression) obj;
        return months == that.months &&
               days == that.days &&
               microseconds == that.microseconds &&
               intervalType == that.intervalType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(intervalType, months, days, microseconds);
    }

    @Override
    public String toString() {
        switch (intervalType) {
            case YEAR_MONTH:
                return "Interval(YEAR_MONTH: " + months + " months)";
            case DAY_TIME:
                return "Interval(DAY_TIME: " + microseconds + " microseconds)";
            case CALENDAR:
                return "Interval(CALENDAR: " + months + " months, " + days + " days, " + microseconds + " microseconds)";
            default:
                return "Interval(" + intervalType + ")";
        }
    }
}
