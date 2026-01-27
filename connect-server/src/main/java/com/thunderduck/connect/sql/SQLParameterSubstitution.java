package com.thunderduck.connect.sql;

import com.thunderduck.connect.converter.ExpressionConverter;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.Literal;
import org.apache.spark.connect.proto.SQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Utility for substituting parameters in SQL query strings.
 *
 * <p>Spark SQL supports two types of parameters:
 * <ul>
 *   <li>Named parameters: {@code :param_name} or {@code ${param_name}}</li>
 *   <li>Positional parameters: {@code $1}, {@code $2}, etc.</li>
 * </ul>
 *
 * <p>Examples:
 * <pre>
 *   spark.sql("SELECT * FROM users WHERE age > :min_age", min_age=18)
 *   spark.sql("SELECT * FROM users WHERE status = $1", "active")
 * </pre>
 *
 * <p>This class converts Spark Connect Expression parameters to SQL literals
 * and performs string substitution in the query text.
 */
public class SQLParameterSubstitution {
    private static final Logger logger = LoggerFactory.getLogger(SQLParameterSubstitution.class);

    private final ExpressionConverter expressionConverter;

    public SQLParameterSubstitution(ExpressionConverter expressionConverter) {
        this.expressionConverter = expressionConverter;
    }

    /**
     * Substitutes parameters in a SQL query from a Spark Connect SQL message.
     *
     * @param sql the SQL message containing query and parameters
     * @return the query string with parameters substituted
     */
    public String substituteParameters(SQL sql) {
        String query = sql.getQuery();

        // Handle named parameters (newer API)
        if (sql.getNamedArgumentsCount() > 0) {
            query = substituteNamedParameters(query, sql.getNamedArgumentsMap());
        }

        // Handle positional parameters (newer API)
        if (sql.getPosArgumentsCount() > 0) {
            query = substitutePositionalParameters(query, sql.getPosArgumentsList());
        }

        // Handle deprecated named parameters
        if (sql.getArgsCount() > 0) {
            query = substituteDeprecatedNamedParameters(query, sql.getArgsMap());
        }

        // Handle deprecated positional parameters
        if (sql.getPosArgsCount() > 0) {
            query = substituteDeprecatedPositionalParameters(query, sql.getPosArgsList());
        }

        return query;
    }

    /**
     * Substitutes named parameters like :param_name or ${param_name}.
     */
    private String substituteNamedParameters(String query,
                                            Map<String, org.apache.spark.connect.proto.Expression> namedArgs) {
        for (Map.Entry<String, org.apache.spark.connect.proto.Expression> entry : namedArgs.entrySet()) {
            String paramName = entry.getKey();
            String value = expressionToSQLLiteral(entry.getValue());

            // Replace :param_name format
            query = query.replace(":" + paramName, value);

            // Replace ${param_name} format
            query = query.replace("${" + paramName + "}", value);
        }

        return query;
    }

    /**
     * Substitutes positional parameters like $1, $2, etc.
     */
    private String substitutePositionalParameters(String query,
                                                  List<org.apache.spark.connect.proto.Expression> posArgs) {
        for (int i = 0; i < posArgs.size(); i++) {
            String value = expressionToSQLLiteral(posArgs.get(i));
            // Positional parameters are 1-indexed in SQL: $1, $2, $3, etc.
            query = query.replace("$" + (i + 1), value);
        }

        return query;
    }

    /**
     * Substitutes deprecated named parameters (using Literal instead of Expression).
     */
    private String substituteDeprecatedNamedParameters(String query,
                                                       Map<String, org.apache.spark.connect.proto.Expression.Literal> args) {
        for (Map.Entry<String, org.apache.spark.connect.proto.Expression.Literal> entry : args.entrySet()) {
            String paramName = entry.getKey();
            String value = literalToSQLLiteral(entry.getValue());

            // Replace :param_name format
            query = query.replace(":" + paramName, value);

            // Replace ${param_name} format
            query = query.replace("${" + paramName + "}", value);
        }

        return query;
    }

    /**
     * Substitutes deprecated positional parameters (using Literal instead of Expression).
     */
    private String substituteDeprecatedPositionalParameters(String query,
                                                            List<org.apache.spark.connect.proto.Expression.Literal> posArgs) {
        for (int i = 0; i < posArgs.size(); i++) {
            String value = literalToSQLLiteral(posArgs.get(i));
            // Positional parameters are 1-indexed in SQL: $1, $2, $3, etc.
            query = query.replace("$" + (i + 1), value);
        }

        return query;
    }

    /**
     * Converts a Spark Connect Expression to a SQL literal string.
     *
     * <p>If the expression is not a simple literal, this method will log a warning
     * and attempt to use the expression's SQL representation.
     */
    private String expressionToSQLLiteral(org.apache.spark.connect.proto.Expression expr) {
        try {
            // Convert protobuf expression to internal representation
            Expression thunderduckExpr = expressionConverter.convert(expr);

            // If it's a literal, use its SQL representation
            if (thunderduckExpr instanceof Literal) {
                return ((Literal) thunderduckExpr).toSQL();
            }

            // For non-literal expressions, use the full SQL representation
            // This allows complex expressions like "SELECT id FROM other_table" as parameters
            logger.debug("Parameter is not a simple literal, using full expression SQL: {}",
                        thunderduckExpr.toSQL());
            return "(" + thunderduckExpr.toSQL() + ")";

        } catch (Exception e) {
            logger.error("Failed to convert expression parameter to SQL: {}", e.getMessage());
            throw new IllegalArgumentException("Invalid SQL parameter expression", e);
        }
    }

    /**
     * Converts a deprecated Spark Connect Literal to a SQL literal string.
     */
    private String literalToSQLLiteral(org.apache.spark.connect.proto.Expression.Literal protoLiteral) {
        try {
            // Convert protobuf literal to internal representation
            // We need to wrap it in an Expression first
            org.apache.spark.connect.proto.Expression expr =
                org.apache.spark.connect.proto.Expression.newBuilder()
                    .setLiteral(protoLiteral)
                    .build();

            Expression thunderduckExpr = expressionConverter.convert(expr);

            if (thunderduckExpr instanceof Literal) {
                return ((Literal) thunderduckExpr).toSQL();
            }

            // This shouldn't happen, but handle it just in case
            logger.warn("Literal conversion did not produce Literal type: {}",
                       thunderduckExpr.getClass().getSimpleName());
            return thunderduckExpr.toSQL();

        } catch (Exception e) {
            logger.error("Failed to convert literal parameter to SQL: {}", e.getMessage());
            throw new IllegalArgumentException("Invalid SQL parameter literal", e);
        }
    }
}
