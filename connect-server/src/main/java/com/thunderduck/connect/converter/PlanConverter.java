package com.thunderduck.connect.converter;

import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.expression.Expression;
import com.thunderduck.schema.SchemaInferrer;

import org.apache.spark.connect.proto.Plan;
import org.apache.spark.connect.proto.Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

/**
 * Converts Spark Connect Protocol Buffers plans to thunderduck LogicalPlans.
 *
 * <p>This is the main entry point for plan deserialization, converting from
 * Protobuf representations to internal logical plan representations that can
 * be translated to SQL.
 */
public class PlanConverter {
    private static final Logger logger = LoggerFactory.getLogger(PlanConverter.class);

    private final RelationConverter relationConverter;
    private final ExpressionConverter expressionConverter;

    /**
     * Creates a PlanConverter without schema inference capability.
     * NA functions requiring schema inference will fail.
     */
    public PlanConverter() {
        this(null);
    }

    /**
     * Creates a PlanConverter with schema inference capability.
     *
     * @param connection the DuckDB connection for schema inference (nullable)
     */
    public PlanConverter(Connection connection) {
        this.expressionConverter = new ExpressionConverter();
        SchemaInferrer schemaInferrer = connection != null ? new SchemaInferrer(connection) : null;
        this.relationConverter = new RelationConverter(expressionConverter, schemaInferrer);
    }

    /**
     * Converts a Spark Connect Plan to a LogicalPlan.
     *
     * @param plan the Protobuf plan
     * @return the converted LogicalPlan
     * @throws PlanConversionException if conversion fails
     */
    public LogicalPlan convert(Plan plan) {
        logger.debug("Converting plan: {}", plan.getOpTypeCase());

        if (plan.hasRoot()) {
            return convertRelation(plan.getRoot());
        }

        throw new PlanConversionException("Unsupported plan type: " + plan.getOpTypeCase());
    }

    /**
     * Converts a Spark Connect Relation to a LogicalPlan.
     *
     * @param relation the Protobuf relation
     * @return the converted LogicalPlan
     * @throws PlanConversionException if conversion fails
     */
    public LogicalPlan convertRelation(Relation relation) {
        return relationConverter.convert(relation);
    }

    /**
     * Converts a Spark Connect Expression to a thunderduck Expression.
     *
     * @param expr the Protobuf expression
     * @return the converted Expression
     * @throws PlanConversionException if conversion fails
     */
    public Expression convertExpression(org.apache.spark.connect.proto.Expression expr) {
        return expressionConverter.convert(expr);
    }

    /**
     * Converts a list of Spark Connect Expressions.
     *
     * @param exprs the list of Protobuf expressions
     * @return the list of converted Expressions
     */
    public List<Expression> convertExpressions(List<org.apache.spark.connect.proto.Expression> exprs) {
        return exprs.stream()
                .map(this::convertExpression)
                .toList();
    }
}