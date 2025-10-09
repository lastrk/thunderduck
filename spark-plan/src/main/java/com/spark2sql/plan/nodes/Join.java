package com.spark2sql.plan.nodes;

import com.spark2sql.plan.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public class Join extends LogicalPlan {
    private final Expression condition;
    private final String joinType;

    public Join(LogicalPlan left, LogicalPlan right, Expression condition, String joinType) {
        super(Arrays.asList(left, right));
        this.condition = condition;
        this.joinType = validateJoinType(joinType);
    }

    public LogicalPlan getLeft() {
        return children.get(0);
    }

    public LogicalPlan getRight() {
        return children.get(1);
    }

    public Expression getCondition() {
        return condition;
    }

    public String getJoinType() {
        return joinType;
    }

    private String validateJoinType(String type) {
        String normalized = type.toLowerCase().trim();
        switch (normalized) {
            case "inner":
            case "left":
            case "left_outer":
            case "right":
            case "right_outer":
            case "outer":
            case "full":
            case "full_outer":
            case "cross":
            case "left_semi":
            case "left_anti":
                return normalized;
            default:
                throw new IllegalArgumentException("Unsupported join type: " + type);
        }
    }

    @Override
    public String nodeName() {
        return "Join[" + joinType + "]";
    }

    @Override
    protected StructType computeSchema() {
        // Combine schemas from both left and right sides
        StructType leftSchema = children.get(0).schema();
        StructType rightSchema = children.get(1).schema();

        List<StructField> allFields = new ArrayList<>();
        for (StructField field : leftSchema.fields()) {
            allFields.add(field);
        }
        for (StructField field : rightSchema.fields()) {
            allFields.add(field);
        }

        return new StructType(allFields.toArray(new StructField[0]));
    }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) {
        return visitor.visitJoin(this);
    }
}
