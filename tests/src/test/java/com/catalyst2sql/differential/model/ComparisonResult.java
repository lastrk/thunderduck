package com.catalyst2sql.differential.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Result of comparing Spark and catalyst2sql execution results.
 */
public class ComparisonResult {
    private final String testName;
    private final List<Divergence> divergences;
    private final boolean passed;

    public ComparisonResult(String testName) {
        this.testName = testName;
        this.divergences = new ArrayList<>();
        this.passed = true;
    }

    public void addDivergence(Divergence divergence) {
        this.divergences.add(divergence);
    }

    public String getTestName() {
        return testName;
    }

    public List<Divergence> getDivergences() {
        return divergences;
    }

    public boolean hasDivergences() {
        return !divergences.isEmpty();
    }

    public boolean isPassed() {
        return divergences.isEmpty();
    }
}
