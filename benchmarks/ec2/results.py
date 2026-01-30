#!/usr/bin/env python3
"""
Results Collection and Reporting

Collects benchmark results and generates reports.
"""
import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class QueryResult:
    """Result of a single query execution."""

    query_name: str
    engine: str
    run_times_ms: List[float]
    avg_time_ms: float
    row_count: int
    success: bool
    error: Optional[str] = None


class ResultCollector:
    """Collects and aggregates benchmark results."""

    def __init__(self):
        self.results: List[QueryResult] = []
        self.metadata: Dict[str, Any] = {}

    def add_result(self, result: QueryResult) -> None:
        """Add a query result."""
        self.results.append(result)

    def add_metadata(self, key: str, value: Any) -> None:
        """Add metadata about the benchmark run."""
        self.metadata[key] = value

    def get_results_by_engine(self) -> Dict[str, List[QueryResult]]:
        """Group results by engine."""
        by_engine = {}
        for result in self.results:
            if result.engine not in by_engine:
                by_engine[result.engine] = []
            by_engine[result.engine].append(result)
        return by_engine

    def get_results_by_query(self) -> Dict[str, List[QueryResult]]:
        """Group results by query name."""
        by_query = {}
        for result in self.results:
            if result.query_name not in by_query:
                by_query[result.query_name] = []
            by_query[result.query_name].append(result)
        return by_query

    def calculate_speedups(self, baseline_engine: str = "thunderduck") -> Dict[str, Dict[str, float]]:
        """Calculate speedup ratios compared to baseline.

        Args:
            baseline_engine: Engine to use as baseline

        Returns:
            Dict mapping query_name -> {engine: speedup_ratio}
        """
        by_query = self.get_results_by_query()
        speedups = {}

        for query_name, results in by_query.items():
            baseline_time = None
            for r in results:
                if r.engine == baseline_engine and r.success:
                    baseline_time = r.avg_time_ms
                    break

            if baseline_time is None:
                continue

            speedups[query_name] = {}
            for r in results:
                if r.success and r.avg_time_ms > 0:
                    # Speedup > 1 means baseline is faster
                    # Speedup < 1 means compared engine is faster
                    speedups[query_name][r.engine] = r.avg_time_ms / baseline_time

        return speedups

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "metadata": self.metadata,
            "results": [asdict(r) for r in self.results],
        }


def generate_report(
    results: List[Dict],
    args: Any,
    report_path: Path,
    json_path: Path,
) -> None:
    """Generate markdown and JSON reports from benchmark results.

    Args:
        results: List of result dicts from each engine
        args: Command-line arguments
        report_path: Path for markdown report
        json_path: Path for JSON results
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Aggregate all results
    collector = ResultCollector()
    collector.add_metadata("timestamp", timestamp)
    collector.add_metadata("instance_type", args.instance_type)
    collector.add_metadata("scale_factor", args.scale_factor)
    collector.add_metadata("benchmark", args.benchmark)
    collector.add_metadata("runs", args.runs)

    for engine_result in results:
        if not engine_result.get("success", False):
            continue

        engine = engine_result["engine"]
        result_data = engine_result.get("results", {})

        for benchmark_name, query_results in result_data.get("benchmarks", {}).items():
            for qr in query_results:
                collector.add_result(QueryResult(
                    query_name=qr["query_name"],
                    engine=engine,
                    run_times_ms=qr["run_times_ms"],
                    avg_time_ms=qr["avg_time_ms"],
                    row_count=qr["row_count"],
                    success=qr["success"],
                    error=qr.get("error"),
                ))

    # Write JSON results
    with open(json_path, "w") as f:
        json.dump(collector.to_dict(), f, indent=2)

    # Generate markdown report
    report_lines = [
        "# Thunderduck Benchmark Report",
        "",
        f"**Date:** {timestamp}",
        f"**Instance Type:** {args.instance_type}",
        f"**Scale Factor:** {args.scale_factor}",
        f"**Benchmark:** {args.benchmark}",
        f"**Runs per Query:** {args.runs}",
        "",
        "---",
        "",
    ]

    # Summary table
    report_lines.extend([
        "## Summary",
        "",
    ])

    by_engine = collector.get_results_by_engine()
    engines = sorted(by_engine.keys())

    if engines:
        # Calculate totals per engine
        engine_totals = {}
        for engine, engine_results in by_engine.items():
            successful = [r for r in engine_results if r.success]
            total_time = sum(r.avg_time_ms for r in successful)
            engine_totals[engine] = {
                "total_time": total_time,
                "successful": len(successful),
                "failed": len(engine_results) - len(successful),
            }

        report_lines.extend([
            "| Engine | Total Time (ms) | Successful | Failed |",
            "|--------|-----------------|------------|--------|",
        ])

        for engine in engines:
            totals = engine_totals[engine]
            report_lines.append(
                f"| {engine} | {totals['total_time']:.1f} | {totals['successful']} | {totals['failed']} |"
            )

        report_lines.append("")

    # Speedup comparison
    speedups = collector.calculate_speedups("thunderduck")
    if speedups:
        report_lines.extend([
            "## Speedup vs Thunderduck",
            "",
            "Values > 1.0 mean Thunderduck is faster.",
            "",
        ])

        # Build header
        other_engines = [e for e in engines if e != "thunderduck"]
        header = "| Query |" + " | ".join(other_engines) + " |"
        separator = "|-------|" + " | ".join(["-------"] * len(other_engines)) + " |"

        report_lines.extend([header, separator])

        for query_name in sorted(speedups.keys()):
            query_speedups = speedups[query_name]
            row = f"| {query_name} |"
            for engine in other_engines:
                ratio = query_speedups.get(engine, 0)
                if ratio > 0:
                    row += f" {ratio:.2f}x |"
                else:
                    row += " N/A |"
            report_lines.append(row)

        report_lines.append("")

    # Detailed results per query
    report_lines.extend([
        "## Detailed Results",
        "",
    ])

    by_query = collector.get_results_by_query()
    for query_name in sorted(by_query.keys()):
        query_results = by_query[query_name]

        report_lines.extend([
            f"### {query_name}",
            "",
            "| Engine | Avg Time (ms) | Min Time (ms) | Max Time (ms) | Rows |",
            "|--------|---------------|---------------|---------------|------|",
        ])

        for qr in sorted(query_results, key=lambda x: x.engine):
            if qr.success:
                min_time = min(qr.run_times_ms[1:]) if len(qr.run_times_ms) > 1 else qr.run_times_ms[0]
                max_time = max(qr.run_times_ms[1:]) if len(qr.run_times_ms) > 1 else qr.run_times_ms[0]
                report_lines.append(
                    f"| {qr.engine} | {qr.avg_time_ms:.1f} | {min_time:.1f} | {max_time:.1f} | {qr.row_count} |"
                )
            else:
                report_lines.append(f"| {qr.engine} | FAILED | - | - | - |")

        report_lines.append("")

    # Errors section
    errors = [r for r in collector.results if not r.success and r.error]
    if errors:
        report_lines.extend([
            "## Errors",
            "",
        ])
        for r in errors:
            report_lines.append(f"- **{r.query_name}** ({r.engine}): {r.error}")
        report_lines.append("")

    # Write report
    with open(report_path, "w") as f:
        f.write("\n".join(report_lines))


def load_results(json_path: Path) -> ResultCollector:
    """Load results from JSON file.

    Args:
        json_path: Path to JSON results file

    Returns:
        ResultCollector with loaded results
    """
    with open(json_path) as f:
        data = json.load(f)

    collector = ResultCollector()
    collector.metadata = data.get("metadata", {})

    for r in data.get("results", []):
        collector.add_result(QueryResult(**r))

    return collector


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Load and print summary from JSON file
        collector = load_results(Path(sys.argv[1]))
        print(f"Loaded {len(collector.results)} results")

        by_engine = collector.get_results_by_engine()
        for engine, results in by_engine.items():
            successful = sum(1 for r in results if r.success)
            print(f"  {engine}: {successful}/{len(results)} successful")
    else:
        print("Usage: python results.py <results.json>")
