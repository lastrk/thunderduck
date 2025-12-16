#!/usr/bin/env bash
# Download all 99 TPC-DS queries from databricks/spark-sql-perf
#
# Compatible with both bash and zsh

mkdir -p /workspace/benchmarks/tpcds_queries
cd /workspace/benchmarks/tpcds_queries

echo "Downloading 99 TPC-DS queries..."
for q in {1..99}; do
  wget -q https://raw.githubusercontent.com/databricks/spark-sql-perf/master/src/main/resources/tpcds_2_4/queries/q$q.sql -O q$q.sql
  if [ $? -eq 0 ]; then
    echo "✓ Q$q"
  else
    echo "✗ Q$q failed"
  fi
done

echo "---"
echo "Total queries downloaded:"
ls -1 q*.sql | wc -l
