# Current Focus: EC2 Comparative Benchmarking

**Status:** In Progress
**Started:** 2025-12-30

## Objective

Create infrastructure for running TPC-H and TPC-DS benchmarks comparing Thunderduck vs Apache Spark (3.5.3 and 4.0.1) on AWS EC2 instances in single-node execution mode.

## Architecture

Each engine runs on its own EC2 instance in parallel:

```
Local Driver (benchmark.py)
       │
       ├── Provisions 3 EC2 instances in parallel (tagged for cleanup)
       │     ├── thunderduck-bench-{run_id}-thunderduck
       │     ├── thunderduck-bench-{run_id}-spark-3.5.3
       │     └── thunderduck-bench-{run_id}-spark-4.0.1
       │
       ├── Each instance:
       │     ├── Downloads data from S3
       │     ├── Starts Connect server (port 15002)
       │     └── Runs benchmark queries
       │
       └── Collects results, generates report
```

## Usage

```bash
# Basic usage
python benchmarks/ec2/benchmark.py \
  --instance-type c6i.2xlarge \
  --scale-factor 100 \
  --benchmark tpch

# Full options
python benchmarks/ec2/benchmark.py \
  --instance-type r6i.4xlarge \
  --scale-factor 1000 \
  --benchmark all \
  --s3-bucket your-bucket \
  --s3-prefix thunderduck-benchmarks \
  --spark-versions 3.5.3,4.0.1 \
  --runs 5 \
  --keep-instances
```

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--instance-type` | Required | EC2 instance type (e.g., c6i.2xlarge, r6i.4xlarge) |
| `--scale-factor` | Required | TPC-H/TPC-DS scale factor (1, 10, 100, 1000, or custom) |
| `--benchmark` | `all` | Which benchmark to run: `tpch`, `tpcds`, or `all` |
| `--s3-bucket` | env var | S3 bucket for test data |
| `--s3-prefix` | `thunderduck-benchmarks` | S3 prefix for test data |
| `--spark-versions` | `3.5.3,4.0.1` | Comma-separated Spark versions to test |
| `--runs` | `5` | Number of runs per query (first is warmup) |
| `--keep-instances` | `false` | Don't terminate EC2 instances after benchmark |

## Cleanup

All benchmark instances are tagged for reliable cleanup:

```bash
# List all benchmark instances
python benchmarks/ec2/cleanup.py

# Terminate all benchmark instances
python benchmarks/ec2/cleanup.py --terminate

# Terminate instances from specific run
python benchmarks/ec2/cleanup.py --run-id abc123

# Terminate instances older than 24 hours
python benchmarks/ec2/cleanup.py --older-than 24h
```

## AWS Prerequisites

1. **IAM Role** with S3 read/write access for EC2 instances
2. **Security Group** allowing SSH (port 22) inbound
3. **Key Pair** for SSH access
4. **S3 Bucket** for benchmark data (auto-created with 30-day expiration if not exists)

## Environment Variables

```bash
export AWS_PROFILE=your-profile        # Optional, uses default chain
export AWS_REGION=us-east-1            # Optional
export BENCHMARK_S3_BUCKET=your-bucket # Required
export BENCHMARK_S3_PREFIX=thunderduck-benchmarks  # Optional
```

## Files

| File | Purpose |
|------|---------|
| `benchmarks/ec2/benchmark.py` | Main driver script |
| `benchmarks/ec2/provisioner.py` | EC2 lifecycle management |
| `benchmarks/ec2/cleanup.py` | EC2 cleanup by tag |
| `benchmarks/ec2/setup_s3.py` | S3 bucket setup with lifecycle |
| `benchmarks/ec2/remote_setup.sh` | Instance setup script |
| `benchmarks/ec2/generate_data.py` | TPC-H/TPC-DS data generation |
| `benchmarks/ec2/run_benchmark.py` | Query execution |
| `benchmarks/ec2/spark_config.py` | Spark configuration |
| `benchmarks/ec2/results.py` | Results collection |
| `benchmarks/queries/tpch_dataframe.py` | TPC-H DataFrame queries |
| `benchmarks/queries/tpcds_dataframe.py` | TPC-DS DataFrame queries |

## Instance Tagging

All benchmark EC2 instances are tagged:

| Tag | Value |
|-----|-------|
| `thunderduck-benchmark` | `true` |
| `thunderduck-bench-run-id` | Unique run ID |
| `thunderduck-bench-engine` | `thunderduck`, `spark-3.5.3`, `spark-4.0.1` |
| `Name` | `thunderduck-bench-{run_id}-{engine}` |
