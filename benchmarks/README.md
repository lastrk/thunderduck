# Thunderduck EC2 Benchmarking

Comparative benchmarking infrastructure for Thunderduck vs Apache Spark on AWS EC2.

## Overview

This benchmark suite runs TPC-H and TPC-DS queries using the DataFrame API, comparing:
- **Thunderduck** - DuckDB-powered Spark Connect server
- **Apache Spark 3.5.3** - Latest stable Spark 3.x
- **Apache Spark 4.0.1** - Latest Spark 4.x

Each engine runs on its own EC2 instance in parallel for fair comparison.

## Quick Start

```bash
# 1. Install dependencies (optionally in a virtual environment)
cd benchmarks

# Option A: Install globally
pip install -r ec2/requirements.txt -r cdk/requirements.txt

# Option B: Use a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r ec2/requirements.txt -r cdk/requirements.txt

# 2. Build Thunderduck (from project root)
cd .. && mvn package -DskipTests && cd benchmarks
# On macOS: mvn package -DskipTests -Puse-system-protoc (see main README.md for setup)

# 3. Set AWS credentials and bootstrap CDK (one-time per account/region)
export AWS_PROFILE=your-profile
export BENCHMARK_S3_BUCKET=your-bucket

  # Only needed once per AWS account/region

# 4. Deploy benchmark infrastructure
npx aws-cdk deploy -c instance_type=m8g.16xlarge

# 5. Get SSH key from SSM Parameter Store
cd ../ec2
python -c "from provisioner import CDKProvisioner; CDKProvisioner.get_ssh_key_from_ssm('ThunderduckBenchmark', 'benchmark_key.pem')"

# 6. Run benchmarks (uses default stack name and key file)
python benchmark.py --scale-factor 10 --s3-bucket $BENCHMARK_S3_BUCKET --benchmark tpch

# 7. Cleanup when done
python cleanup.py --destroy
```

## Prerequisites

### AWS Configuration

The CDK stack automatically creates:
- **Security Group** with SSH access
- **SSH Key Pair** (stored in SSM Parameter Store)
- **IAM Role** with S3 access for EC2 instances
- **EC2 Instances** (one per engine)

You only need:
1. **AWS credentials** configured (via profile or environment)
2. **Node.js** installed (for `npx aws-cdk` commands)
3. **S3 Bucket** for benchmark data (created manually or via setup_s3.py)
4. **CDK bootstrap** run once per account/region (`npx aws-cdk bootstrap`)

### Environment Variables

```bash
export AWS_PROFILE=your-profile           # Optional, uses default chain
export AWS_REGION=us-east-1               # Optional
export BENCHMARK_S3_BUCKET=your-bucket    # Required
export BENCHMARK_S3_PREFIX=thunderduck-benchmarks  # Optional
```

## Usage

### Deploy Infrastructure with CDK

```bash
cd benchmarks/cdk

# Deploy with default settings (m6i.xlarge, thunderduck + spark-3.5.3 + spark-4.0.1)
npx aws-cdk deploy

# Deploy with custom instance type and engines
npx aws-cdk deploy -c instance_type=m8g.16xlarge -c engines=thunderduck,spark-3.5.3,spark-4.0.1

# Deploy with custom stack name
npx aws-cdk deploy -c stack_name=MyBenchmark -c instance_type=c6i.2xlarge
```

### Run Benchmarks

```bash
cd benchmarks/ec2

# Basic benchmark (uses default stack name and key file)
python benchmark.py --scale-factor 100 --s3-bucket my-bucket --benchmark tpch
```

### All Benchmark Options

```bash
python benchmark.py \
  --scale-factor 1000 \                # TPC-H/TPC-DS scale factor (required)
  --s3-bucket my-bucket \              # S3 bucket for data (required)
  --benchmark all \                    # tpch, tpcds, or all
  --s3-prefix benchmarks \             # S3 prefix (default: thunderduck-benchmarks)
  --runs 5 \                           # Runs per query (default: 5, first is warmup)
  --output-dir results \               # Directory for output files
  --stack-name ThunderduckBenchmark \  # CDK stack name (default)
  --key-file benchmark_key.pem         # SSH private key file (default)
```

### Generate Test Data

```bash
# Generate locally (for testing)
python benchmarks/ec2/generate_data.py \
  --benchmark tpch \
  --scale-factor 10 \
  --output-dir /tmp/tpch-data

# Generate and upload to S3
python benchmarks/ec2/generate_data.py \
  --benchmark tpch \
  --scale-factor 100 \
  --output-dir /tmp/tpch-data \
  --upload-to-s3 my-bucket
```

### Setup S3 Bucket

```bash
# Create bucket with 30-day lifecycle policy
python benchmarks/ec2/setup_s3.py my-bucket

# List available benchmark data
python benchmarks/ec2/setup_s3.py my-bucket --list
```

### Cleanup Resources

#### CDK Stack Cleanup (Recommended)

When using CDK-managed infrastructure:

```bash
cd benchmarks/ec2

# Destroy the default CDK stack (ThunderduckBenchmark)
python cleanup.py --destroy

# Destroy a specific stack by name
python cleanup.py --destroy-stack MyBenchmark

# List available benchmark stacks
python cleanup.py --list-stacks

# Dry run (see what would happen)
python cleanup.py --destroy --dry-run

# Or use CDK directly
cd benchmarks/cdk
npx aws-cdk destroy --force
```

#### Legacy Instance Cleanup

For manually tagged instances (not CDK-managed):

```bash
# List all benchmark instances
python benchmarks/ec2/cleanup.py

# Terminate all benchmark instances
python benchmarks/ec2/cleanup.py --terminate

# Terminate instances older than 24 hours
python benchmarks/ec2/cleanup.py --older-than 24h --terminate

# Dry run (see what would be terminated)
python benchmarks/ec2/cleanup.py --terminate --dry-run
```

## Instance Tagging

CDK-managed EC2 instances are tagged:

| Tag | Value | Purpose |
|-----|-------|---------|
| `Project` | `thunderduck-benchmark` | Identifies project |
| `ManagedBy` | `CDK` | Indicates CDK management |
| `thunderduck-bench-engine` | `thunderduck`, `spark-3.5.3`, etc. | Identifies engine |
| `Name` | `thunderduck-bench-{engine}` | Human-readable name |

## Benchmark Queries

### TPC-H (22 queries)

All 22 TPC-H queries implemented with DataFrame API:
- Q1: Pricing Summary Report
- Q2: Minimum Cost Supplier
- Q3: Shipping Priority
- Q4-Q22: See `benchmarks/queries/tpch_dataframe.py`

### TPC-DS (17 queries)

Subset of TPC-DS queries that translate well to DataFrame operations:
- Q3, Q7, Q12, Q19, Q27, Q34, Q42, Q52, Q55, Q68, Q73, Q79, Q82, Q84, Q91, Q96, Q98

See `benchmarks/queries/tpcds_dataframe.py`

## Output

Benchmarks generate two output files:

1. **Markdown Report** (`benchmark_report_YYYYMMDD_HHMMSS.md`)
   - Summary table with total times per engine
   - Speedup comparison vs Thunderduck
   - Detailed per-query results
   - Error summary

2. **JSON Results** (`benchmark_results_YYYYMMDD_HHMMSS.json`)
   - Raw timing data for further analysis
   - Metadata (instance type, scale factor, etc.)

## Directory Structure

```
benchmarks/
├── cdk/                      # AWS CDK infrastructure
│   ├── app.py                # CDK app entry point
│   ├── benchmark_stack.py    # CDK stack definition
│   ├── cdk.json              # CDK configuration
│   └── requirements.txt      # CDK Python dependencies
├── ec2/
│   ├── benchmark.py          # Main driver script
│   ├── provisioner.py        # SSH access to CDK instances
│   ├── cleanup.py            # Resource cleanup utility
│   ├── setup_s3.py           # S3 bucket setup
│   ├── remote_setup.sh       # EC2 instance setup script
│   ├── generate_data.py      # TPC-H/TPC-DS data generation
│   ├── run_benchmark.py      # Remote benchmark execution
│   ├── spark_config.py       # Spark configuration helper
│   ├── results.py            # Results collection/reporting
│   └── requirements.txt      # Python dependencies
├── queries/
│   ├── tpch_dataframe.py     # TPC-H DataFrame queries
│   └── tpcds_dataframe.py    # TPC-DS DataFrame queries
└── README.md                 # This file
```

## Scale Factor Guidelines

| Scale Factor | Data Size | Recommended Instance |
|--------------|-----------|---------------------|
| 1 | ~1 GB | m6i.large |
| 10 | ~10 GB | m6i.xlarge |
| 100 | ~100 GB | c6i.2xlarge |
| 1000 | ~1 TB | r6i.4xlarge |

## Troubleshooting

### CDK Deploy Failed

- Ensure CDK is bootstrapped: `npx aws-cdk bootstrap`
- Check AWS credentials are configured: `aws sts get-caller-identity`
- Verify default VPC exists in the region
- Check instance type is available in the region

### SSH Connection Failed

- Retrieve SSH key from SSM: `python -c "from provisioner import CDKProvisioner; CDKProvisioner.get_ssh_key_from_ssm('ThunderduckBenchmark', 'benchmark_key.pem')"`
- Verify key file permissions: `chmod 600 benchmark_key.pem`
- Check security group allows SSH from your IP (CDK creates this automatically)
- Ensure instance is running: check AWS console or `aws ec2 describe-instances`

### Benchmark Data Not Found

- Generate data first: `python generate_data.py --benchmark tpch --scale-factor 100 --upload-to-s3 bucket`
- Check S3 permissions (CDK creates IAM role with S3 access)
- Verify S3 bucket and prefix are correct

### Instance Not Terminating

- Use CDK destroy: `python cleanup.py --destroy`
- Or destroy directly: `cd benchmarks/cdk && cdk destroy --force`
- For legacy instances: `python cleanup.py --terminate`

### Server Start Failed

- Check instance has enough memory for scale factor
- Review logs: `/tmp/thunderduck.log` or `/tmp/spark-connect.log`

### npx/Node.js Not Found

- Install Node.js: https://nodejs.org/ (LTS version recommended)
- Verify installation: `node --version && npx --version`
