#!/usr/bin/env python3
"""
AWS CDK Application for Thunderduck Benchmark Infrastructure

Deploys EC2 instances, security groups, SSH keys, and IAM roles for benchmarking.

Usage:
    cdk deploy -c instance_type=m8g.16xlarge -c engines=thunderduck,spark-3.5.3,spark-4.0.1
    cdk destroy --force
"""
import os

import aws_cdk as cdk

from benchmark_stack import BenchmarkStack


def main():
    app = cdk.App()

    # Read context parameters
    instance_type = app.node.try_get_context("instance_type") or "m6i.xlarge"
    engines_str = app.node.try_get_context("engines") or "thunderduck,spark-3.5.3,spark-4.0.1"
    engines = [e.strip() for e in engines_str.split(",")]
    stack_name = app.node.try_get_context("stack_name") or "ThunderduckBenchmark"

    # Get environment from context or defaults
    account = os.environ.get("CDK_DEFAULT_ACCOUNT")
    region = app.node.try_get_context("region") or os.environ.get("CDK_DEFAULT_REGION", "us-east-1")

    print(f"Deploying benchmark infrastructure:")
    print(f"  Stack Name: {stack_name}")
    print(f"  Instance Type: {instance_type}")
    print(f"  Engines: {engines}")
    print(f"  Region: {region}")

    BenchmarkStack(
        app,
        stack_name,
        instance_type=instance_type,
        engines=engines,
        env=cdk.Environment(account=account, region=region),
        description="Thunderduck benchmark infrastructure - EC2 instances for TPC-H/TPC-DS benchmarks",
    )

    app.synth()


if __name__ == "__main__":
    main()
