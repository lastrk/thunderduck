#!/usr/bin/env python3
"""
Spark Configuration Helper

Generates optimal Spark configuration based on EC2 instance type.
"""
from typing import Dict, Optional

# EC2 instance specifications (vCPUs, Memory GB)
# Reference: https://aws.amazon.com/ec2/instance-types/
INSTANCE_SPECS = {
    # Compute optimized
    "c6i.large": (2, 4),
    "c6i.xlarge": (4, 8),
    "c6i.2xlarge": (8, 16),
    "c6i.4xlarge": (16, 32),
    "c6i.8xlarge": (32, 64),
    "c6i.12xlarge": (48, 96),
    "c6i.16xlarge": (64, 128),
    "c6i.24xlarge": (96, 192),
    "c6i.32xlarge": (128, 256),
    # Memory optimized
    "r6i.large": (2, 16),
    "r6i.xlarge": (4, 32),
    "r6i.2xlarge": (8, 64),
    "r6i.4xlarge": (16, 128),
    "r6i.8xlarge": (32, 256),
    "r6i.12xlarge": (48, 384),
    "r6i.16xlarge": (64, 512),
    "r6i.24xlarge": (96, 768),
    "r6i.32xlarge": (128, 1024),
    # General purpose
    "m6i.large": (2, 8),
    "m6i.xlarge": (4, 16),
    "m6i.2xlarge": (8, 32),
    "m6i.4xlarge": (16, 64),
    "m6i.8xlarge": (32, 128),
    "m6i.12xlarge": (48, 192),
    "m6i.16xlarge": (64, 256),
    "m6i.24xlarge": (96, 384),
    "m6i.32xlarge": (128, 512),
    # Graviton (ARM)
    "c7g.large": (2, 4),
    "c7g.xlarge": (4, 8),
    "c7g.2xlarge": (8, 16),
    "c7g.4xlarge": (16, 32),
    "c7g.8xlarge": (32, 64),
    "c7g.12xlarge": (48, 96),
    "c7g.16xlarge": (64, 128),
    "r7g.large": (2, 16),
    "r7g.xlarge": (4, 32),
    "r7g.2xlarge": (8, 64),
    "r7g.4xlarge": (16, 128),
    "r7g.8xlarge": (32, 256),
    "r7g.12xlarge": (48, 384),
    "r7g.16xlarge": (64, 512),
    "m7g.large": (2, 8),
    "m7g.xlarge": (4, 16),
    "m7g.2xlarge": (8, 32),
    "m7g.4xlarge": (16, 64),
    "m7g.8xlarge": (32, 128),
    "m7g.12xlarge": (48, 192),
    "m7g.16xlarge": (64, 256),
}


def get_instance_specs(instance_type: str) -> tuple:
    """Get vCPUs and memory for an instance type.

    Args:
        instance_type: EC2 instance type (e.g., 'c6i.2xlarge')

    Returns:
        Tuple of (vcpus, memory_gb)
    """
    if instance_type in INSTANCE_SPECS:
        return INSTANCE_SPECS[instance_type]

    # Try to parse from instance type name for unknown types
    # Format: family.size (e.g., c6i.2xlarge)
    parts = instance_type.split(".")
    if len(parts) == 2:
        size = parts[1]
        # Rough estimation based on size suffix
        size_multipliers = {
            "nano": (1, 0.5),
            "micro": (1, 1),
            "small": (1, 2),
            "medium": (2, 4),
            "large": (2, 8),
            "xlarge": (4, 16),
            "2xlarge": (8, 32),
            "4xlarge": (16, 64),
            "8xlarge": (32, 128),
            "12xlarge": (48, 192),
            "16xlarge": (64, 256),
            "24xlarge": (96, 384),
            "32xlarge": (128, 512),
        }
        if size in size_multipliers:
            return size_multipliers[size]

    # Default fallback
    return (4, 16)


def get_spark_config(
    instance_type: str,
    connect_port: int = 15002,
    memory_fraction: float = 0.8,
) -> Dict[str, str]:
    """Generate optimal Spark config based on instance type.

    Args:
        instance_type: EC2 instance type (e.g., 'c6i.2xlarge')
        connect_port: Port for Spark Connect server
        memory_fraction: Fraction of memory to allocate to Spark

    Returns:
        Dict of Spark configuration properties
    """
    cores, memory_gb = get_instance_specs(instance_type)

    # Calculate memory allocation
    # Reserve some memory for OS and other processes
    spark_memory = int(memory_gb * memory_fraction)
    driver_memory = max(4, spark_memory)  # At least 4GB

    config = {
        # Execution
        "spark.master": "local[*]",
        "spark.driver.memory": f"{driver_memory}g",
        "spark.executor.memory": f"{driver_memory}g",
        # Parallelism
        "spark.sql.shuffle.partitions": str(cores * 2),
        "spark.default.parallelism": str(cores * 2),
        # Adaptive Query Execution
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        # Memory management
        "spark.memory.fraction": "0.8",
        "spark.memory.storageFraction": "0.3",
        # Spark Connect
        "spark.connect.grpc.binding.port": str(connect_port),
        # Performance tuning
        "spark.sql.parquet.compression.codec": "zstd",
        "spark.sql.files.maxPartitionBytes": "256m",
        # Disable unused features for benchmarking
        "spark.ui.enabled": "false",
        "spark.eventLog.enabled": "false",
    }

    return config


def get_spark_submit_args(
    instance_type: str,
    spark_version: str,
    connect_port: int = 15002,
) -> str:
    """Generate spark-submit arguments for starting Connect server.

    Args:
        instance_type: EC2 instance type
        spark_version: Spark version (e.g., '3.5.3')
        connect_port: Port for Spark Connect server

    Returns:
        String of spark-submit arguments
    """
    config = get_spark_config(instance_type, connect_port)

    args = [f"--packages org.apache.spark:spark-connect_2.13:{spark_version}"]

    for key, value in config.items():
        args.append(f"--conf {key}={value}")

    return " ".join(args)


def print_config(instance_type: str) -> None:
    """Print Spark configuration for an instance type."""
    cores, memory_gb = get_instance_specs(instance_type)
    config = get_spark_config(instance_type)

    print(f"Instance Type: {instance_type}")
    print(f"  vCPUs: {cores}")
    print(f"  Memory: {memory_gb} GB")
    print()
    print("Spark Configuration:")
    for key, value in sorted(config.items()):
        print(f"  {key}={value}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        print_config(sys.argv[1])
    else:
        print("Usage: python spark_config.py <instance-type>")
        print("Example: python spark_config.py c6i.2xlarge")
