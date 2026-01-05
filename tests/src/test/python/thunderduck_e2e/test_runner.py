"""Base test runner for thunderduck end-to-end tests."""

import unittest
import subprocess
import time
import os
import socket
from contextlib import closing
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np


def is_port_open(host, port):
    """Check if a port is open."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        return sock.connect_ex((host, port)) == 0


class ThunderduckE2ETestBase(unittest.TestCase):
    """Base class for all end-to-end tests."""

    spark = None
    server_process = None

    @classmethod
    def setUpClass(cls):
        """Start thunderduck server and create PySpark session."""
        # Check if server is already running (e.g., started by Maven)
        if not is_port_open('localhost', 15002):
            cls.server_process = cls._start_thunderduck_server()
            cls._wait_for_server()

        # Create PySpark session connected to thunderduck
        cls.spark = SparkSession.builder \
            .appName("thunderduck-e2e-tests") \
            .remote("sc://localhost:15002") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()

        # Setup test data
        cls._setup_test_data()

    @classmethod
    def tearDownClass(cls):
        """Stop PySpark session and thunderduck server."""
        if cls.spark:
            cls.spark.stop()

        if cls.server_process:
            cls._stop_thunderduck_server(cls.server_process)

    @staticmethod
    def _start_thunderduck_server():
        """Start thunderduck Spark Connect server."""
        jar_path = os.path.join(
            os.path.dirname(__file__),
            "../../../../../connect-server/target/thunderduck-connect-server-0.1.0-SNAPSHOT.jar"
        )

        if not os.path.exists(jar_path):
            raise FileNotFoundError(
                f"thunderduck server JAR not found at {jar_path}. "
                "Run 'mvn package' first."
            )

        # Required JVM flags for Apache Arrow (all platforms)
        # See: https://arrow.apache.org/docs/java/install.html
        # Using the exact format recommended by Arrow's error message
        cmd = [
            "java",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "-jar",
            jar_path
        ]

        # Don't capture stderr so we can see server errors in real-time
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return process

    @staticmethod
    def _stop_thunderduck_server(process):
        """Stop thunderduck server."""
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

    @classmethod
    def _wait_for_server(cls, timeout=30):
        """Wait for the server to be ready."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if is_port_open('localhost', 15002):
                time.sleep(2)  # Extra wait for full initialization
                return
            time.sleep(1)

        raise TimeoutError("thunderduck server did not start within timeout")

    @classmethod
    def _setup_test_data(cls):
        """Create test tables and data."""
        from datetime import date

        # Create sample employees table
        employees_data = [
            (1, 'John', 'Engineering', 75000, date(2020, 1, 15)),
            (2, 'Jane', 'Marketing', 65000, date(2019, 3, 22)),
            (3, 'Bob', 'Engineering', 80000, date(2018, 7, 30)),
            (4, 'Alice', 'HR', 55000, date(2021, 5, 10)),
            (5, 'Charlie', 'Engineering', 70000, date(2019, 11, 3))
        ]
        employees_df = cls.spark.createDataFrame(
            employees_data,
            schema=['id', 'name', 'department', 'salary', 'hire_date']
        )
        employees_df.createOrReplaceTempView('employees')

        # Create departments table
        departments_data = [
            ('Engineering', 'Building A', 100),
            ('Marketing', 'Building B', 50),
            ('HR', 'Building C', 25)
        ]
        departments_df = cls.spark.createDataFrame(
            departments_data,
            schema=['name', 'location', 'budget_millions']
        )
        departments_df.createOrReplaceTempView('departments')

    def assert_dataframes_equal(self, df1, df2, check_order=True, rtol=1e-5):
        """Assert two DataFrames are equal."""
        pdf1 = df1.toPandas()
        pdf2 = df2.toPandas()

        if not check_order:
            # Sort by all columns for comparison
            sort_cols = list(pdf1.columns)
            pdf1 = pdf1.sort_values(by=sort_cols).reset_index(drop=True)
            pdf2 = pdf2.sort_values(by=sort_cols).reset_index(drop=True)

        # Use pandas testing utilities
        pd.testing.assert_frame_equal(
            pdf1, pdf2,
            check_dtype=False,
            rtol=rtol
        )

    def assert_result_valid(self, df, expected_rows=None, expected_cols=None):
        """Basic validation of query results."""
        self.assertIsNotNone(df)

        if expected_rows is not None:
            actual_rows = df.count()
            self.assertEqual(
                actual_rows, expected_rows,
                f"Expected {expected_rows} rows, got {actual_rows}"
            )

        if expected_cols is not None:
            actual_cols = len(df.columns)
            self.assertEqual(
                actual_cols, expected_cols,
                f"Expected {expected_cols} columns, got {actual_cols}"
            )
