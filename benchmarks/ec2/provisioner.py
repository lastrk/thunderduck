#!/usr/bin/env python3
"""
EC2 Provisioning Module

Provides SSH access to EC2 instances managed by CDK.
Reads instance information from CloudFormation stack outputs.
"""
import time
from typing import Dict, List, Optional

import boto3
import paramiko
from botocore.exceptions import ClientError


class CDKProvisioner:
    """Reads CDK stack outputs and provides SSH access to benchmark instances."""

    def __init__(
        self,
        stack_name: str,
        key_file: str,
        region: Optional[str] = None,
    ):
        """Initialize provisioner with CDK stack information.

        Args:
            stack_name: Name of the CDK stack
            key_file: Path to SSH private key file
            region: AWS region (default: from environment)
        """
        self.stack_name = stack_name
        self.key_file = key_file
        self.region = region or boto3.session.Session().region_name or "us-east-1"

        self.cfn = boto3.client("cloudformation", region_name=self.region)
        self._ssh_clients: Dict[str, paramiko.SSHClient] = {}

        self.instance_ips: Dict[str, str] = {}
        self.instance_ids: Dict[str, str] = {}
        self.engines: List[str] = []

        self._load_stack_outputs()

    def _load_stack_outputs(self) -> None:
        """Load instance information from CloudFormation stack outputs."""
        try:
            response = self.cfn.describe_stacks(StackName=self.stack_name)
        except ClientError as e:
            raise RuntimeError(
                f"Failed to describe stack '{self.stack_name}': {e}. "
                f"Make sure the stack is deployed with 'cdk deploy'."
            )

        if not response["Stacks"]:
            raise RuntimeError(f"Stack '{self.stack_name}' not found")

        outputs = response["Stacks"][0].get("Outputs", [])

        for output in outputs:
            key = output["OutputKey"]
            value = output["OutputValue"]

            if key.startswith("InstanceIP"):
                # Extract engine name from key like "InstanceIPthunderduck"
                engine = key.replace("InstanceIP", "").replace("-", ".")
                self.instance_ips[engine] = value
            elif key.startswith("InstanceID"):
                engine = key.replace("InstanceID", "").replace("-", ".")
                self.instance_ids[engine] = value
            elif key == "Engines":
                self.engines = [e.strip() for e in value.split(",")]

        if not self.instance_ips:
            raise RuntimeError(
                f"No instance IPs found in stack '{self.stack_name}' outputs. "
                f"Check that the stack deployed successfully."
            )

        print(f"Loaded {len(self.instance_ips)} instances from stack '{self.stack_name}':")
        for engine, ip in self.instance_ips.items():
            print(f"  {engine}: {ip}")

    def get_engines(self) -> List[str]:
        """Get list of engines from stack."""
        return self.engines or list(self.instance_ips.keys())

    def get_ssh_client(
        self,
        engine: str,
        username: str = "ec2-user",
        timeout: int = 300,
    ) -> paramiko.SSHClient:
        """Get SSH client for an engine's instance.

        Args:
            engine: Engine name (e.g., "thunderduck", "spark-3.5.3")
            username: SSH username (default: ec2-user for Amazon Linux)
            timeout: Connection timeout in seconds

        Returns:
            Connected paramiko SSHClient
        """
        if engine in self._ssh_clients:
            return self._ssh_clients[engine]

        ip = self.instance_ips.get(engine)
        if not ip:
            # Try with normalized name
            normalized = engine.replace(".", "-")
            ip = self.instance_ips.get(normalized)

        if not ip:
            available = list(self.instance_ips.keys())
            raise ValueError(
                f"No IP found for engine '{engine}'. Available engines: {available}"
            )

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Wait for SSH to be available
        start_time = time.time()
        last_error = None

        while time.time() - start_time < timeout:
            try:
                ssh.connect(
                    hostname=ip,
                    username=username,
                    key_filename=self.key_file,
                    timeout=10,
                )
                self._ssh_clients[engine] = ssh
                print(f"[{engine}] SSH connected to {ip}")
                return ssh
            except Exception as e:
                last_error = e
                time.sleep(5)

        raise TimeoutError(
            f"Could not connect to {engine} at {ip} after {timeout}s: {last_error}"
        )

    def close_all(self) -> None:
        """Close all SSH connections."""
        for engine, ssh in self._ssh_clients.items():
            try:
                ssh.close()
            except Exception:
                pass
        self._ssh_clients.clear()

    @staticmethod
    def get_ssh_key_from_ssm(stack_name: str, output_file: str, region: str = None) -> str:
        """Retrieve SSH private key from SSM Parameter Store.

        Args:
            stack_name: CDK stack name
            output_file: Path to write the key file
            region: AWS region

        Returns:
            Path to the key file
        """
        cfn = boto3.client("cloudformation", region_name=region)
        ssm = boto3.client("ssm", region_name=region)

        # Get key pair ID from stack outputs
        response = cfn.describe_stacks(StackName=stack_name)
        outputs = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0].get("Outputs", [])}

        # The SSH key command output contains the parameter name
        ssh_cmd = outputs.get("SSHKeyCommand", "")
        if not ssh_cmd:
            raise RuntimeError("SSHKeyCommand not found in stack outputs")

        # Extract parameter name from command
        # Format: aws ssm get-parameter --name /ec2/keypair/{key_id} ...
        import re
        match = re.search(r"--name (/ec2/keypair/[^\s]+)", ssh_cmd)
        if not match:
            raise RuntimeError(f"Could not parse parameter name from: {ssh_cmd}")

        param_name = match.group(1)

        # Get private key from SSM
        response = ssm.get_parameter(Name=param_name, WithDecryption=True)
        private_key = response["Parameter"]["Value"]

        # Write to file
        import os
        with open(output_file, "w") as f:
            f.write(private_key)
        os.chmod(output_file, 0o600)

        print(f"SSH key saved to: {output_file}")
        return output_file


# Legacy support - keep old class for backwards compatibility
class EC2Provisioner(CDKProvisioner):
    """Legacy alias for CDKProvisioner."""

    def __init__(self, stack_name: str, key_file: str, **kwargs):
        super().__init__(stack_name, key_file, **kwargs)
