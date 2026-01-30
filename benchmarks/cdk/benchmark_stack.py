"""
AWS CDK Stack for Thunderduck Benchmark Infrastructure

Creates:
- Security Group with SSH access
- SSH Key Pair (stored in SSM Parameter Store)
- IAM Role with S3 access
- EC2 Instances (one per engine)
"""
from typing import List

from aws_cdk import (
    CfnOutput,
    RemovalPolicy,
    Stack,
    Tags,
    aws_ec2 as ec2,
    aws_iam as iam,
)
from constructs import Construct


class BenchmarkStack(Stack):
    """CDK stack for Thunderduck benchmark infrastructure."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        instance_type: str,
        engines: List[str],
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Tag all resources in this stack
        Tags.of(self).add("Project", "thunderduck-benchmark")
        Tags.of(self).add("ManagedBy", "CDK")

        # 1. Use default VPC
        vpc = ec2.Vpc.from_lookup(self, "DefaultVpc", is_default=True)

        # 2. Security Group with SSH access
        security_group = ec2.SecurityGroup(
            self,
            "BenchmarkSecurityGroup",
            vpc=vpc,
            description="Thunderduck benchmark - SSH access",
            allow_all_outbound=True,
        )
        security_group.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(22),
            "SSH access from anywhere",
        )

        # 3. SSH Key Pair (CDK creates and stores private key in SSM)
        key_pair = ec2.KeyPair(
            self,
            "BenchmarkKeyPair",
            key_pair_name=f"thunderduck-bench-{construct_id}",
            type=ec2.KeyPairType.RSA,
        )
        # Key pair private key is automatically stored in SSM Parameter Store
        # at /ec2/keypair/{key_pair_id}

        # 4. IAM Role for EC2 instances with S3 access
        instance_role = iam.Role(
            self,
            "BenchmarkInstanceRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            description="Role for Thunderduck benchmark EC2 instances",
            managed_policies=[
                # S3 access for benchmark data
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                # SSM access for debugging (optional but useful)
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"),
            ],
        )

        # 5. Machine image - Amazon Linux 2023
        # Detect if ARM instance type for correct AMI
        is_arm = any(arm in instance_type for arm in ["g.", "gd.", "g1.", "a1."])
        if is_arm:
            machine_image = ec2.MachineImage.latest_amazon_linux2023(
                cpu_type=ec2.AmazonLinuxCpuType.ARM_64,
            )
        else:
            machine_image = ec2.MachineImage.latest_amazon_linux2023(
                cpu_type=ec2.AmazonLinuxCpuType.X86_64,
            )

        # 6. Create EC2 instances - one per engine
        instances = {}
        for engine in engines:
            # Sanitize engine name for CloudFormation logical ID
            safe_engine_name = engine.replace(".", "-").replace("_", "-")

            instance = ec2.Instance(
                self,
                f"Instance{safe_engine_name}",
                instance_type=ec2.InstanceType(instance_type),
                machine_image=machine_image,
                vpc=vpc,
                security_group=security_group,
                key_pair=key_pair,
                role=instance_role,
                vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
                # Ensure public IP for SSH access
                associate_public_ip_address=True,
                # Root volume - 100GB for benchmark data
                block_devices=[
                    ec2.BlockDevice(
                        device_name="/dev/xvda",
                        volume=ec2.BlockDeviceVolume.ebs(
                            volume_size=100,
                            volume_type=ec2.EbsDeviceVolumeType.GP3,
                            delete_on_termination=True,
                        ),
                    ),
                ],
            )

            # Tag with engine name
            Tags.of(instance).add("thunderduck-bench-engine", engine)
            Tags.of(instance).add("Name", f"thunderduck-bench-{engine}")

            instances[engine] = instance

            # Output instance public IP
            CfnOutput(
                self,
                f"InstanceIP{safe_engine_name}",
                value=instance.instance_public_ip,
                description=f"Public IP for {engine} instance",
                export_name=f"{construct_id}-IP-{safe_engine_name}",
            )

            # Output instance ID
            CfnOutput(
                self,
                f"InstanceID{safe_engine_name}",
                value=instance.instance_id,
                description=f"Instance ID for {engine}",
            )

        # Output SSH key retrieval command
        CfnOutput(
            self,
            "SSHKeyCommand",
            value=(
                f"aws ssm get-parameter "
                f"--name /ec2/keypair/{key_pair.key_pair_id} "
                f"--with-decryption "
                f"--query Parameter.Value "
                f"--output text > benchmark_key.pem && chmod 600 benchmark_key.pem"
            ),
            description="Command to retrieve SSH private key from SSM",
        )

        # Output security group ID
        CfnOutput(
            self,
            "SecurityGroupId",
            value=security_group.security_group_id,
            description="Security group ID",
        )

        # Output key pair name
        CfnOutput(
            self,
            "KeyPairName",
            value=key_pair.key_pair_name,
            description="EC2 key pair name",
        )

        # Output all engine names
        CfnOutput(
            self,
            "Engines",
            value=",".join(engines),
            description="Comma-separated list of engines",
        )
