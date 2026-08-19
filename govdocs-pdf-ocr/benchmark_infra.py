import base64
import json
import time
from datetime import UTC, datetime

import boto3
import requests
from botocore.exceptions import ClientError

REGION = "us-east-1"
VPC_ID = "vpc-00619dc5f04c58395"
SUBNET_ID = "subnet-018f3bfbf26bbd485"
SECURITY_GROUP_NAME = "govdocs-distributed-benchmark"
ROLE_NAME = "govdocs-distributed-benchmark"
PROFILE_NAME = ROLE_NAME


def _client(profile: str, service: str):
    return boto3.Session(profile_name=profile, region_name=REGION).client(service)


def ensure_instance_profile(profile: str, bucket: str) -> str:
    iam = _client(profile, "iam")
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    try:
        iam.get_role(RoleName=ROLE_NAME)
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchEntity":
            raise
        iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="GovDocs Dask and Ray benchmark nodes",
        )

    for policy_arn in (
        "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
        "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy",
        "arn:aws:iam::aws:policy/AmazonEC2FullAccess",
    ):
        iam.attach_role_policy(RoleName=ROLE_NAME, PolicyArn=policy_arn)

    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="GovDocsBenchmarkArtifacts",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["s3:ListBucket"],
                        "Resource": f"arn:aws:s3:::{bucket}",
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "cloudwatch:GetMetricData",
                            "cloudwatch:GetMetricStatistics",
                            "ec2:DescribeInstances",
                        ],
                        "Resource": "*",
                    },
                    {
                        "Effect": "Allow",
                        "Action": "iam:PassRole",
                        "Resource": (
                            "arn:aws:iam::002645521087:role/"
                            "govdocs-distributed-benchmark"
                        ),
                    },
                ],
            }
        ),
    )

    try:
        instance_profile = iam.get_instance_profile(InstanceProfileName=PROFILE_NAME)[
            "InstanceProfile"
        ]
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchEntity":
            raise
        instance_profile = iam.create_instance_profile(
            InstanceProfileName=PROFILE_NAME
        )["InstanceProfile"]

    role_names = {role["RoleName"] for role in instance_profile.get("Roles", [])}
    if ROLE_NAME not in role_names:
        iam.add_role_to_instance_profile(
            InstanceProfileName=PROFILE_NAME, RoleName=ROLE_NAME
        )
        time.sleep(10)
    return instance_profile["Arn"]


def ensure_security_group(profile: str) -> str:
    ec2 = _client(profile, "ec2")
    groups = ec2.describe_security_groups(
        Filters=[
            {"Name": "group-name", "Values": [SECURITY_GROUP_NAME]},
            {"Name": "vpc-id", "Values": [VPC_ID]},
        ]
    )["SecurityGroups"]
    if groups:
        group_id = groups[0]["GroupId"]
    else:
        group_id = ec2.create_security_group(
            GroupName=SECURITY_GROUP_NAME,
            Description="GovDocs Dask and Ray benchmark",
            VpcId=VPC_ID,
        )["GroupId"]
        ec2.create_tags(
            Resources=[group_id],
            Tags=[{"Key": "project", "Value": "govdocs-distributed-benchmark"}],
        )

    public_ip = requests.get("https://checkip.amazonaws.com", timeout=10).text.strip()
    permissions = [
        {
            "IpProtocol": "-1",
            "UserIdGroupPairs": [{"GroupId": group_id}],
        },
        {
            "IpProtocol": "tcp",
            "FromPort": 22,
            "ToPort": 22,
            "IpRanges": [
                {"CidrIp": f"{public_ip}/32", "Description": "benchmark driver"}
            ],
        },
        {
            "IpProtocol": "tcp",
            "FromPort": 8265,
            "ToPort": 8787,
            "IpRanges": [
                {"CidrIp": f"{public_ip}/32", "Description": "framework APIs"}
            ],
        },
        {
            "IpProtocol": "tcp",
            "FromPort": 10001,
            "ToPort": 10001,
            "IpRanges": [{"CidrIp": f"{public_ip}/32", "Description": "Ray client"}],
        },
    ]
    for permission in permissions:
        try:
            ec2.authorize_security_group_ingress(
                GroupId=group_id, IpPermissions=[permission]
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "InvalidPermission.Duplicate":
                raise
    return group_id


def allow_ray_head_worker_ssh(
    profile: str, security_group_id: str, cluster_name: str
) -> str:
    ec2 = _client(profile, "ec2")
    instances = ec2.describe_instances(
        Filters=[
            {"Name": "tag:ray-cluster-name", "Values": [cluster_name]},
            {"Name": "tag:ray-node-type", "Values": ["head"]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ]
    )["Reservations"]
    head = next(
        instance
        for reservation in instances
        for instance in reservation["Instances"]
    )
    cidr = f"{head['PublicIpAddress']}/32"
    try:
        ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [
                        {
                            "CidrIp": cidr,
                            "Description": "Ray head worker bootstrap",
                        }
                    ],
                }
            ],
        )
    except ClientError as error:
        if error.response["Error"]["Code"] != "InvalidPermission.Duplicate":
            raise
    return cidr


def revoke_ray_head_worker_ssh(
    profile: str, security_group_id: str, cidr: str | None
) -> None:
    if not cidr:
        return
    ec2 = _client(profile, "ec2")
    try:
        ec2.revoke_security_group_ingress(
            GroupId=security_group_id,
            IpProtocol="tcp",
            FromPort=22,
            ToPort=22,
            CidrIp=cidr,
        )
    except ClientError as error:
        if error.response["Error"]["Code"] != "InvalidPermission.NotFound":
            raise


def metrics_bootstrap(namespace: str) -> list[str]:
    config = {
        "agent": {"metrics_collection_interval": 10, "run_as_user": "root"},
        "metrics": {
            "namespace": namespace,
            "append_dimensions": {
                "InstanceId": "${aws:InstanceId}",
                "InstanceType": "${aws:InstanceType}",
            },
            "metrics_collected": {
                "cpu": {
                    "measurement": ["cpu_usage_active"],
                    "metrics_collection_interval": 10,
                    "resources": ["*"],
                    "totalcpu": True,
                },
                "mem": {
                    "measurement": ["mem_used_percent"],
                    "metrics_collection_interval": 10,
                },
            },
        },
    }
    payload = base64.b64encode(json.dumps(config).encode()).decode()
    return [
        (
            "curl --retry 5 --retry-all-errors --connect-timeout 30 "
            "-fsSLo /tmp/amazon-cloudwatch-agent.deb "
            "https://amazoncloudwatch-agent.s3.amazonaws.com/ubuntu/amd64/latest/"
            "amazon-cloudwatch-agent.deb"
        ),
        "dpkg -i /tmp/amazon-cloudwatch-agent.deb",
        f"echo {payload} | base64 -d > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json",
        (
            "/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl "
            "-a fetch-config -m ec2 -s -c "
            "file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json"
        ),
    ]


def tagged_instances(profile: str, framework: str, run_id: str) -> list[dict]:
    ec2 = _client(profile, "ec2")
    reservations = ec2.describe_instances(
        Filters=[
            {"Name": "tag:benchmark-framework", "Values": [framework]},
            {"Name": "tag:benchmark-run-id", "Values": [run_id]},
            {
                "Name": "instance-state-name",
                "Values": [
                    "pending",
                    "running",
                    "stopping",
                    "stopped",
                    "shutting-down",
                ],
            },
        ]
    )["Reservations"]
    return [
        instance
        for reservation in reservations
        for instance in reservation["Instances"]
    ]


def collect_metrics(
    profile: str,
    namespace: str,
    instances: list[dict],
    started_at: datetime,
    completed_at: datetime,
) -> dict:
    cloudwatch = _client(profile, "cloudwatch")
    completed_at = completed_at.astimezone(UTC)
    started_at = started_at.astimezone(UTC)
    per_node = {}
    for instance in instances:
        instance_id = instance["InstanceId"]
        instance_type = instance["InstanceType"]
        dimensions = [
            {"Name": "InstanceId", "Value": instance_id},
            {"Name": "InstanceType", "Value": instance_type},
        ]
        node = {
            "instance_id": instance_id,
            "instance_type": instance_type,
            "launch_time": instance["LaunchTime"].astimezone(UTC).isoformat(),
        }
        for output_key, metric_name, extra_dimensions in (
            (
                "cpu_percent",
                "cpu_usage_active",
                [{"Name": "cpu", "Value": "cpu-total"}],
            ),
            ("memory_percent", "mem_used_percent", []),
        ):
            response = cloudwatch.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=dimensions + extra_dimensions,
                StartTime=started_at,
                EndTime=completed_at,
                Period=10,
                Statistics=["Average", "Maximum"],
            )
            datapoints = sorted(
                response["Datapoints"], key=lambda value: value["Timestamp"]
            )
            node[output_key] = [
                {
                    "timestamp": value["Timestamp"].astimezone(UTC).isoformat(),
                    "average": value["Average"],
                    "maximum": value["Maximum"],
                }
                for value in datapoints
            ]
        per_node[instance_id] = node
    return {
        "namespace": namespace,
        "period_seconds": 10,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "nodes": per_node,
    }
