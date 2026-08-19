import socket
import time
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError

from benchmark_infra import (
    PROFILE_NAME,
    REGION,
    SUBNET_ID,
    metrics_bootstrap,
)

BASE_AMI_ID = "ami-042d92d7f194cd14c"


@dataclass
class DaskEc2Cluster:
    profile: str
    run_id: str
    image: str
    namespace: str
    security_group_id: str
    scheduler_instance_type: str
    worker_instance_type: str
    worker_count: int

    def __post_init__(self):
        self.ec2 = boto3.Session(profile_name=self.profile, region_name=REGION).client(
            "ec2"
        )
        self.instances = []

    def _user_data(self, command: str) -> str:
        commands = [
            "#!/bin/bash",
            "set -euxo pipefail",
            *metrics_bootstrap(self.namespace),
            f"docker pull {self.image}",
            command,
        ]
        return "\n".join(commands) + "\n"

    def _launch(
        self,
        instance_type: str,
        count: int,
        node_kind: str,
        command: str,
    ) -> list[dict]:
        response = self.ec2.run_instances(
            ImageId=BASE_AMI_ID,
            InstanceType=instance_type,
            MinCount=count,
            MaxCount=count,
            SubnetId=SUBNET_ID,
            SecurityGroupIds=[self.security_group_id],
            IamInstanceProfile={"Name": PROFILE_NAME},
            UserData=self._user_data(command),
            Monitoring={"Enabled": True},
            InstanceInitiatedShutdownBehavior="terminate",
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {
                            "Key": "Name",
                            "Value": f"dask-{self.run_id}-{node_kind}",
                        },
                        {
                            "Key": "project",
                            "Value": "govdocs-distributed-benchmark",
                        },
                        {"Key": "benchmark-framework", "Value": "dask"},
                        {"Key": "benchmark-run-id", "Value": self.run_id},
                        {"Key": "benchmark-node-kind", "Value": node_kind},
                    ],
                },
                {
                    "ResourceType": "volume",
                    "Tags": [
                        {
                            "Key": "project",
                            "Value": "govdocs-distributed-benchmark",
                        }
                    ],
                },
            ],
        )
        instances = response["Instances"]
        self.instances.extend(instances)
        return instances

    def start(self) -> str:
        scheduler_command = (
            "exec docker run --rm --network host "
            "--name dask-scheduler "
            f"{self.image} "
            "python -m distributed.cli.dask_scheduler "
            "--host 0.0.0.0 --port 8786 --dashboard-address :8787"
        )
        scheduler = self._launch(
            self.scheduler_instance_type,
            1,
            "scheduler",
            scheduler_command,
        )[0]
        self.ec2.get_waiter("instance_running").wait(
            InstanceIds=[scheduler["InstanceId"]]
        )
        scheduler = self.ec2.describe_instances(InstanceIds=[scheduler["InstanceId"]])[
            "Reservations"
        ][0]["Instances"][0]
        private_ip = scheduler["PrivateIpAddress"]
        public_ip = scheduler["PublicIpAddress"]

        worker_processes = 1 if self.worker_instance_type == "m7i.large" else 8
        worker_threads = 2 if self.worker_instance_type == "m7i.large" else 8
        worker_command = (
            "exec docker run --rm --network host "
            "--name dask-worker "
            "--ulimit nofile=65536:65536 "
            f"{self.image} "
            "python -m distributed.cli.dask_worker "
            f"tcp://{private_ip}:8786 "
            f"--nworkers {worker_processes} --nthreads {worker_threads} "
            "--memory-limit 0 --no-dashboard"
        )
        if self.worker_count:
            self._launch(
                self.worker_instance_type,
                self.worker_count,
                "worker",
                worker_command,
            )
        return f"tcp://{public_ip}:8786"

    def wait_for_scheduler(self, timeout: int = 600) -> None:
        scheduler = next(
            instance
            for instance in self.describe_instances()
            if self._tag(instance, "benchmark-node-kind") == "scheduler"
        )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(
                    (scheduler["PublicIpAddress"], 8786), timeout=2
                ):
                    return
            except OSError:
                time.sleep(2)
        raise TimeoutError("Dask scheduler did not open port 8786")

    def describe_instances(self) -> list[dict]:
        instance_ids = [instance["InstanceId"] for instance in self.instances]
        if not instance_ids:
            return []
        for attempt in range(10):
            try:
                response = self.ec2.describe_instances(InstanceIds=instance_ids)
                return [
                    instance
                    for reservation in response["Reservations"]
                    for instance in reservation["Instances"]
                ]
            except ClientError as error:
                if (
                    error.response["Error"]["Code"] != "InvalidInstanceID.NotFound"
                    or attempt == 9
                ):
                    raise
                time.sleep(2)
        raise AssertionError("unreachable")

    def close(self) -> None:
        instance_ids = [
            instance["InstanceId"]
            for instance in self.describe_instances()
            if instance["State"]["Name"] not in {"shutting-down", "terminated"}
        ]
        if instance_ids:
            self.ec2.terminate_instances(InstanceIds=instance_ids)

    @staticmethod
    def _tag(instance: dict, key: str) -> str | None:
        return next(
            (tag["Value"] for tag in instance.get("Tags", []) if tag["Key"] == key),
            None,
        )
