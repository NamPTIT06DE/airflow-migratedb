import os
import shlex
from datetime import datetime, timedelta

from airflow.decorators import dag
# from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.ssh.operators.ssh import SSHOperator


GRANT_IMAGE = os.getenv("GRANT_ORACLE_IMAGE", "service-grant-oracle:v1")
PLATFORM_NETWORK = os.getenv("PLATFORM_DOCKER_NETWORK", "platform_default")
SSH_CONN_ID = os.getenv("DOCKER_HOST_SSH_CONN_ID", "docker_host_run_images")
GRANT_ENV_FILE = os.getenv(
    "GRANT_ORACLE_ENV_FILE",
    "/home/nguyennam/SONET/airflow-migratedb/config/grant_oracle.env",
)


def _docker_run_command() -> str:
    """Tạo lệnh chạy image có sẵn trên Docker host, không pull lại image."""
    return " ".join(
        [
            "docker run",
            "--pull=never",
            "--rm",
            "--network",
            shlex.quote(PLATFORM_NETWORK),
            "--env-file",
            shlex.quote(GRANT_ENV_FILE),
            shlex.quote(GRANT_IMAGE),
        ]
    )


@dag(
    dag_id="grant_oracle_privileges",
    description="Chạy image one-shot qua SSH để cấp và kiểm tra quyền Oracle",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["oracle", "setup", "docker", "ssh"],
)
def grant_oracle_privileges():
    # Phương án đang dùng: chạy image trên Docker host thông qua SSH.
    SSHOperator(
        task_id="run_grant_oracle_image",
        ssh_conn_id=SSH_CONN_ID,
        command=_docker_run_command(),
        cmd_timeout=None,
        get_pty=True,
        do_xcom_push=False,
    )

    # Phương án DockerOperator:
    # Khi chuyển sang phương án này, hãy comment khối SSHOperator phía trên,
    # bỏ comment import DockerOperator và khối bên dưới.
    # DockerOperator(
    #     task_id="run_grant_oracle_image",
    #     image=GRANT_IMAGE,
    #     docker_url=os.getenv("DOCKER_URL", "unix:///var/run/docker.sock"),
    #     network_mode=PLATFORM_NETWORK,
    #     env_file="/opt/airflow/config/grant_oracle.env",
    #     mount_tmp_dir=False,
    #     auto_remove="force",
    #     force_pull=False,
    #     do_xcom_push=False,
    # )


grant_oracle_privileges()
