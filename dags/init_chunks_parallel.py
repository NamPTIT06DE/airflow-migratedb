import os
import shlex
from datetime import datetime, timedelta

from airflow.decorators import dag
# from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.ssh.operators.ssh import SSHOperator


INIT_CHUNK_IMAGE = os.getenv(
    "INIT_CHUNK_IMAGE", "service-init-chunk-data:v1"
)
PLATFORM_NETWORK = os.getenv(
    "PLATFORM_DOCKER_NETWORK", "platform_default"
)
SSH_CONN_ID = os.getenv(
    "DOCKER_HOST_SSH_CONN_ID", "docker_host_run_images"
)
INIT_CHUNK_ENV_FILE = os.getenv(
    "INIT_CHUNK_ENV_FILE",
    "/home/nguyennam/SONET/airflow-migratedb/config/init_chunk_data.env",
)


def _docker_run_command() -> str:
    """Chạy image init chunk có sẵn trên Docker host."""
    return " ".join(
        [
            "docker run",
            "--pull=never",
            "--rm",
            "--network",
            shlex.quote(PLATFORM_NETWORK),
            "--env-file",
            shlex.quote(INIT_CHUNK_ENV_FILE),
            shlex.quote(INIT_CHUNK_IMAGE),
        ]
    )


@dag(
    dag_id="init_chunks",
    description=(
        "Chạy image one-shot để tạo bảng PostgreSQL và khởi tạo Oracle chunks"
    ),
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["init_chunks", "oracle", "postgres", "docker", "ssh"],
)
def init_chunks():
    # Phương án đang dùng: chạy image trên Docker host thông qua SSH.
    SSHOperator(
        task_id="run_init_chunk_image",
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
    #     task_id="run_init_chunk_image",
    #     image=INIT_CHUNK_IMAGE,
    #     docker_url=os.getenv("DOCKER_URL", "unix:///var/run/docker.sock"),
    #     network_mode=PLATFORM_NETWORK,
    #     env_file="/opt/airflow/config/init_chunk_data.env",
    #     mount_tmp_dir=False,
    #     auto_remove="force",
    #     force_pull=False,
    #     do_xcom_push=False,
    # )


init_chunks()
