from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import os
import re
from pathlib import Path
import requests
import base64
import json

args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def normalize_time(created_at_str: str):
    formats = [
        # Ngày + giờ liền nhau
        "%Y%m%d-%H%M%S",  # 20261002-162530
        "%Y%m%dT%H%M%S",  # 20261002T162530
        # Ngày có dấu - + giờ liền nhau
        "%Y-%m-%d-%H%M%S",  # 2026-10-02-162530
        "%Y-%m-%dT%H%M%S",  # 2026-10-02T162530
        # Giờ có dấu -
        "%Y%m%d-%H-%M-%S",  # 20261002-16-25-30
        "%Y%m%dT%H-%M-%S",  # 20261002T16-25-30
        "%Y-%m-%d-%H-%M-%S",  # 2026-10-02-16-25-30
        "%Y-%m-%dT%H-%M-%S",  # 2026-10-02T16-25-30
    ]
    for format in formats:
        try:
            return datetime.strptime(created_at_str, format).strftime(
                "%d/%m/%YT%H:%M:%S"
            )
        except ValueError:
            continue
    return None


@dag(
    dag_id="api_share_c06",
    default_args=args,
    schedule="59 23 * * *",  # Chạy lúc 23:59 mỗi ngày (cuối ngày)
    catchup=False,
    tags=["api_share_c06"],
)
def api_share_c06():
    host_folder = os.getenv(
        "DATA_SHARE_HOST_FOLDER", "D:\\SONET\\MigrateDataImage\\data_share_c06"
    )
    container_folder = "/opt/airflow/data_share_c06"
    api_endpoint = os.getenv(
        "API_SHARE_DATA_C06_URL", "http://localhost:5051/api/v1/images"
    )

    scan_task = DockerOperator(
        task_id="scan_file_from_folder_share_data_c06",
        image="python:3.12-slim",
        # Quét đệ quy toàn bộ file, in mỗi đường dẫn ra 1 dòng → Airflow bắt làm XCom
        command=(
            'python -c "'
            "import os; "
            f"files = [os.path.join(r, f) for r, _, fs in os.walk('{container_folder}') for f in fs]; "
            '[print(p) for p in files]"'
        ),
        docker_url="tcp://host.docker.internal:2375",
        network_mode="host",
        auto_remove="force",
        do_xcom_push=True,  # Airflow bắt stdout làm XCom
        xcom_all=True,  # Lấy toàn bộ output (mỗi dòng = 1 file path)
        mounts=[
            Mount(
                source=host_folder,  # Thư mục gốc trên máy HOST
                target=container_folder,  # Thư mục bên trong container con
                type="bind",
                read_only=True,
            )
        ],
        mount_tmp_dir=False,
    )

    @task
    def parse_scanned_files(raw_output, scan_container_folder: str):
        """Parse XCom output từ DockerOperator thành list đường dẫn file.

        scan_container_folder: path mà scan_task dùng bên trong container của nó.
        Sẽ được thay bằng DATA_SHARE_CONTAINER_FOLDER (path Airflow worker truy cập được).
        """
        if not raw_output:
            print("[INFO] Không có file nào được quét.")
            return []

        if isinstance(raw_output, list):
            lines = []
            for item in raw_output:
                lines.extend(str(item).splitlines())
        else:
            lines = str(raw_output).splitlines()

        # Path mà Airflow worker (không phải scan container) có thể truy cập
        worker_folder = os.getenv("DATA_SHARE_CONTAINER_FOLDER", scan_container_folder)

        files = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Thay prefix container scan → prefix worker accessible
            normalized = line.replace(scan_container_folder, worker_folder, 1)
            files.append(normalized)

        print(f"[INFO] Quét được {len(files)} file:")
        for f in files:
            print(f"  - {f}")
        return files

    @task
    def post_api_share_data(files):

        arg_header = {
            "Content-Type": "application/json",
            "X-Access-Key": os.getenv("X-Access-Key", "Testa05"),
        }

        for file in files:
            path_obj = Path(file)
            print(f"ten file: {path_obj}")
            stem = path_obj.stem
            print(f"stem file: {stem}")

            parts = re.split(r"[_\-]", stem)
            print(f"parts: {parts}")
            cccd = parts[0]
            created_at = None
            try:
                if len(parts) > 1:
                    created_at = normalize_time(parts[1])
            except Exception:
                pass

            if created_at is None:
                try:
                    created_at = datetime.fromtimestamp(
                        path_obj.stat().st_mtime
                    ).strftime("%d/%m/%YT%H:%M:%S")
                except Exception:
                    created_at = datetime.now().strftime("%d/%m/%YT%H:%M:%S")

            with open(file, "rb") as f:
                image_base64 = base64.b64encode(f.read()).decode("utf-8")

            payload = {
                "SoDinhDanh": cccd,
                "HinhAnh": f"data:image/png;base64,{image_base64}",
                "created_at": created_at,
            }
            # Gửi API với cơ chế retry 1 lần
            max_attempts = 2
            for attempt in range(1, max_attempts + 1):
                try:
                    response = requests.post(
                        url=api_endpoint,
                        headers=arg_header,
                        json=payload,
                        timeout=30,
                    )
                    print(f"[{cccd}] attempt={attempt} status={response.status_code}")
                    if response.status_code < 500:
                        break

                    print(
                        f"[{cccd}] server error, retrying... resp={response.text[:200]}"
                    )
                except requests.exceptions.Timeout:
                    print(f"[{cccd}] attempt={attempt} timeout")
                except requests.exceptions.ConnectionError as e:
                    print(f"[{cccd}] attempt={attempt} connection error: {e}")

    result_parse_scanned_files = parse_scanned_files(scan_task.output, container_folder)
    post_api_share_data(result_parse_scanned_files)


api_share_c06()
