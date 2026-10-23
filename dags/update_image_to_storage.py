from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import os
import re
from pathlib import Path


args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="update_image_to_storage",
    default_args=args,
    schedule="@daily",
    catchup=False,
    tags=["image"],
)
def update_image_to_storage():

    # Đường dẫn thư mục trên máy HOST (đọc từ biến môi trường DATA_UPDATE_HOST_FOLDER trong worker.env)
    host_folder = os.getenv("DATA_UPDATE_HOST_FOLDER", "C:/data_update")
    # Đường dẫn hiển thị bên trong container con
    container_folder = "/opt/airflow/data_update"

    # ──────────────────────────────────────────────
    # Task 1: Mount thư mục từ HOST, quét danh sách file
    # ──────────────────────────────────────────────
    scan_task = DockerOperator(
        task_id="scan_file_from_folder",
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
    def parse_scanned_files(raw_output):
        """Parse XCom output từ DockerOperator thành list đường dẫn file."""
        if not raw_output:
            print("[INFO] Không có file nào được quét.")
            return []

        if isinstance(raw_output, list):
            # Mỗi item trong list có thể chứa nhiều path ghép bằng \n
            lines = []
            for item in raw_output:
                lines.extend(str(item).splitlines())
        else:
            lines = str(raw_output).splitlines()

        files = [line.strip() for line in lines if line.strip()]
        print(f"[INFO] Quét được {len(files)} file:")
        for f in files:
            print(f"  - {f}")
        return files

    @task
    def init_metadata_img(files_path):

        folder_root = os.getenv("S3_FOLDER_ROOT", "image_root")
        bucket = os.getenv("S3_BUCKET", "images")

        s3Uris = []
        date_folder = created_at_str = datetime.now().strftime("%Y%m%dT%H%M%S")

        for file_path in files_path:
            try:
                path_obj = Path(file_path)
                file_name = path_obj.name
                stem = (
                    path_obj.stem
                )  # Ví dụ: '010212345678' hoặc '010212345678_20260910'

                parts = re.split(r"[_\-]", stem)
                cccd = parts[0]
                # Lấy thời gian tạo/sửa của file vật lý
                try:
                    created_at = datetime.fromtimestamp(
                        path_obj.stat().st_mtime
                    ).strftime("%Y%m%dT%H%M%S")
                except Exception:
                    created_at = datetime.now().strftime("%Y%m%dT%H%M%S")

                key_record = f"{cccd}_{created_at}"

                s3_uri = f"{date_folder}/{key_record}.jpg"

                metadata = {
                    "cccd": cccd,
                    "key_record": key_record,
                    "created_at": created_at,
                    "s3_uri": s3_uri,
                    "file_path": file_path,  # cần thiết cho upload_to_minio
                }
                s3Uris.append(metadata)

            except Exception as e:
                print(f"Error parsing metadata for {file_path}: {e}")

        return s3Uris

    @task
    def upload_to_minio(metadata_img):

        bucket = os.getenv("S3_BUCKET", "images")
        s3_hook = S3Hook(aws_conn_id="s3_upload_images_cccd")
        status_upload = []
        for metadata in metadata_img:
            file_path = metadata["file_path"]
            s3_uri = metadata["s3_uri"]
            try:

                s3_hook.load_file(
                    filename=file_path,
                    key=s3_uri,
                    bucket_name=bucket,
                    replace=True,
                )
                metadata["status"] = "success"
                status_upload.append(metadata)

            except Exception as e:
                metadata["status"] = "failed"
                metadata["error"] = str(e)
                status_upload.append(metadata)
                print(f"Error uploading file {file_path}: {e}")

        return status_upload

    @task
    def update_metadata_to_db(satus_upload_s3):
        if not satus_upload_s3:
            print("Không có bản ghi nào để cập nhật database.")
            return

        records = [
            (
                item["key_record"],
                item["cccd"],
                item["created_at"],
                item["s3_uri"],
            )
            for item in satus_upload_s3
            if item.get("status") == "success"
        ]

        if not records:
            print("Không có file nào upload thành công để insert DB.")
            return

        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO images_metadata (key_record, cccd, created_at, s3_uri)
            VALUES (%s, %s, %s, %s)
        """

        try:
            from psycopg2.extras import execute_batch

            execute_batch(cursor, insert_query, records, page_size=1000)
            conn.commit()
            print(
                f"Đã insert thành công {len(records)} bản ghi vào bảng images_metadata"
            )
        except Exception as e:
            conn.rollback()
            print(f"Lỗi khi insert metadata vào Postgres: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    # ──────────────────────────────────────────────
    # Luồng chạy DAG
    # ──────────────────────────────────────────────
    files_path = parse_scanned_files(scan_task.output)
    metadata_img = init_metadata_img(files_path)
    satus_upload_s3 = upload_to_minio(metadata_img)
    update_metadata_to_db(satus_upload_s3)


update_image_to_storage()
