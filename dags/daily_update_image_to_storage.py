import os
import re
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="daily_update_image_to_storage",
    default_args=args,
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["image", "daily"],
)
def daily_update_image_to_storage():
    data_update_folder = os.getenv(
        "DATA_UPDATE_CONTAINER_FOLDER",
        "/opt/airflow/data_update",
    )

    @task
    def scan_previous_day_files(root_folder: str) -> list[str]:
        """Quét toàn bộ file trong thư mục YYYYMMDD của ngày hôm trước."""
        context = get_current_context()
        run_time = context["data_interval_end"]
        previous_day = (run_time - timedelta(days=1)).strftime("%Y%m%d")
        scan_folder = Path(root_folder) / previous_day

        print(f"[INFO] Thời gian chạy DAG: {run_time.isoformat()}")
        print(f"[INFO] Thư mục ngày hôm trước: {scan_folder}")

        if not scan_folder.is_dir():
            print(f"[WARN] Thư mục không tồn tại: {scan_folder}")
            return []

        files = sorted(
            str(path)
            for path in scan_folder.rglob("*")
            if path.is_file()
        )

        print(f"[INFO] Quét được {len(files)} file trong {scan_folder}")
        for file_path in files:
            print(f"  - {file_path}")
        return files

    @task
    def init_metadata_img(files_path: list[str]) -> list[dict]:
        metadata_images = []

        for file_path in files_path:
            try:
                path_obj = Path(file_path)
                file_name = path_obj.name
                date_folder = path_obj.parent.name
                stem = path_obj.stem

                parts = re.split(r"[_\-]", stem)
                cccd = parts[0]

                time_text = stem[len(cccd) :].lstrip("_-")
                time_formats = [
                    "%Y%m%d%H%M%S",   # 20260910162530
                    "%Y%m%dT%H%M%S",  # 20260910T162530
                    "%Y%m%d_%H%M%S",  # 20260910_162530
                ]

                try:
                    for time_format in time_formats:
                        try:
                            created_at = datetime.strptime(
                                time_text,
                                time_format,
                            ).strftime("%Y%m%dT%H%M%S")
                            break
                        except ValueError:
                            continue
                    else:
                        raise ValueError(
                            f"Không đọc được thời gian từ tên file: {file_name}"
                        )
                except ValueError:
                    try:
                        created_at = datetime.fromtimestamp(
                            path_obj.stat().st_mtime
                        ).strftime("%Y%m%dT%H%M%S")
                    except OSError:
                        created_at = datetime.now().strftime("%Y%m%dT%H%M%S")

                key_record = f"{cccd}_{created_at}"
                s3_uri = f"{date_folder}/{key_record}.jpg"

                metadata_images.append(
                    {
                        "cccd": cccd,
                        "key_record": key_record,
                        "created_at": created_at,
                        "s3_uri": s3_uri,
                        "file_path": file_path,
                    }
                )
            except Exception as error:
                print(f"[ERROR] Không tạo được metadata cho {file_path}: {error}")

        print(f"[INFO] Đã tạo metadata cho {len(metadata_images)} file")
        return metadata_images

    @task
    def upload_to_s3(metadata_images: list[dict]) -> list[dict]:
        bucket = os.getenv("S3_BUCKET", "images")
        s3_hook = S3Hook(aws_conn_id="s3_upload_images_cccd")
        upload_results = []

        for metadata in metadata_images:
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
                print(f"[OK] Upload thành công: {file_path} -> {s3_uri}")
            except Exception as error:
                metadata["status"] = "failed"
                metadata["error"] = str(error)
                print(f"[ERROR] Upload thất bại {file_path}: {error}")

            upload_results.append(metadata)

        return upload_results

    @task
    def update_metadata_to_postgres(upload_results: list[dict]) -> None:
        records = [
            (
                item["key_record"],
                item["cccd"],
                item["created_at"],
                item["s3_uri"],
            )
            for item in upload_results
            if item.get("status") == "success"
        ]

        if not records:
            print("[INFO] Không có file upload thành công để insert PostgreSQL")
            return

        hook = PostgresHook(postgres_conn_id="postgres_cccd")
        connection = hook.get_conn()

        insert_query = """
            INSERT INTO images_metadata (key_record, cccd, created_at, s3_uri)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (key_record) DO NOTHING
        """

        try:
            from psycopg2.extras import execute_batch

            with connection.cursor() as cursor:
                execute_batch(cursor, insert_query, records, page_size=1000)
            connection.commit()
            print(
                f"[OK] Đã xử lý {len(records)} metadata; "
                "các key_record trùng đã được bỏ qua"
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    files = scan_previous_day_files(data_update_folder)
    metadata = init_metadata_img(files)
    upload_results = upload_to_s3(metadata)
    update_metadata_to_postgres(upload_results)


daily_update_image_to_storage()
