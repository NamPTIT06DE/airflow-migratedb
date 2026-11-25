from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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
    schedule=None,
    catchup=False,
    tags=["image"],
)
def update_image_to_storage():

    # Thư mục đã được Docker Compose mount vào Airflow scheduler.
    container_folder = os.getenv(
        "DATA_UPDATE_CONTAINER_FOLDER",
        "/opt/airflow/data_update",
    )

    @task
    def parse_scanned_files(folder: str):
        """Quét trực tiếp thư mục ảnh đã được mount vào Airflow scheduler."""
        if not os.path.isdir(folder):
            print(f"[WARN] Thư mục không tồn tại hoặc chưa được mount: {folder}")
            return []

        files = sorted(
            os.path.join(root, filename)
            for root, _, filenames in os.walk(folder)
            for filename in filenames
        )
        print(f"[INFO] Quét được {len(files)} file:")
        for f in files:
            print(f"  - {f}")
        return files

    @task
    def init_metadata_img(files_path):

        folder_root = os.getenv("S3_FOLDER_ROOT", "image_root")
        bucket = os.getenv("S3_BUCKET", "images")

        s3Uris = []

        for file_path in files_path:
            try:
                path_obj = Path(file_path)
                file_name = path_obj.name
                # Lấy tên thư mục cha trực tiếp chứa ảnh.
                # Ví dụ: /opt/airflow/data_update/20261123/001001708018.png
                # thì date_folder = "20261123".
                date_folder = path_obj.parent.name
                stem = (
                    path_obj.stem
                )  # Ví dụ: '010212345678' hoặc '010212345678_20260910'

                parts = re.split(r"[_\-]", stem)
                cccd = parts[0]

                # Ưu tiên lấy thời gian nằm sau CCCD trong tên file.
                # Ví dụ: 010212345678_20260910T162530.jpg
                time_text = stem[len(cccd) :].lstrip("_-")
                time_formats = [
                    "%Y%m%d%H%M%S",      # 20260910162530
                    "%Y%m%dT%H%M%S",     # 20260910T162530
                    "%Y%m%d_%H%M%S",     # 20260910_162530
                ]

                try:
                    for time_format in time_formats:
                        try:
                            created_at = datetime.strptime(
                                time_text, time_format
                            ).strftime("%Y%m%dT%H%M%S")
                            break
                        except ValueError:
                            continue
                    else:
                        raise ValueError(
                            f"Không đọc được thời gian từ tên file: {file_name}"
                        )
                except ValueError:
                    # Tên file không có thời gian hợp lệ: dùng mtime của file vật lý.
                    try:
                        created_at = datetime.fromtimestamp(
                            path_obj.stat().st_mtime
                        ).strftime("%Y%m%dT%H%M%S")
                    except OSError:
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

        hook = PostgresHook(postgres_conn_id="postgres_cccd")
        conn = hook.get_conn()
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO images_metadata (key_record, cccd, created_at, s3_uri)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (key_record) DO NOTHING
        """

        try:
            from psycopg2.extras import execute_batch

            execute_batch(cursor, insert_query, records, page_size=1000)
            conn.commit()
            print(
                f"Đã xử lý {len(records)} bản ghi; "
                "các key_record trùng đã được bỏ qua"
            )
        except Exception as e:
            conn.rollback()
            print(f"Lỗi khi insert metadata vào Postgres: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    files_path = parse_scanned_files(container_folder)
    metadata_img = init_metadata_img(files_path)
    satus_upload_s3 = upload_to_minio(metadata_img)
    update_metadata_to_db(satus_upload_s3)


update_image_to_storage()
