from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import re
import base64
import requests
from pathlib import Path


args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def normalize_time(s: str):

    formats = [
        "%Y%m%d-%H%M%S",
        "%Y%m%dT%H%M%S",
        "%Y-%m-%d-%H%M%S",
        "%Y-%m-%dT%H%M%S",
        "%Y%m%d-%H-%M-%S",
        "%Y%m%dT%H-%M-%S",
        "%Y-%m-%d-%H-%M-%S",
        "%Y-%m-%dT%H-%M-%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).strftime("%d/%m/%YT%H:%M:%S")
        except ValueError:
            continue
    return None


@dag(
    dag_id="api_share_c06_native",
    default_args=args,
    schedule="59 23 * * *",  # 23:59 mỗi ngày
    catchup=False,
    tags=["api_share_c06"],
)
def api_share_c06_native():

    data_folder = os.getenv(
        "DATA_SHARE_HOST_FOLDER",
        "D:\\SONET\\MigrateDataImage\\data_share_c06",
    )
    api_endpoint = os.getenv(
        "API_SHARE_DATA_C06_URL",
        "http://localhost:5051/api/v1/images",
    )

    @task
    def scan_files(folder: str) -> list[str]:

        if not os.path.isdir(folder):
            print(f"[WARN] Thư mục không tồn tại: {folder}")
            return []

        files = [
            os.path.join(root, f)
            for root, _, filenames in os.walk(folder)
            for f in filenames
        ]
        return files

    @task
    def post_api_share_data(files: list[str], api_url: str):

        if not files:
            print("[INFO] Không có file nào để xử lý.")
            return

        headers = {
            "Content-Type": "application/json",
            "X-Access-Key": os.getenv("X-Access-Key", "Testa05"),
        }

        for file in files:
            path_obj = Path(file)
            stem = path_obj.stem
            parts = re.split(r"[_\-]", stem)
            cccd = parts[0]

            # Lấy created_at: ưu tiên parse từ tên file, fallback về mtime
            created_at = None
            if len(parts) > 1:
                created_at = normalize_time(parts[1])
                if created_at is None:
                    print(f"[WARN] Không parse được thời gian từ '{parts[1]}'")

            if created_at is None:
                try:
                    created_at = datetime.fromtimestamp(
                        path_obj.stat().st_mtime
                    ).strftime("%d/%m/%YT%H:%M:%S")
                except Exception:
                    created_at = datetime.now().strftime("%d/%m/%YT%H:%M:%S")

            try:
                with open(file, "rb") as f:
                    img_b64 = base64.b64encode(f.read()).decode("utf-8")
            except FileNotFoundError:
                print(f"[ERR] File không tồn tại: {file}")
                continue
            except Exception as e:
                print(f"[ERR] Không đọc được file {file}: {e}")
                continue

            mime = "image/png"

            payload = {
                "SoDinhDanh": cccd,
                "HinhAnh": f"data:{mime};base64,{img_b64}",
                "created_at": created_at,
            }
            # retry
            for number_retry in range(1, 3):
                try:
                    response = requests.post(
                        url=api_url,
                        headers=headers,
                        json=payload,
                        timeout=30,
                    )
                    print(f"  attempt={number_retry} status={response.status_code}")
                    if response.status_code < 500:
                        break

                except Exception as e:
                    print(f"  attempt={number_retry} error: {e}")

    file_list = scan_files(data_folder)
    post_api_share_data(file_list, api_endpoint)


api_share_c06_native()
