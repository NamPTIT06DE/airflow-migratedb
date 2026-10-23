from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.decorators import dag, task
from datetime import datetime
import os


def _load_env_file(filepath: str) -> dict:
    """Đọc file .env và trả về dict {KEY: VALUE}. Bỏ qua dòng trống và comment."""
    env = {}
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip()
    return env


# Đường dẫn tới file worker.env (cùng thư mục với DAG này)
_WORKER_ENV_PATH = os.path.join(os.path.dirname(__file__), "worker.env")


@dag(
    dag_id="trigger_worker_migrate",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
)
def trigger_worker_migrate():
    NUM_WORKERS = 3
    workers = []

    worker_env = _load_env_file(_WORKER_ENV_PATH)

    for i in range(NUM_WORKERS):
        worker_task = DockerOperator(
            task_id=f"trigger_worker_migrate_{i}",
            image="namnguyendevsonet/migrate-data:v2",
            command="--nums_pull_chunk 1",
            docker_url="tcp://host.docker.internal:2375",
            network_mode="host",
            auto_remove="force",
            do_xcom_push=True,
            xcom_all=False,  # Chỉ lấy dòng CUỐI CÙNG của stdout (JSON summary)
            environment=worker_env,
        )

        workers.append(worker_task)

    @task
    def monitor_worker_job(**context):
        """Pull XCom từ tất cả worker, log chi tiết và trả về JSON thống kê chunk."""
        import json
        from datetime import datetime, timezone

        ti = context["ti"]

        total_rows_success = 0
        total_chunks = 0
        total_chunks_success = 0
        all_chunks = []

        for i in range(NUM_WORKERS):
            xcom_log = ti.xcom_pull(task_ids=f"trigger_worker_migrate_{i}")

            if not xcom_log:
                print(
                    f"[WORKER {i}] Không có XCom (worker có thể đã lỗi hoặc không chạy)"
                )
                continue

            try:
                if isinstance(xcom_log, str):
                    # Tự động tìm đoạn JSON bắt đầu từ { đến } cuối cùng
                    start_idx = xcom_log.find("{")
                    end_idx = xcom_log.rfind("}")
                    if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                        json_str = xcom_log[start_idx : end_idx + 1]
                        xcom_log = json.loads(json_str)
                    else:
                        print(f"[WORKER {i}] Output không chứa JSON: {xcom_log}")
                        continue
            except Exception as e:
                print(f"[WORKER {i}] Lỗi parse XCom: {e}")
                continue

            details = xcom_log.get("details", [])

            print(f"\n[WORKER {i}] total_chunks={xcom_log.get('total_chunks', 0)}")
            for detail in details:
                rows = detail.get("total_rows_success", 0)
                status = detail.get("status", "UNKNOWN")
                total_rows_success += rows
                total_chunks += 1
                if status == "COMPLETE":
                    total_chunks_success += 1

                all_chunks.append({**detail, "worker_id": i})

        # --- Tổng kết log ---
        print(f"\n{'='*40}")
        print(f"=== TỔNG KẾT MIGRATE ===")
        print(f"{'='*40}")
        print(f"Tổng chunks        : {total_chunks}")
        print(f"Chunks thành công  : {total_chunks_success}")
        print(f"Tổng rows migrated : {total_rows_success}")
        print(f"{'='*40}")

        # --- JSON thống kê trả về XCom ---
        summary = {
            "run_at": datetime.now(timezone.utc).isoformat(),
            "number_worker": NUM_WORKERS,
            "total_chunks": total_chunks,
            "chunks_success": total_chunks_success,
            "total_rows_success": total_rows_success,
            "chunks": all_chunks,
        }

        return summary

    @task.short_circuit
    def check_trigger_comeback(**context):
        ti = context["ti"]
        summary = ti.xcom_pull(task_ids="monitor_worker_job")

        if not summary:
            print("Không tìm thấy summary từ monitor_worker_job, dừng.")
            return False

        total_chunks = summary.get("total_chunks", 0)
        chunks_success = summary.get("chunks_success", 0)

        if total_chunks == 0:
            print("Không có chunk nào được xử lý không trigger tiếp.")
            return False

        ratio_success = chunks_success / total_chunks
        print(
            f"Tỷ lệ thành công: {ratio_success:.2%} ({chunks_success}/{total_chunks})"
        )

        if ratio_success >= 0.8:
            print("Tỷ lệ thành công >= 80%. Kích hoạt trigger chạy tiếp DAG!")
            return True
        else:
            print("Tỷ lệ fail nhiều (< 80%), tạm thời không trigger lại.")
            return False

    trigger_comeback = TriggerDagRunOperator(
        task_id="trigger_next_migrate_run",
        trigger_dag_id="trigger_worker_migrate",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    monitor = monitor_worker_job()
    check = check_trigger_comeback()

    workers >> monitor >> check >> trigger_comeback


dag_instance = trigger_worker_migrate()
