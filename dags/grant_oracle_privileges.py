from airflow.decorators import dag, task
from airflow.providers.oracle.hooks.oracle import OracleHook

from datetime import datetime
import os


@dag(
    dag_id="grant_oracle_privileges",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["oracle", "setup"],
)
def grant_oracle_privileges():
    """
    DAG chạy một lần để GRANT các quyền cần thiết cho user Oracle.
    Kết nối qua Connection 'oracle_dba' (SYSTEM hoặc user có role DBA).
    """

    @task
    def run_grants():
        # User Oracle cần được cấp quyền (đọc từ env hoặc hardcode)
        target_user = os.getenv("ORACLE_USER", "IMGDATA").upper()
        table_owner = os.getenv("ORACLE_TABLE_OWNER", "IMGDATA").upper()
        table_name = os.getenv("ORACLE_TABLE_NAME", "IMAGES").upper()

        # Kết nối bằng account DBA (SYSTEM)
        conn = OracleHook(oracle_conn_id="oracle_dba").get_conn()
        cursor = conn.cursor()

        grants = [
            # Quyền dùng DBMS_PARALLEL_EXECUTE (bắt buộc cho init_chunks DAG)
            f"GRANT EXECUTE ON DBMS_PARALLEL_EXECUTE TO {target_user}",
            # Quyền tạo bảng tracking nội bộ của DBMS_PARALLEL_EXECUTE
            f"GRANT CREATE TABLE TO {target_user}",
            # Quyền đọc bảng gốc (nếu user kết nối khác schema chứa data)
            f"GRANT SELECT ON {table_owner}.{table_name} TO {target_user}",
        ]

        results = []
        for stmt in grants:
            try:
                cursor.execute(stmt)
                conn.commit()
                print(f"OK: {stmt}")
                results.append({"sql": stmt, "status": "SUCCESS"})
            except Exception as e:
                # Bỏ qua nếu đã được GRANT trước đó (ORA-01927)
                print(f"SKIP: {stmt} | Loi: {e}")
                results.append({"sql": stmt, "status": "SKIPPED", "error": str(e)})

        cursor.close()
        conn.close()

        print("\n=== KET QUA GRANT ===")
        for r in results:
            print(r)

        return results

    @task
    def verify_grants(results):
        """Kiểm tra lại quyền đã được cấp chưa."""
        target_user = os.getenv("ORACLE_USER", "IMGDATA").upper()

        conn = OracleHook(oracle_conn_id="oracle_dba").get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT GRANTEE, TABLE_NAME, PRIVILEGE
            FROM DBA_TAB_PRIVS
            WHERE GRANTEE = :grantee_name
              AND TABLE_NAME = 'DBMS_PARALLEL_EXECUTE'
            """,
            {"grantee_name": target_user},
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if rows:
            print(f"User {target_user} da co quyen EXECUTE ON DBMS_PARALLEL_EXECUTE")
        else:
            raise ValueError(
                f"User {target_user} CHUA co quyen EXECUTE ON DBMS_PARALLEL_EXECUTE. "
                "Kiem tra lai Connection 'oracle_dba' co du quyen DBA khong."
            )

        return rows

    results = run_grants()
    verify_grants(results)


grant_oracle_privileges()
