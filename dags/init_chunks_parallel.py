from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.hooks.oracle import OracleHook

from datetime import datetime
import os


def _to_base64(n: int, length: int) -> str:
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    result = []
    for _ in range(length):
        result.append(chars[n % 64])
        n //= 64
    return "".join(reversed(result))


def _make_rowid(data_obj_id: int, rfile: int, block: int, row: int) -> str:
    return (
        _to_base64(data_obj_id, 6)
        + _to_base64(rfile, 3)
        + _to_base64(block, 6)
        + _to_base64(row, 3)
    )


@dag(
    dag_id="init_chunks",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["init_chunks"],
)
def init_chunks():
    @task
    def generate_checkpoints_insert_to_db():

        owner = os.getenv("ORACLE_TABLE_OWNER")
        table = os.getenv("ORACLE_TABLE_NAME")
        target_rows = int(os.getenv("TARGET_ROWS_PER_CHUNK", "5000"))

        oracle_conn = OracleHook(oracle_conn_id="oracle_user").get_conn()
        oracle_cursor = oracle_conn.cursor()  # Sửa lỗi: Chưa khởi tạo oracle_cursor

        # 1. Tạo một tác vụ chunking trong Oracle
        oracle_cursor.execute(
            "BEGIN dbms_parallel_execute.create_task(task_name => 'MY_CHUNK_TASK_3'); END;"
        )

        # 2. sinh chunk theo row, mỗi chunk ~target_rows dòng
        oracle_cursor.execute(
            """
                    BEGIN 
                        dbms_parallel_execute.create_chunks_by_rowid(
                            task_name   => 'MY_CHUNK_TASK_3',
                            table_owner => :owner,
                            table_name  => :tbl_name,
                            by_row      => TRUE,
                            chunk_size  => :chunk_size
                        );
                    END;
                """,
            {"owner": owner, "tbl_name": table, "chunk_size": target_rows},
        )

        # lay danh sách chunk
        oracle_cursor.execute(
            """
             SELECT chunk_id, start_rowid, end_rowid 
                    FROM user_parallel_execute_chunks 
                    WHERE task_name = 'MY_CHUNK_TASK_3'
                    ORDER BY chunk_id
        """,
        )

        row_fetch_oracle = oracle_cursor.fetchall()

        if not row_fetch_oracle:
            raise ValueError(f"khong tim thay bang {owner}.{table}")

        chunks = [
            (start_rowid, end_rowid, target_rows)
            for _, start_rowid, end_rowid in row_fetch_oracle
        ]
        return chunks

    @task
    def insert_info_chunk_to_psql(chunks):
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        pg_cursor = pg_hook.get_cursor()
        schema = os.getenv("POSTGRES_SCHEMA", "public")

        insert_query = f"""
            INSERT INTO {schema}.migration_chunks 
                (start_rowid, end_rowid, target_rows, status)
            VALUES (%s, %s, %s, 'PENDING')
        """

        try:
            from psycopg2.extras import execute_batch

            execute_batch(pg_cursor, insert_query, chunks, page_size=100)
            pg_cursor.connection.commit()
            print(f"Đã insert {len(chunks)} chunk vào bảng processing_chunks")
        except Exception as e:
            pg_cursor.connection.rollback()
            print(f"Lỗi khi insert chunk vào Postgres: {e}")
            raise
        finally:
            pg_cursor.close()

    chunks = generate_checkpoints_insert_to_db()
    insert_info_chunk_to_psql(chunks)


init_chunks()
