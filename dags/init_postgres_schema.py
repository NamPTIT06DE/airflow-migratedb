from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


@dag(
    dag_id="init_postgres_schema",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgres", "setup"],
    doc_md="""
    ### DAG Khởi tạo bảng PostgreSQL
    Chạy 1 lần (Manual trigger) để khởi tạo các bảng và index cần thiết trên PostgreSQL:
    - `migration_chunks`: Quản lý tiến trình chunk migration
    - `images_metadata`: Lưu metadata ảnh sau khi tải lên S3/MinIO
    """,
)
def init_postgres_schema():

    @task
    def create_tables():
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS migration_chunks (
            chunk_id          SERIAL PRIMARY KEY,
            start_rowid       VARCHAR(20) NOT NULL,
            end_rowid         VARCHAR(20) NOT NULL,
            target_rows       INTEGER,             
            actual_rows       INTEGER DEFAULT 0,
            rows_uploaded     INTEGER DEFAULT 0,
            last_rowid_done   VARCHAR(20),
            status            VARCHAR(20) DEFAULT 'PENDING',
            worker_name       VARCHAR(100),    
            started_at        TIMESTAMP,
            completed_at      TIMESTAMP,
            error_message     TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_migration_status ON migration_chunks(status, chunk_id);

        CREATE TABLE IF NOT EXISTS images_metadata (
            id BIGSERIAL PRIMARY KEY,
            key_record VARCHAR(255) NOT NULL UNIQUE,
            cccd VARCHAR(50) NOT NULL,
            s3_uri TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_images_metadata_cccd ON images_metadata(cccd);
        """

        hook = PostgresHook(postgres_conn_id="postgres_admin")
        hook.run(create_tables_sql)
        print("Khoi tao schema PostgreSQL thanh cong!")

    create_tables()


init_postgres_schema()
