CREATE TABLE migration_chunks (
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
CREATE INDEX idx_migration_status ON migration_chunks(status, chunk_id);

CREATE TABLE images_metadata (
    id BIGSERIAL PRIMARY KEY,
    key_record VARCHAR(255) NOT NULL UNIQUE,
    cccd VARCHAR(50) NOT NULL,
    s3_uri TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);


CREATE INDEX idx_images_metadata_cccd ON images_metadata(cccd);


