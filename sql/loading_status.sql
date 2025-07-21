CREATE TABLE IF NOT EXISTS loading_status (
    id            bigserial PRIMARY KEY,
    state_code    char(2),
    document_type varchar(10),
    version       varchar(20),
    file_hash     varchar(64),
    status        varchar(20),
    error_message text,
    row_count     integer,
    started_at    timestamptz DEFAULT now(),
    completed_at  timestamptz
);

CREATE INDEX IF NOT EXISTS idx_loading_status_recent
  ON loading_status (started_at DESC);
