/* ========================================================================== */
/*  SST  TEMPORAL  DATABASE  –  FULL  DDL                                     */
/* ========================================================================== */

-- Extension needed for the EXCLUDE constraint
CREATE EXTENSION IF NOT EXISTS btree_gist;

-------------------------------------------------------------------------------
-- 1.  REFERENCE  TABLES
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS states (
    state_code      CHAR(2)     PRIMARY KEY,
    state_name      VARCHAR(100) NOT NULL,
    sst_member_since DATE,
    created_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS document_types (
    document_type_id SERIAL PRIMARY KEY,
    document_type    VARCHAR(10) UNIQUE NOT NULL
        CHECK (document_type IN ('LOD','CERT','TAP')),
    description      TEXT
);

INSERT INTO document_types (document_type, description) VALUES
    ('LOD',  'Library of Definitions')
  , ('CERT', 'Certificate of Compliance')
  , ('TAP',  'Tax Administration Practices')
ON CONFLICT (document_type) DO NOTHING;

-------------------------------------------------------------------------------
-- 2.  DOCUMENT  VERSIONS  (TEMPORAL  ANCHOR)
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS document_versions (
    document_version_id BIGSERIAL PRIMARY KEY,
    state_code          CHAR(2)  NOT NULL REFERENCES states,
    document_type_id    INT      NOT NULL REFERENCES document_types,
    version             TEXT     NOT NULL,                -- e.g. v2024.1
    effective_date      DATE     NOT NULL,
    valid_to            DATE,                              -- NULL = current
    metadata            JSONB    NOT NULL,
    loaded_at           TIMESTAMPTZ DEFAULT now(),
    loaded_by           TEXT,

    UNIQUE (state_code, document_type_id, version),

    -- prevent overlapping date ranges for a (state, doc_type)
    EXCLUDE USING gist (
        state_code        WITH =,
        document_type_id  WITH =,
        daterange(effective_date, COALESCE(valid_to,'infinity'), '[)') WITH &&
    )
);

CREATE INDEX IF NOT EXISTS idx_doc_versions_effective
    ON document_versions (state_code, document_type_id, effective_date);

CREATE INDEX IF NOT EXISTS idx_doc_versions_current
    ON document_versions (state_code, document_type_id)
    WHERE valid_to IS NULL;

-------------------------------------------------------------------------------
-- 3.  LIBRARY  OF  DEFINITIONS  ITEMS
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lod_items (
    lod_item_id         BIGSERIAL PRIMARY KEY,
    document_version_id BIGINT NOT NULL REFERENCES document_versions,
    item_type           VARCHAR(30) NOT NULL CHECK (
                           item_type IN ('admin_definition','product_definition','holiday_item')),
    code                VARCHAR(30) NOT NULL,
    group_name          TEXT,
    description         TEXT,
    taxable             BOOLEAN,
    exempt              BOOLEAN,
    included            BOOLEAN,
    excluded            BOOLEAN,
    threshold           NUMERIC(10,2),
    rate                NUMERIC(7,4),
    statute             TEXT,
    citation            TEXT,
    comment             TEXT,
    data                JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- denormalised for fast lookups
    state_code          CHAR(2) NOT NULL,
    effective_date      DATE    NOT NULL,

    UNIQUE (document_version_id, item_type, code)
);

CREATE INDEX IF NOT EXISTS idx_lod_items_lookup
    ON lod_items (state_code, code, effective_date);
CREATE INDEX IF NOT EXISTS idx_lod_items_type
    ON lod_items (item_type, code);
CREATE INDEX IF NOT EXISTS idx_lod_items_taxable
    ON lod_items (code)
    WHERE item_type = 'product_definition' AND taxable;
CREATE INDEX IF NOT EXISTS idx_lod_items_data_gin
    ON lod_items USING gin (data);

-------------------------------------------------------------------------------
-- 4.  CERTIFICATE  OF  COMPLIANCE  ITEMS
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cert_items (
    cert_item_id        BIGSERIAL PRIMARY KEY,
    document_version_id BIGINT NOT NULL REFERENCES document_versions,
    section             VARCHAR(20) NOT NULL,
    code                VARCHAR(20) NOT NULL,
    topic               TEXT,
    description         TEXT NOT NULL,
    compliance_met      BOOLEAN,
    citation            TEXT,
    effective_dates     TEXT,
    notes               TEXT,
    data                JSONB NOT NULL DEFAULT '{}'::jsonb,
    state_code          CHAR(2) NOT NULL,
    effective_date      DATE NOT NULL,

    UNIQUE (document_version_id, code)
);

CREATE INDEX IF NOT EXISTS idx_cert_items_lookup
    ON cert_items (state_code, code, effective_date);
CREATE INDEX IF NOT EXISTS idx_cert_items_section
    ON cert_items (state_code, section);
CREATE INDEX IF NOT EXISTS idx_cert_items_noncompliant
    ON cert_items (state_code, section)
    WHERE compliance_met = false;
CREATE INDEX IF NOT EXISTS idx_cert_items_data_gin
    ON cert_items USING gin (data);

-------------------------------------------------------------------------------
-- 5.  TAX  ADMINISTRATION  PRACTICES  ITEMS
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tap_items (
    tap_item_id         BIGSERIAL PRIMARY KEY,
    document_version_id BIGINT NOT NULL REFERENCES document_versions,
    group_name          TEXT NOT NULL,
    subgroup            TEXT,
    code                VARCHAR(20) NOT NULL,
    description         TEXT NOT NULL,
    compliance_met      BOOLEAN,
    citation            TEXT,
    comment             TEXT,
    data                JSONB NOT NULL DEFAULT '{}'::jsonb,
    state_code          CHAR(2) NOT NULL,
    effective_date      DATE NOT NULL,

    UNIQUE (document_version_id, code)
);

CREATE INDEX IF NOT EXISTS idx_tap_items_lookup
    ON tap_items (state_code, code, effective_date);
CREATE INDEX IF NOT EXISTS idx_tap_items_group
    ON tap_items (state_code, group_name);
CREATE INDEX IF NOT EXISTS idx_tap_items_noncompliant
    ON tap_items (state_code, compliance_met)
    WHERE compliance_met = false;
CREATE INDEX IF NOT EXISTS idx_tap_items_data_gin
    ON tap_items USING gin (data);

-------------------------------------------------------------------------------
-- 6.  CURRENT  VIEWS
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW current_document_versions AS
SELECT * FROM document_versions
WHERE valid_to IS NULL;

CREATE OR REPLACE VIEW current_lod_items AS
SELECT l.*, dv.version, dv.metadata
FROM   lod_items l
JOIN   current_document_versions dv USING (document_version_id);

CREATE OR REPLACE VIEW current_cert_items AS
SELECT c.*, dv.version, dv.metadata
FROM   cert_items c
JOIN   current_document_versions dv USING (document_version_id);

CREATE OR REPLACE VIEW current_tap_items AS
SELECT t.*, dv.version, dv.metadata
FROM   tap_items t
JOIN   current_document_versions dv USING (document_version_id);

-- Compliance summary (CERT + TAP)
CREATE OR REPLACE VIEW current_compliance_summary AS
SELECT state_code,
       'CERT' AS doc_type,
       COUNT(*)                          AS total,
       COUNT(*) FILTER (WHERE compliance_met)         AS compliant,
       COUNT(*) FILTER (WHERE NOT compliance_met)     AS non_compliant,
       COUNT(*) FILTER (WHERE compliance_met IS NULL) AS not_applicable
FROM current_cert_items
GROUP BY state_code

UNION ALL

SELECT state_code,
       'TAP' AS doc_type,
       COUNT(*),
       COUNT(*) FILTER (WHERE compliance_met),
       COUNT(*) FILTER (WHERE NOT compliance_met),
       COUNT(*) FILTER (WHERE compliance_met IS NULL)
FROM current_tap_items
GROUP BY state_code;

-- Current taxability matrix (product‑level)
CREATE OR REPLACE VIEW current_taxability_matrix AS
SELECT state_code, code, description, group_name,
       taxable, exempt, statute,
       version, effective_date
FROM current_lod_items
WHERE item_type = 'product_definition'
ORDER BY state_code, code;

-------------------------------------------------------------------------------
-- 7.  LOADING‑STATUS  TABLE  (ETL  progress / retries)
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS loading_status (
    id            BIGSERIAL PRIMARY KEY,
    state_code    CHAR(2),
    document_type VARCHAR(10),
    version       VARCHAR(20),
    file_hash     VARCHAR(64),
    status        VARCHAR(20),   -- started / completed / failed
    error_message TEXT,
    row_count     INTEGER,
    started_at    TIMESTAMPTZ DEFAULT now(),
    completed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_loading_status_recent
  ON loading_status (started_at DESC);

-------------------------------------------------------------------------------
-- 8.  HELPER  FUNCTIONS
-------------------------------------------------------------------------------
-- Point‑in‑time product taxability lookup
CREATE OR REPLACE FUNCTION get_product_taxability(
    p_state_code  CHAR(2),
    p_product_code VARCHAR(20),
    p_as_of_date  DATE DEFAULT CURRENT_DATE
) RETURNS TABLE (
    version        TEXT,
    code           TEXT,
    description    TEXT,
    taxable        BOOLEAN,
    exempt         BOOLEAN,
    statute        TEXT,
    comment        TEXT,
    effective_date DATE
) AS $$
BEGIN
    RETURN QUERY
    SELECT dv.version, l.code, l.description,
           l.taxable, l.exempt,
           l.statute, l.comment,
           dv.effective_date
    FROM lod_items l
    JOIN document_versions dv USING (document_version_id)
    WHERE l.state_code = p_state_code
      AND l.code = p_product_code
      AND l.item_type = 'product_definition'
      AND dv.effective_date <= p_as_of_date
      AND (dv.valid_to IS NULL OR dv.valid_to >= p_as_of_date);
END;
$$ LANGUAGE plpgsql;

-- Track compliance changes across versions
CREATE OR REPLACE FUNCTION get_compliance_changes(
    p_state_code CHAR(2),
    p_doc_type   VARCHAR(10),
    p_from_date  DATE,
    p_to_date    DATE DEFAULT CURRENT_DATE
) RETURNS TABLE (
    version         TEXT,
    code            TEXT,
    description     TEXT,
    old_compliance  BOOLEAN,
    new_compliance  BOOLEAN,
    changed_date    DATE
) AS $$
BEGIN
    RETURN QUERY
    WITH hist AS (
        SELECT dv.version,
               dv.effective_date,
               COALESCE(c.code, t.code)              AS code,
               COALESCE(c.description, t.description) AS description,
               COALESCE(c.compliance_met, t.compliance_met) AS compliance
        FROM document_versions dv
        LEFT JOIN cert_items c ON dv.document_version_id = c.document_version_id
        LEFT JOIN tap_items  t ON dv.document_version_id = t.document_version_id
        WHERE dv.state_code = p_state_code
          AND dv.document_type_id = (
              SELECT document_type_id
              FROM document_types
              WHERE document_type = p_doc_type)
          AND dv.effective_date BETWEEN p_from_date AND p_to_date
    )
    SELECT h.version,
           h.code,
           h.description,
           LAG(h.compliance)
               OVER (PARTITION BY h.code ORDER BY h.effective_date) AS old_compliance,
           h.compliance                                           AS new_compliance,
           h.effective_date                                       AS changed_date
    FROM hist h
    WHERE h.compliance IS DISTINCT FROM
          LAG(h.compliance) OVER (PARTITION BY h.code ORDER BY h.effective_date);
END;
$$ LANGUAGE plpgsql;

/* ========================================================================== */
/*  END  OF  FILE                                                             */
/* ========================================================================== */
