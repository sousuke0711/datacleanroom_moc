-- ============================================================
-- Flink SQL: 匿名化済みPulsarトピック → Iceberg テーブル
--
-- 実行方法:
--   docker compose exec flink-jobmanager \
--     /opt/flink/bin/sql-client.sh -f /opt/flink/job/iceberg_sink.sql
-- ============================================================

-- ── Iceberg カタログ (Nessie REST + MinIO) ─────────────────────
CREATE CATALOG IF NOT EXISTS nessie_catalog WITH (
    'type'                  = 'iceberg',
    'catalog-type'          = 'rest',
    'uri'                   = 'http://nessie:19120/iceberg',
    'warehouse'             = 's3a://iceberg-warehouse/',
    'io-impl'               = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'           = 'http://minio:9000',
    's3.access-key-id'      = 'minioadmin',
    's3.secret-access-key'  = 'minioadmin',
    's3.path-style-access'  = 'true'
);

CREATE DATABASE IF NOT EXISTS nessie_catalog.anonymized;

-- ── Iceberg シンクテーブル ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS nessie_catalog.anonymized.users (
    user_id    INT,
    name       STRING  COMMENT 'pseudonymized',
    email      STRING  COMMENT 'masked',
    phone      STRING  COMMENT 'masked',
    user_token STRING  COMMENT 'tokenized',
    age        INT,
    region     STRING
) WITH (
    'write.format.default' = 'parquet',
    'write.upsert.enabled' = 'false'
);

-- ── Pulsar ソーステーブル (匿名化済みトピック) ──────────────────
CREATE TEMPORARY TABLE pulsar_anon_source (
    user_id    INT,
    name       STRING,
    email      STRING,
    phone      STRING,
    user_token STRING,
    age        INT,
    region     STRING
) WITH (
    'connector'                          = 'pulsar',
    'service-url'                        = 'pulsar://pulsar:6650',
    'admin-url'                          = 'http://pulsar:8080',
    'topics'                             = 'persistent://public/default/anonymized-user-data',
    'format'                             = 'json',
    'pulsar.source.subscriptionType'     = 'Shared',
    'pulsar.source.subscriptionName'     = 'flink-iceberg-sub',
    'scan.startup.mode'                  = 'latest'
);

-- ── 匿名化済みデータを Iceberg に書き込む ────────────────────────
INSERT INTO nessie_catalog.anonymized.users
SELECT user_id, name, email, phone, user_token, age, region
FROM   pulsar_anon_source;
