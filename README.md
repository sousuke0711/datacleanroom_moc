# datacleanroom MoC — メタデータ駆動 仮名化/匿名化パイプライン

OpenMetadata に登録された PII タグを参照し、Apache Flink でストリームデータをリアルタイムに仮名化・匿名化するパイプラインの MoC (Mock of Concept) です。

## アーキテクチャ概要

```
生データ (JSON)
    │
    ▼
Apache Pulsar ─── raw-user-data トピック
    │
    ▼
Apache Flink ──── AnonymizationFunction
    │  ↑
    │  └─ 起動時に OpenMetadata REST API からPIIタグ取得
    │       PII.Mask / PII.Tokenize / PII.Pseudonymize
    │
    ├──▶ Pulsar ─── anonymized-user-data トピック
    │
    └──▶ Iceberg Table (Nessie カタログ / MinIO ストレージ)
```

詳細は [ARCHITECTURE.md](./ARCHITECTURE.md) を参照してください。

## 技術スタック

| コンポーネント | バージョン | 役割 | 実行環境 |
|---|---|---|---|
| Apache Flink | 1.17.2 | ストリーム処理エンジン | Docker（カスタムビルド） |
| Apache Pulsar | 3.1.0 | メッセージブローカー | Docker |
| OpenMetadata | 1.3.0 | メタデータ管理・PIIタグ付け | Docker |
| Apache Iceberg | 1.4.2 | オープンテーブルフォーマット | Docker（Flink内ライブラリ） |
| Project Nessie | 0.74.0 | Iceberg REST カタログ | Docker |
| MinIO | latest | S3互換オブジェクトストレージ | Docker |

## コンポーネント実行環境

### Docker管理（`docker compose up -d` で一括起動）

| サービス名 | イメージ | 備考 |
|---|---|---|
| `flink-jobmanager` | `flink-job/Dockerfile`（カスタムビルド） | PyFlink + コネクタJAR 同梱 |
| `flink-taskmanager` | 同上 | JobManager と同一イメージ |
| `pulsar` | `apachepulsar/pulsar:3.1.0` | スタンドアロンモード |
| `openmetadata` | `openmetadata/server:1.3.0` | 認証なし（MoC用） |
| `om_mysql` | `mysql:8.0` | OpenMetadata のメタデータDB |
| `om_elasticsearch` | `elasticsearch:8.10.2` | OpenMetadata の検索バックエンド |
| `minio` | `minio/minio:latest` | Iceberg データ / Flink チェックポイント用 |
| `minio_init` | `minio/mc:latest` | 初回バケット作成のみ（使い捨てコンテナ） |
| `nessie` | `ghcr.io/projectnessie/nessie:0.74.0` | Iceberg REST カタログ |

### ローカル実行（Python / bash スクリプト）

| スクリプト | 実行タイミング | 説明 |
|---|---|---|
| `scripts/setup_openmetadata.py` | サービス起動後（1回） | PIIタグ・スキーマの初期登録 |
| `scripts/submit_job.sh` | セットアップ後 | Flink ジョブの投入 |
| `scripts/produce_sample_data.py` | ジョブ投入後 | サンプルデータの継続送信 |
| `scripts/verify_output.py` | 任意のタイミング | 匿名化済みデータの確認 |

## 匿名化手法

| OpenMetadata タグ | 手法 | 例 |
|---|---|---|
| `PII.Pseudonymize` | 仮名化（決定論的） | `田中太郎` → `Suzuki Ichiro` |
| `PII.Mask` | マスキング | `user@gmail.com` → `****@gmail.com` |
| `PII.Tokenize` | トークン化（HMAC-SHA256） | `abc123...` → `tok_a3f9b2c1d4e5` |

タグを変更すると **TTL 60秒** のキャッシュ失効後に自動反映されます。

## 前提条件

- Docker Desktop
- Python 3.9+
- `pip install requests pulsar-client`

## クイックスタート

### 1. サービス起動　`[Docker]`

```bash
docker compose up -d
```

初回は Flink イメージのビルドと JAR ダウンロードに数分かかります。

### 2. OpenMetadata にスキーマ・PIIタグを登録　`[ローカル実行]`

OpenMetadata の起動完了（約3分）を待ってから実行します。

```bash
python scripts/setup_openmetadata.py
```

以下が登録されます:
- PII 分類 (`PII.Mask` / `PII.Tokenize` / `PII.Pseudonymize`)
- テーブル `sample_data.default.default.users` とカラムのPIIタグ

### 3. Flink ジョブを投入　`[ローカル実行]`

```bash
bash scripts/submit_job.sh
```

### 4. サンプルデータを送信　`[ローカル実行]`

```bash
python scripts/produce_sample_data.py
```

2秒ごとにランダムなユーザーデータを `raw-user-data` トピックに送信します。

### 5. 匿名化済みデータを確認　`[ローカル実行]`

```bash
python scripts/verify_output.py
```

### 6. (オプション) Iceberg に永続化　`[Docker内 Flink コンテナで実行]`

```bash
docker compose exec flink-jobmanager \
  /opt/flink/bin/sql-client.sh -f /opt/flink/job/iceberg_sink.sql
```

## ディレクトリ構成

```
mock-anonymization/
├── docker-compose.yml              # 全サービス定義
├── ARCHITECTURE.md                 # アーキテクチャ図
├── flink-job/
│   ├── Dockerfile                  # PyFlink + コネクタJAR
│   ├── requirements.txt
│   ├── anonymization_job.py        # メインジョブ (Pulsar→Flink→Pulsar)
│   ├── openmetadata_client.py      # OMタグ取得クライアント
│   ├── anonymizer.py               # mask / tokenize / pseudonymize
│   └── iceberg_sink.sql            # Flink SQL: 匿名化済みデータ→Iceberg
└── scripts/
    ├── setup_openmetadata.py       # PIIタグ・スキーマ登録
    ├── produce_sample_data.py      # サンプルデータ送信
    ├── verify_output.py            # 匿名化結果の確認
    └── submit_job.sh               # Flinkジョブ投入
```

## 管理画面

| サービス | URL | 用途 |
|---|---|---|
| OpenMetadata UI | http://localhost:8585 | PIIタグ・スキーマ管理 |
| Flink Web UI | http://localhost:8081 | ジョブ監視・チェックポイント確認 |
| Pulsar Admin UI | http://localhost:8080 | トピック・サブスクリプション確認 |
| MinIO Console | http://localhost:9001 | Iceberg データファイル確認 |
| Nessie API | http://localhost:19120 | Iceberg カタログ REST API |

MinIO の認証情報: `minioadmin` / `minioadmin`

## 環境変数

Flink ジョブの動作は `docker-compose.yml` の `flink-jobmanager` サービスの環境変数で制御できます。

| 変数名 | デフォルト値 | 説明 |
|---|---|---|
| `TABLE_FQN` | `sample_data.default.default.users` | OpenMetadata のテーブル完全修飾名 |
| `INPUT_TOPIC` | `persistent://public/default/raw-user-data` | 入力 Pulsar トピック |
| `OUTPUT_TOPIC` | `persistent://public/default/anonymized-user-data` | 出力 Pulsar トピック |
| `ANONYMIZATION_SECRET` | `change-me-in-production` | トークン化・仮名化のHMACシークレット |

## トラブルシューティング

**OpenMetadata が起動しない**
```bash
docker compose logs openmetadata --tail=50
```
MySQL / Elasticsearch の起動完了を待ってリトライしてください。

**Flink ジョブが失敗する**
```bash
docker compose logs flink-jobmanager --tail=50
```
Flink Web UI (http://localhost:8081) の Exceptions タブでエラー詳細を確認してください。

**全サービスをリセットする**
```bash
docker compose down -v
docker compose up -d
```

## ライセンス

MIT
