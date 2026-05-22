#!/usr/bin/env python3
"""
OpenMetadata に以下を登録するセットアップスクリプト:

1. PII 分類 + タグ (PII.Mask / PII.Tokenize / PII.Pseudonymize)
2. DatabaseService → Database → Schema → Table
3. カラムごとに適切なPIIタグを設定

実行方法:
    python scripts/setup_openmetadata.py

前提: OpenMetadata が http://localhost:8585 で起動済みであること。
"""
import sys
import time

import requests

BASE = "http://localhost:8585/api/v1"
SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json"})


# ── ユーティリティ ──────────────────────────────────────────────

def wait_for_openmetadata(retries: int = 24, delay: int = 10) -> None:
    for i in range(retries):
        try:
            r = SESSION.get(f"{BASE}/system/status", timeout=5)
            if r.status_code == 200:
                print("[OK] OpenMetadata is ready.")
                return
        except requests.RequestException:
            pass
        print(f"[..] Waiting for OpenMetadata ({i + 1}/{retries}) ...")
        time.sleep(delay)
    sys.exit("[ERR] OpenMetadata did not become ready in time.")


def upsert(path: str, payload: dict) -> dict:
    r = SESSION.put(f"{BASE}/{path}", json=payload)
    if r.status_code not in (200, 201, 409):
        print(f"[WARN] {path}: HTTP {r.status_code} – {r.text[:200]}")
    return r.json() if r.content else {}


def post(path: str, payload: dict) -> dict:
    r = SESSION.post(f"{BASE}/{path}", json=payload)
    if r.status_code not in (200, 201, 409):
        print(f"[WARN] {path}: HTTP {r.status_code} – {r.text[:200]}")
    return r.json() if r.content else {}


# ── PII 分類・タグ ──────────────────────────────────────────────

def setup_pii_tags() -> None:
    print("[..] Creating PII classification and tags ...")

    # 分類 (Classification)
    post("classifications", {
        "name":        "PII",
        "description": "個人情報 (Personally Identifiable Information) の分類",
    })

    # 各タグ
    for tag_name, desc in [
        ("Sensitive",      "PII全般 (デフォルト: マスキング)"),
        ("Mask",           "マスキング: 値を * で置換"),
        ("Tokenize",       "トークン化: HMAC-SHA256 トークンに置換 (一貫性あり)"),
        ("Pseudonymize",   "仮名化: 決定論的仮名に置換"),
    ]:
        post("tags", {
            "name":           tag_name,
            "classification": {"name": "PII"},
            "description":    desc,
        })

    print("[OK] PII tags created.")


def _tag_label(fqn: str) -> dict:
    return {
        "tagFQN":    fqn,
        "source":    "Classification",
        "labelType": "Manual",
        "state":     "Confirmed",
    }


# ── サービス・DB・スキーマ・テーブル ────────────────────────────

def create_database_service() -> str:
    print("[..] Creating DatabaseService 'sample_data' ...")
    upsert("services/databaseServices", {
        "name":        "sample_data",
        "serviceType": "CustomDatabase",
        "connection": {
            "config": {
                "type":              "CustomDatabase",
                "sourcePythonClass": "sample_data",
            }
        },
    })
    return "sample_data"


def create_database(service_fqn: str) -> str:
    print("[..] Creating Database ...")
    upsert("databases", {
        "name":    "default",
        "service": {"fullyQualifiedName": service_fqn},
    })
    return f"{service_fqn}.default"


def create_schema(database_fqn: str) -> str:
    print("[..] Creating DatabaseSchema ...")
    upsert("databaseSchemas", {
        "name":     "default",
        "database": {"fullyQualifiedName": database_fqn},
    })
    return f"{database_fqn}.default"


def create_table(schema_fqn: str) -> str:
    print("[..] Creating Table 'users' with PII tags ...")

    columns = [
        {
            "name":     "user_id",
            "dataType": "INT",
            "description": "ユーザーID (非PII)",
        },
        {
            "name":        "name",
            "dataType":    "VARCHAR",
            "dataLength":  255,
            "description": "氏名 → 仮名化",
            "tags":        [_tag_label("PII.Pseudonymize")],
        },
        {
            "name":        "email",
            "dataType":    "VARCHAR",
            "dataLength":  255,
            "description": "メールアドレス → マスキング",
            "tags":        [_tag_label("PII.Mask")],
        },
        {
            "name":        "phone",
            "dataType":    "VARCHAR",
            "dataLength":  20,
            "description": "電話番号 → マスキング",
            "tags":        [_tag_label("PII.Mask")],
        },
        {
            "name":        "user_token",
            "dataType":    "VARCHAR",
            "dataLength":  64,
            "description": "認証トークン → トークン化",
            "tags":        [_tag_label("PII.Tokenize")],
        },
        {
            "name":     "age",
            "dataType": "INT",
            "description": "年齢 (非PII)",
        },
        {
            "name":        "region",
            "dataType":    "VARCHAR",
            "dataLength":  50,
            "description": "地域 (非PII)",
        },
    ]

    r = SESSION.put(f"{BASE}/tables", json={
        "name":           "users",
        "databaseSchema": {"fullyQualifiedName": schema_fqn},
        "columns":        columns,
    })

    if r.status_code not in (200, 201, 409):
        print(f"[WARN] Table creation: HTTP {r.status_code} – {r.text[:300]}")
        return f"{schema_fqn}.users"

    fqn = r.json().get("fullyQualifiedName", f"{schema_fqn}.users")
    print(f"[OK] Table created: {fqn}")
    return fqn


# ── メイン ───────────────────────────────────────────────────────

def main() -> None:
    wait_for_openmetadata()
    setup_pii_tags()

    svc_fqn    = create_database_service()
    db_fqn     = create_database(svc_fqn)
    schema_fqn = create_schema(db_fqn)
    table_fqn  = create_table(schema_fqn)

    print()
    print("=" * 55)
    print("Setup complete!")
    print(f"  OpenMetadata UI : http://localhost:8585")
    print(f"  Table FQN       : {table_fqn}")
    print()
    print("Flink ジョブ起動時の環境変数:")
    print(f"  TABLE_FQN={table_fqn}")
    print("=" * 55)


if __name__ == "__main__":
    main()
