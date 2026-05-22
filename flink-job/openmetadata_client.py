"""
OpenMetadata REST API クライアント。
テーブルのカラム定義とPIIタグを取得し、
{カラム名: 匿名化メソッド} の辞書を返す。

タグと匿名化手法のマッピング:
  PII.Pseudonymize → "pseudonymize"
  PII.Tokenize     → "tokenize"
  PII.Mask         → "mask"
  PII.Sensitive    → "mask" (デフォルト)
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)

TAG_METHOD_MAP: Dict[str, str] = {
    "PII.Pseudonymize": "pseudonymize",
    "PII.Tokenize":     "tokenize",
    "PII.Mask":         "mask",
    "PII.Sensitive":    "mask",
}

CACHE_TTL_SECONDS = 60


class OpenMetadataClient:
    def __init__(self, base_url: str, jwt_token: Optional[str] = None):
        self._base = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})
        if jwt_token:
            self._session.headers["Authorization"] = f"Bearer {jwt_token}"
        self._cache: Dict[str, tuple] = {}

    def get_anonymization_rules(self, table_fqn: str) -> Dict[str, str]:
        """カラム名 → 匿名化メソッド のルール辞書を返す (キャッシュ付き)。"""
        cached = self._cache.get(table_fqn)
        if cached:
            rules, expires_at = cached
            if datetime.utcnow() < expires_at:
                return rules

        rules = self._fetch_rules(table_fqn)
        expires = datetime.utcnow() + timedelta(seconds=CACHE_TTL_SECONDS)
        self._cache[table_fqn] = (rules, expires)
        return rules

    def _fetch_rules(self, table_fqn: str) -> Dict[str, str]:
        url = f"{self._base}/v1/tables/name/{table_fqn}"
        try:
            resp = self._session.get(url, params={"fields": "columns"}, timeout=5)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error("OpenMetadata API error [%s]: %s", table_fqn, exc)
            return {}

        rules: Dict[str, str] = {}
        for col in resp.json().get("columns", []):
            for tag in col.get("tags", []):
                method = TAG_METHOD_MAP.get(tag.get("tagFQN", ""))
                if method:
                    rules[col["name"]] = method
                    logger.debug("col=%s tag=%s method=%s", col["name"], tag["tagFQN"], method)
                    break

        return rules
