"""
匿名化ロジック: mask / tokenize / pseudonymize の3手法。

- mask:         構造を保ちながら文字を * に置換
- tokenize:     HMAC-SHA256 ベースの一貫したトークン (tok_{16hex})
- pseudonymize: HMACベースの決定論的仮名 (同じ入力→同じ仮名)
"""
import hashlib
import hmac
import os
import re
from typing import Any

_SECRET = os.getenv("ANONYMIZATION_SECRET", "change-me-in-production").encode()

_SURNAMES   = ["Yamada", "Suzuki", "Tanaka", "Watanabe", "Ito",
               "Nakamura", "Kobayashi", "Saito", "Kato", "Yoshida"]
_GIVENNAMES = ["Taro", "Hanako", "Ichiro", "Yuki", "Kenji",
               "Akira", "Ryo", "Mika", "Sota", "Hana"]
_DOMAINS    = ["example.com", "test.org", "sample.net", "mock.jp"]


class Anonymizer:
    def apply(self, value: Any, method: str) -> Any:
        if value is None:
            return None
        s = str(value)
        if method == "mask":
            return _mask(s)
        if method == "tokenize":
            return _tokenize(s)
        if method == "pseudonymize":
            return _pseudonymize(s)
        return _mask(s)


def _mask(value: str) -> str:
    if not value:
        return value
    # メールアドレス: ローカルパートをマスク、ドメインは保持
    if "@" in value:
        local, domain = value.split("@", 1)
        return f"{'*' * len(local)}@{domain}"
    # 日本の電話番号 (xxx-xxxx-xxxx)
    if re.match(r"^\d{2,4}-\d{4}-\d{4}$", value):
        parts = value.split("-")
        return f"{parts[0]}-****-{parts[2]}"
    return "*" * len(value)


def _tokenize(value: str) -> str:
    digest = hmac.new(_SECRET, value.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"tok_{digest[:16]}"


def _pseudonymize(value: str) -> str:
    h = int(hmac.new(_SECRET, value.encode("utf-8"), hashlib.sha256).hexdigest(), 16)
    # メールアドレス形式
    if re.match(r".+@.+\..+", value):
        user = f"user{h % 9999 + 1:04d}"
        domain = _DOMAINS[h % len(_DOMAINS)]
        return f"{user}@{domain}"
    # それ以外は姓名形式
    surname   = _SURNAMES[h % len(_SURNAMES)]
    givenname = _GIVENNAMES[(h >> 8) % len(_GIVENNAMES)]
    return f"{surname} {givenname}"
