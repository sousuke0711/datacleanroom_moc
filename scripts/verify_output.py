#!/usr/bin/env python3
"""
匿名化済みデータを Pulsar から受信して表示する検証スクリプト。

実行方法:
    python scripts/verify_output.py
"""
import json

PULSAR_URL = "pulsar://localhost:6650"
TOPIC      = "persistent://public/default/anonymized-user-data"
SUB_NAME   = "verify-sub"

# 検証: これらのフィールドは変換されているはず
SHOULD_ANONYMIZE = {"name", "email", "phone", "user_token"}


def check(record: dict) -> list[str]:
    issues = []
    for field in SHOULD_ANONYMIZE:
        if field not in record:
            continue
        val = str(record[field])
        # 元の値の特徴が残っていないか簡易チェック
        if field == "email" and "@" in val and not val.startswith("*"):
            issues.append(f"  [!] email がマスキングされていません: {val}")
        if field == "user_token" and not val.startswith("tok_"):
            issues.append(f"  [!] user_token がトークン化されていません: {val}")
    return issues


def main() -> None:
    try:
        import pulsar
    except ImportError:
        raise SystemExit("pulsar-client が必要です: pip install pulsar-client")

    client   = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(
        TOPIC,
        subscription_name=SUB_NAME,
        consumer_type=pulsar.ConsumerType.Shared,
    )

    print(f"Consuming from {TOPIC}  (Ctrl+C で停止)\n")
    print(f"{'─' * 70}")

    try:
        while True:
            try:
                msg = consumer.receive(timeout_millis=5_000)
            except pulsar.Timeout:
                print("(5秒間メッセージなし。データ送信待ち ...)")
                continue

            data   = json.loads(msg.data().decode("utf-8"))
            issues = check(data)

            print(json.dumps(data, ensure_ascii=False, indent=2))
            for issue in issues:
                print(issue)
            print(f"{'─' * 70}")

            consumer.acknowledge(msg)

    except KeyboardInterrupt:
        print("\n停止しました。")
    finally:
        client.close()


if __name__ == "__main__":
    main()
