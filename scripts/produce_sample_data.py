#!/usr/bin/env python3
"""
サンプルユーザーデータを Pulsar の raw-user-data トピックに送信する。

依存:
    pip install pulsar-client

実行方法:
    python scripts/produce_sample_data.py
    python scripts/produce_sample_data.py --count 50 --interval 0.5
"""
import argparse
import json
import random
import time

PULSAR_URL = "pulsar://localhost:6650"
TOPIC      = "persistent://public/default/raw-user-data"

NAMES   = ["田中太郎", "鈴木花子", "佐藤一郎", "渡辺美咲", "伊藤健二",
           "山田恵子", "中村智也", "小林菜々子", "加藤亮", "吉田美穂"]
DOMAINS = ["gmail.com", "yahoo.co.jp", "example.com", "docomo.ne.jp"]
REGIONS = ["東京", "大阪", "名古屋", "福岡", "札幌", "横浜", "京都", "神戸"]


def random_token(length: int = 32) -> str:
    return "".join(random.choices("abcdef0123456789", k=length))


def make_record(user_id: int) -> dict:
    name   = random.choice(NAMES)
    domain = random.choice(DOMAINS)
    prefix = f"user{user_id:04d}"
    return {
        "user_id":    user_id,
        "name":       name,
        "email":      f"{prefix}@{domain}",
        "phone":      f"0{random.randint(70,90)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}",
        "user_token": random_token(),
        "age":        random.randint(20, 70),
        "region":     random.choice(REGIONS),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Pulsar sample data producer")
    parser.add_argument("--count",    type=int,   default=0,   help="送信件数 (0=無限)")
    parser.add_argument("--interval", type=float, default=2.0, help="送信間隔 (秒)")
    args = parser.parse_args()

    try:
        import pulsar
    except ImportError:
        raise SystemExit("pulsar-client が必要です: pip install pulsar-client")

    client   = pulsar.Client(PULSAR_URL)
    producer = client.create_producer(TOPIC)
    print(f"Producing to {TOPIC}  (Ctrl+C で停止)")

    user_id = 1
    sent    = 0
    try:
        while args.count == 0 or sent < args.count:
            rec = make_record(user_id)
            msg = json.dumps(rec, ensure_ascii=False)
            producer.send(msg.encode("utf-8"))
            print(f"[{user_id:>5}] {msg}")
            user_id += 1
            sent    += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\n停止しました (送信済み: {sent} 件)")
    finally:
        producer.flush()
        client.close()


if __name__ == "__main__":
    main()
