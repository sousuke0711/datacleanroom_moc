"""
メタデータ駆動 仮名化/匿名化 Flink ジョブ。

フロー:
  Pulsar (raw-user-data)
    → AnonymizationFunction  ← OpenMetadata PIIタグ取得 (起動時)
    → Pulsar (anonymized-user-data)

環境変数:
  PULSAR_SERVICE_URL    デフォルト: pulsar://pulsar:6650
  PULSAR_ADMIN_URL      デフォルト: http://pulsar:8080
  INPUT_TOPIC           デフォルト: persistent://public/default/raw-user-data
  OUTPUT_TOPIC          デフォルト: persistent://public/default/anonymized-user-data
  OM_BASE_URL           デフォルト: http://openmetadata:8585/api
  TABLE_FQN             デフォルト: sample_data.default.default.users
  ANONYMIZATION_SECRET  デフォルト: change-me-in-production
"""
import json
import logging
import os

from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.pulsar import (
    DeliveryGuarantee,
    PulsarSink,
    PulsarSource,
    StartCursor,
)
from pyflink.datastream.functions import MapFunction, RuntimeContext

from anonymizer import Anonymizer
from openmetadata_client import OpenMetadataClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

PULSAR_SERVICE = os.getenv("PULSAR_SERVICE_URL", "pulsar://pulsar:6650")
PULSAR_ADMIN   = os.getenv("PULSAR_ADMIN_URL",   "http://pulsar:8080")
INPUT_TOPIC    = os.getenv("INPUT_TOPIC",  "persistent://public/default/raw-user-data")
OUTPUT_TOPIC   = os.getenv("OUTPUT_TOPIC", "persistent://public/default/anonymized-user-data")
OM_URL         = os.getenv("OM_BASE_URL",  "http://openmetadata:8585/api")
TABLE_FQN      = os.getenv("TABLE_FQN",    "sample_data.default.default.users")


class AnonymizationFunction(MapFunction):
    """
    OpenMetadata から取得したPIIルールに従い、各フィールドを匿名化する。
    ルールは open() で一度取得してキャッシュ。OpenMetadata 側のキャッシュTTLは60秒。
    """

    def __init__(self, om_url: str, table_fqn: str):
        self._om_url    = om_url
        self._table_fqn = table_fqn
        self._client: OpenMetadataClient = None
        self._anon:   Anonymizer         = None
        self._rules:  dict               = {}

    def open(self, ctx: RuntimeContext):  # noqa: ARG002
        self._client = OpenMetadataClient(self._om_url)
        self._anon   = Anonymizer()
        self._rules  = self._client.get_anonymization_rules(self._table_fqn)
        logger.info("Rules loaded from OpenMetadata: %s", self._rules)

    def map(self, record: str) -> str:
        try:
            data = json.loads(record)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Invalid JSON skipped: %.120s", record)
            return record

        out = {
            field: (self._anon.apply(val, self._rules[field]) if field in self._rules else val)
            for field, val in data.items()
        }
        return json.dumps(out, ensure_ascii=False)


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30_000)

    source = (
        PulsarSource.builder()
        .set_service_url(PULSAR_SERVICE)
        .set_admin_url(PULSAR_ADMIN)
        .set_topics(INPUT_TOPIC)
        .set_start_cursor(StartCursor.latest())
        .set_deserialization_schema(SimpleStringSchema())
        .set_subscription_name("flink-anon-sub")
        .build()
    )

    sink = (
        PulsarSink.builder()
        .set_service_url(PULSAR_SERVICE)
        .set_admin_url(PULSAR_ADMIN)
        .set_topics(OUTPUT_TOPIC)
        .set_serialization_schema(SimpleStringSchema())
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    (
        env
        .from_source(source, WatermarkStrategy.no_watermarks(), "Pulsar: raw-user-data")
        .map(AnonymizationFunction(OM_URL, TABLE_FQN), output_type=Types.STRING())
        .sink_to(sink)
    )

    logger.info("Starting PII Anonymization Pipeline ...")
    env.execute("PII Anonymization Pipeline")


if __name__ == "__main__":
    main()
