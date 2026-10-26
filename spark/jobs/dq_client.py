"""
dq_client — py4j wrapper mỏng trên dqplatform.client.PyBridge (jar DQ nạp qua spark.jars).

Mọi logic (compile rule, đo bằng Deequ, quyết định gate, ghi Delta) nằm ở JVM + service; Python chỉ chuyển JSON
handle/evaluation/accepted và DataFrame (`df._jdf`). Không tính rule/policy lại, không ghi Delta business output.

Cấu hình: spark.dq.service.url, spark.dq.dataset, spark.dq.purpose, spark.dq.compat.manifest.id,
spark.dq.state.dir, spark.dq.service.timeout.sec; env DQ_API_TOKEN, DQ_RUN_KEY (bắt buộc), DQ_IMAGE_DIGEST.
"""

from __future__ import annotations

import json

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession

# exception JVM mang `code` an toàn; không bao giờ chuyển message gốc sang Python.
_CODED = (
    "dqplatform.client.DQFailedException",
    "dqplatform.client.OutcomeConflict",
    "dqplatform.client.ServiceException",
    "dqplatform.client.ProtocolException",
)


class DQFailed(RuntimeError):
    """Lỗi kỹ thuật/dịch vụ/cấu hình/conflict: job exit 2. `code` là mã ổn định từ JVM."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _dumps(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


class DQClient:
    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._bridge = spark._jvm.dqplatform.client.PyBridge
        self._jspark = spark._jsparkSession

    def _call(self, name: str, *args) -> str:
        try:
            return getattr(self._bridge, name)(self._jspark, *args)
        except Py4JJavaError as e:
            ex = e.java_exception
            if ex.getClass().getName() in _CODED:
                raise DQFailed(str(ex.code())) from None
            raise
        except TypeError as e:  # py4j trả JavaPackage khi không thấy class
            raise RuntimeError("không tìm thấy dqplatform.client.PyBridge: thêm jar DQ vào spark.jars") from e

    def start_write(self, partition_values: dict, parameters: dict, event_time: str,
                    event_origin: str = "SCHEDULED", previous_execution_id: str | None = None) -> dict:
        """event_time: Instant ISO-8601 UTC (vd 2026-10-20T00:00:00Z). Mọi value là string."""
        req = {
            "partition_values": {k: str(v) for k, v in partition_values.items()},
            "parameters": {k: str(v) for k, v in parameters.items()},
            "event_time": event_time,
            "event_origin": event_origin,
        }
        if previous_execution_id is not None:
            req["previous_execution_id"] = previous_execution_id
        return json.loads(self._call("startWrite", _dumps(req)))

    def evaluate(self, handle: dict, df: DataFrame) -> dict:
        return json.loads(self._call("evaluate", _dumps(handle), df._jdf))

    def submit(self, evaluation: dict) -> dict:
        """AcceptedResponse; `decision` null hoặc {decision: ALLOW|DENY, ...}."""
        return json.loads(self._call("submit", _dumps(evaluation)))

    def publish(self, evaluation: dict, accepted: dict) -> dict:
        """{silver:{state,evidence,commit_version}, quarantine:{...}} — chỉ gọi khi decision ALLOW."""
        return json.loads(self._call("publish", _dumps(evaluation), _dumps(accepted)))
