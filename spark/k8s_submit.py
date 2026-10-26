#!/usr/bin/env python3
"""Entrypoint của Spark driver trong pod Argo Workflows (client mode, executor là pod k8s). Chỉ stdlib.

Đọc cấu hình từ env (do WorkflowTemplate đặt), dựng lệnh spark-submit rồi exec:
  - DQ_GATED_DATASET rỗng  -> exec spark-submit trực tiếp (probe/bootstrap/verify, không qua DQ gate)
  - DQ_GATED_DATASET có giá trị -> exec run_dataset_job.py (launcher) bọc spark-submit; launcher chờ executor
    pod (label dq-run-key) biến mất rồi mới gửi stop-confirmation `k8s-pod:...`.

Env bắt buộc: POD_NAME, POD_NAMESPACE, POD_IP, DQ_RUN_KEY, DQ_SPARK_IMAGE, DQ_S3_ENDPOINT, DQ_S3_SECRET,
DQ_JOB_CLASS, DQ_JOB_ARGS (JSON array string). Gated thêm: DQ_SERVICE_URL, DQ_PURPOSE, DQ_COMPAT_MANIFEST_ID,
DQ_STATE_DIR, DQ_API_TOKEN (chỉ đọc bởi launcher/JVM, không bao giờ vào argv hay Spark conf).
Tùy chọn: DQ_EXECUTOR_INSTANCES (2), DQ_EXECUTOR_MEMORY (1g), DQ_EXECUTOR_CORES (1), DQ_EXECUTOR_WAIT_SEC (300),
DQ_SPARK_SERVICE_ACCOUNT (dq-spark), DQ_JOB_JAR (local:///opt/dq/jars/dq-jobs-0.1.0.jar).
"""
from __future__ import annotations

import json
import os
import re
import sys

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
LABEL_VALUE = re.compile(r"^[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$")
LAUNCHER = "/opt/dq/bin/run_dataset_job.py"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"


class ConfigError(Exception):
    pass


def _req(env: dict, key: str) -> str:
    v = env.get(key, "").strip()
    if not v:
        raise ConfigError(f"thiếu env {key}")
    return v


def build(env: dict) -> list[str]:
    """argv để exec; không chứa token/credential (S3 qua secretKeyRef, token qua env)."""
    run_key = _req(env, "DQ_RUN_KEY")
    if not LABEL_VALUE.match(run_key):
        raise ConfigError("DQ_RUN_KEY phải là k8s label value hợp lệ")
    ns, pod, ip = _req(env, "POD_NAMESPACE"), _req(env, "POD_NAME"), _req(env, "POD_IP")
    secret = _req(env, "DQ_S3_SECRET")
    try:
        job_args = json.loads(_req(env, "DQ_JOB_ARGS"))
    except ValueError:
        raise ConfigError("DQ_JOB_ARGS phải là JSON array string") from None
    if not isinstance(job_args, list) or not all(isinstance(a, str) for a in job_args):
        raise ConfigError("DQ_JOB_ARGS phải là JSON array string")

    conf = {
        "spark.master": "k8s://https://kubernetes.default.svc:443",
        "spark.submit.deployMode": "client",
        "spark.kubernetes.namespace": ns,
        "spark.kubernetes.container.image": _req(env, "DQ_SPARK_IMAGE"),
        "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
        "spark.kubernetes.authenticate.caCertFile": f"{SA_DIR}/ca.crt",
        "spark.kubernetes.authenticate.oauthTokenFile": f"{SA_DIR}/token",
        "spark.kubernetes.authenticate.serviceAccountName": env.get("DQ_SPARK_SERVICE_ACCOUNT", "dq-spark"),
        # driver là owner của executor pod: xóa pod workflow -> GC executor
        "spark.kubernetes.driver.pod.name": pod,
        # driver và executor cùng image: executor nạp jar DQ từ image, không tải qua driver vào work-dir
        # (work-dir của image thuộc root, user spark không ghi được)
        "spark.jars": "local:///opt/dq/jars/dq-client-0.1.0.jar,local:///opt/dq/jars/dq-jobs-0.1.0.jar,"
                      "local:///opt/dq/jars/deequ-2.0.21-spark-3.5.jar",
        "spark.kubernetes.executor.label.dq-run-key": run_key,
        "spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID": f"{secret}:access-key",
        "spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY": f"{secret}:secret-key",
        "spark.driver.host": ip,
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.port": "7078",
        "spark.blockManager.port": "7079",
        "spark.executor.instances": env.get("DQ_EXECUTOR_INSTANCES", "2"),
        "spark.executor.memory": env.get("DQ_EXECUTOR_MEMORY", "1g"),
        "spark.executor.cores": env.get("DQ_EXECUTOR_CORES", "1"),
        "spark.hadoop.fs.s3a.endpoint": _req(env, "DQ_S3_ENDPOINT"),
    }
    dataset = env.get("DQ_GATED_DATASET", "").strip()
    if dataset:
        conf.update({
            "spark.dq.service.url": _req(env, "DQ_SERVICE_URL"),
            "spark.dq.dataset": dataset,
            "spark.dq.purpose": _req(env, "DQ_PURPOSE"),
            "spark.dq.compat.manifest.id": _req(env, "DQ_COMPAT_MANIFEST_ID"),
            "spark.dq.state.dir": _req(env, "DQ_STATE_DIR"),
        })
    submit = [SPARK_SUBMIT]
    for k, v in conf.items():
        submit += ["--conf", f"{k}={v}"]
    submit += ["--class", _req(env, "DQ_JOB_CLASS"), env.get("DQ_JOB_JAR", "local:///opt/dq/jars/dq-jobs-0.1.0.jar"),
               *job_args]
    if not dataset:
        return submit
    return [sys.executable, LAUNCHER, "--dataset", dataset, "--slot-dir", "/tmp/dq-slot",
            "--service-url", conf["spark.dq.service.url"],
            "--k8s-executor-wait", env.get("DQ_EXECUTOR_WAIT_SEC", "300"), "--command", *submit]


def main() -> int:
    try:
        argv = build(dict(os.environ))
    except ConfigError as e:
        print(f"k8s_submit: {e}", file=sys.stderr)
        return 2
    os.execv(argv[0], argv)
    return 2  # không tới


if __name__ == "__main__":
    sys.exit(main())
