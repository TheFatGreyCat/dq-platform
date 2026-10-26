# spark (image, cấu hình và job Python)

Phía Spark (data plane): image chạy cả job Python lẫn Scala, cấu hình `spark.dq.*` và runtime profile, job Python mẫu và bộ sinh Bronze.

```
spark/
├── Dockerfile                 # stage 1: mvn package (jar mỏng) + connector jars; stage 2: apache/spark 3.5.1
├── conf/spark-defaults.conf   # jar DQ, Delta, runtime profile dq-runtime-v1, S3A, spark.dq.*
├── with-s3-env.sh             # nạp credential S3A từ /run/secrets vào env của tiến trình Spark
├── jobs/
│   ├── dq_client.py           # cầu py4j mỏng trên dqplatform.client.PyBridge
│   ├── bronze_to_silver.py    # job Python: transform -> start_write -> evaluate -> submit -> publish
│   └── generate_bronze.py     # sinh Bronze mẫu (Delta, partition dt) có lỗi cố ý
└── tests/test_jobs.py         # hành vi dữ liệu của job Python (chạy trong image Spark)
```

Logic DQ **không** nằm ở đây: đo, phân loại dòng, ghi Delta nằm ở [jvm/dq-client](../jvm/dq-client/README.md); quyết định ở [dq-service](../dq-service/README.md). Phía Python chỉ chuyển JSON và DataFrame, không tự tính rule/policy và không ghi Delta bảng đích. Job Scala tương đương ở [jvm/dq-jobs](../jvm/dq-jobs/README.md).

## Dockerfile

Build context phải là **thư mục gốc repo**; bắt buộc truyền `HADOOP_VERSION` (phải đúng Hadoop của image Spark; không có mặc định):

```bash
docker build -f spark/Dockerfile --build-arg HADOOP_VERSION=<version> -t dq-demo/spark-deequ:3.5.1 .
```

| Stage | Làm gì |
|---|---|
| `build` (Maven, JDK 17, pin digest) | `mvn package` ra `dq-client`, `dq-jobs` và jar phụ thuộc rời (Deequ...); tải `hadoop-aws` đúng `HADOOP_VERSION`, `aws-java-sdk-bundle` theo POM của nó, và `delta-spark`/`delta-storage` 3.2.0 |
| cuối (`apache/spark:3.5.1-scala2.12-java17-python3-ubuntu`, pin digest) | Delta/S3A vào `/opt/spark/jars`; jar DQ và Deequ vào `/opt/dq/jars`; `spark-defaults.conf`, `k8s_submit.py`, `run_dataset_job.py` vào image; `WORKDIR /opt/dq/work` (user `spark` ghi được, executor k8s chép file vào đây) |

Không còn jar shade: mỗi jar giữ nguyên để hash từng jar trong compat manifest có nghĩa. Không cần `pip install` thêm: Deequ chạy trong JVM, executor không cần package Python.

Trên k8s, entrypoint driver là `/opt/dq/bin/k8s_submit.py` (env do WorkflowTemplate `dq-spark-job` đặt): dựng `spark-submit` client mode với `spark.master=k8s://...`, jar `local://` từ image, executor gắn label `dq-run-key`, credential S3 qua `secretKeyRef`; job có DQ gate được bọc bởi `run_dataset_job.py --k8s-executor-wait`.

## spark-defaults.conf

| Config | Ý nghĩa |
|---|---|
| `spark.jars` | `dq-client`, `dq-jobs`, Deequ. Thiếu thì Python báo không tìm thấy `PyBridge` |
| `spark.sql.extensions`, `spark.sql.catalog.spark_catalog` | Delta |
| 5 conf runtime profile `dq-runtime-v1` | `spark.sql.session.timeZone=UTC`, `spark.sql.ansi.enabled=false`, `spark.sql.caseSensitive=false`, `spark.sql.legacy.timeParserPolicy=EXCEPTION`, `spark.sql.sources.partitionOverwriteMode=static`. Server ghim đúng 5 khoá này trong plan; SDK kiểm lại, lệch thì dừng (`RUNTIME_MISMATCH`) trước khi đọc dữ liệu |
| `spark.hadoop.fs.s3a.*` | MinIO/S3A path-style; credential qua env `AWS_*` do `with-s3-env.sh` đặt từ file secret, không nằm trong conf, CLI hay log |
| `spark.dq.service.url`, `spark.dq.service.timeout.sec`, `spark.dq.purpose` | Địa chỉ service, timeout HTTP (mặc định 10), purpose (mặc định `default`) |

`spark.dq.dataset`, `spark.dq.compat.manifest.id`, `spark.dq.state.dir` do launcher/harness đặt theo từng lần chạy. Token: env `DQ_API_TOKEN` của driver; `DQ_RUN_KEY` bắt buộc (launcher đặt). Bảng đầy đủ các key ở [dq-client](../jvm/dq-client/README.md#cấu-hình).

## Job Python

### `dq_client.py`

```python
dq = DQClient(spark)
handle     = dq.start_write({"dt": "2026-10-17"}, parameters, "2026-10-17T00:00:00Z")
evaluation = dq.evaluate(handle, df)
accepted   = dq.submit(evaluation)       # accepted["decision"]["decision"] là ALLOW hoặc DENY
publication = dq.publish(evaluation, accepted)   # chỉ khi ALLOW; {silver:{state,...}, quarantine:{...}}
```

Handle/evaluation/accepted là dict JSON. `DQFailed` mang `code` ổn định của JVM (exception gốc không bao giờ được chuyển sang Python). Không có `DQResult`, `verdict`, `valid_df` hay cờ publish.

### `bronze_to_silver.py`

Luồng giống job Scala: kiểm runtime profile → đọc Bronze Delta ở version cố định, lọc `dt` → tính tham số (kể cả `source_row_count` độc lập với transform) → transform (`dt` là `DateType`) → `start_write` → `evaluate` → `submit` → nếu `ALLOW` thì `publish` (JVM ghi quarantine rồi Silver).

```bash
spark-submit --py-files jobs/dq_client.py jobs/bronze_to_silver.py --dt 2026-10-17 --lake /data/lake
```

Chỉ có `--dt` và `--lake` (Bronze dev). Bảng đích do registration của dataset (`spark.dq.dataset`) quyết định. Exit code: `0` đã commit cả hai (quarantine không áp dụng cũng tính), `3` `DENY`, `2` lỗi kỹ thuật/service/cấu hình/ghi chưa xác định. Dòng cuối stdout là JSON `{execution_id, decision, publication}`. Nên chạy qua [scripts/run_dataset_job.py](../scripts/run_dataset_job.py).

Transform phải khớp [TransactionsTransform.scala](../jvm/dq-jobs/src/main/scala/dqplatform/jobs/TransactionsTransform.scala): sửa một bên thì sửa cả hai, rồi so hai execution bằng [scripts/parity_check.py](../scripts/parity_check.py) (`--python-execution E1 --scala-execution E2 --service-url URL --output result.json`, token ở env `DQ_API_TOKEN`): so report đã lưu (rule, số đo, schema evidence, số đếm, lỗi, decision) và với `--spark --dt` so từng dòng của output đã publish bằng `exceptAll` hai chiều.

### `generate_bronze.py`

Sinh Bronze **Delta** (`partitionBy dt`, `dt` kiểu date) với 50 dòng replay và lỗi cố ý mỗi loại ứng với một dimension. Ngày đã có thì thay đúng partition bằng `replaceWhere` (giữ ngày khác); lần đầu tạo bảng.

```bash
spark-submit generate_bronze.py --out /data/lake/bronze/transactions --dt 2026-10-17 --rows 10000 --bad-ratio 0.03
```

`--bad-ratio` thử kịch bản `DENY` khi tỉ lệ quarantine vượt `max_quarantine_ratio` của policy. `--seed` mặc định 42.

## Chạy

Dùng stack cô lập ([README gốc](../README.md#stack-cô-lập-và-nghiệm-thu)); driver Spark là service `spark` (profile `jobs`):

```bash
DQ_SECRETS_DIR=/abs/secrets MINIO_IMAGE=<image@sha256:...> HADOOP_VERSION=<version> \
  docker compose -p <project> run --rm spark <spark-submit ...>
```

Dataset, grant, suite, purpose và manifest phải được tạo và accept trước (README gốc, "Thứ tự khởi tạo").

## Test

`spark/tests/test_jobs.py` kiểm hành vi dữ liệu (Bronze Delta, transform, tham số suite) và phải chạy trong image Spark; lệnh nằm ở docstring đầu file. Test của service và JVM: `python -m pytest dq-service/tests`, `mvn -f jvm/pom.xml -B test`; của launcher: `python -m pytest scripts/tests`.
