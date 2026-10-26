# dq-jobs (job Spark Scala)

Các job Scala dùng [dq-client](../dq-client/README.md). Jar **mỏng**: `dq-jobs-0.1.0.jar` kèm các jar phụ thuộc trong `target/lib/` (không còn jar shade); image Spark chép chúng vào `/opt/dq/jars`.

| Object | Việc |
|---|---|
| `BronzeToSilverTransactions` | Job ghi qua DQ gate: Bronze Delta → Silver + quarantine |
| `AuditTable` | Audit chỉ đọc một snapshot Silver đã pin |
| `BootstrapTables` | Tạo sẵn Silver và quarantine (Delta rỗng) và in registration |
| `RestoreProbe` | Quét chỉ-đọc hai bảng đích sau restore PostgreSQL, sinh trang findings |
| `Maintenance` | Xoá thư mục candidate hết hạn, không pin |
| `CompatibilityProbe` | Kiểm tương thích Spark + Delta + runtime, sinh manifest |
| `TransactionsTransform` | Transform Bronze → Silver, dùng chung ngữ nghĩa với job Python |

Ghi chú chung: lỗi chỉ in **mã ổn định** (`DQ ERROR: <code>`), không in message gốc vì có thể chứa giá trị dữ liệu. Token lấy từ env `DQ_API_TOKEN`; cấu hình `spark.dq.*` và `DQ_RUN_KEY` xem [dq-client](../dq-client/README.md#cấu-hình). Nên chạy qua [scripts/run_dataset_job.py](../../scripts/run_dataset_job.py), launcher này đặt `DQ_RUN_KEY`.

## `BronzeToSilverTransactions`

```
đọc Bronze (Delta root, versionAsOf cố định) -> transform -> startWrite -> evaluate -> submit -> publish
```

```bash
spark-submit --class dqplatform.jobs.BronzeToSilverTransactions \
  --jars /opt/dq/jars/dq-client-0.1.0.jar,/opt/dq/jars/deequ-2.0.21-spark-3.5.jar \
  --conf spark.dq.service.url=http://dq-service:8000 \
  --conf spark.dq.dataset=silver.transactions.scala \
  --conf spark.dq.compat.manifest.id=<manifest-uuid> \
  --conf spark.dq.state.dir=/tmp/dq-state \
  /opt/dq/jars/dq-jobs-0.1.0.jar --dt 2026-10-17 --lake /data/lake
```

| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `--dt` | bắt buộc | Ngày partition `yyyy-MM-dd` |
| `--lake` | `/data/lake` | Gốc Bronze dev; đọc `<lake>/bronze/transactions` (Delta, partition `dt`) |

Chỉ hai tham số này. Bảng đích (Silver, quarantine), candidate root và predicate do **registration + plan** của dataset quyết định; job không nhận đường dẫn đích. Dataset được chọn bằng `spark.dq.dataset` (bản Python và Scala cần hai dataset riêng với bảng riêng để chạy song song).

Job:

1. Đọc Bronze ở một version cố định, lọc `dt`; dùng cùng version cho transform và `source_row_count` (số giao dịch khác nhau, độc lập với transform).
2. Tham số suite (`partition_start`, `partition_end`, `source_row_count`) chỉ là **dữ kiện**; ngưỡng nằm trong suite ở registry.
3. Kiểm runtime profile trước khi đếm nguồn và trước khi materialize.
4. Gọi `startWrite`/`evaluate`/`submit`; nếu `ALLOW` thì `publish`. Job không tự ghi Silver hay quarantine.

Transform giữ nguyên ngữ nghĩa: chuẩn hoá `msisdn`, `amount` → `decimal(18,2)`, `status`, `event_time` UTC; dedup theo `transaction_id` lấy `ingest_time` mới nhất, giữ mọi dòng thiếu `transaction_id`; `dt` là `DateType`. Sửa transform thì sửa cả [bronze_to_silver.py](../../spark/jobs/bronze_to_silver.py).

### Exit code

| Code | Ý nghĩa |
|---|---|
| `0` | Cả Silver và quarantine đã commit (quarantine 0 dòng, `NOT_APPLICABLE`, vẫn tính là thành công) |
| `3` | Gate `DENY`: không ghi bảng đích |
| `2` | Lỗi kỹ thuật, service, cấu hình, hoặc ghi chưa xác định (`PARTIAL`/`UNKNOWN`, cần reconcile) |

Dòng cuối stdout là JSON `{execution_id, decision, publication}` (không có dòng dữ liệu).

## `AuditTable`

```bash
spark-submit --class dqplatform.jobs.AuditTable ... /opt/dq/jars/dq-jobs-0.1.0.jar \
  --param partition_start="2026-10-17 00:00:00" --param partition_end="2026-10-18 00:00:00"
```

Cần grant `job` cho purpose của workflow `AUDIT_TABLE`. Đọc registration, chụp version hiện tại của Silver, `startAudit`, đọc đúng `versionAsOf` đó, đo và gửi report. Ghi gì vào bảng nào cũng không. Một lần append sau khi đã pin không ảnh hưởng kết quả. `--param name=value` lặp lại cho tham số suite đã khai báo.

Exit `0` khi `assessment` là `PASS` hoặc `VIOLATION`; `2` khi `ERROR` hoặc lỗi service/cấu hình. Stdout thêm trường `assessment`.

## `BootstrapTables`

```bash
spark-submit --class dqplatform.jobs.BootstrapTables ... dq-jobs-0.1.0.jar \
  --root s3a://bucket/lake --dataset silver.transactions --registration-output /tmp/registration.json
```

Tạo Silver và quarantine rỗng (Delta, `partitionBy dt`), thất bại nếu đường dẫn đã tồn tại, rồi ghi registration (URI, `table_id`, schema hash đọc từ bảng đã lưu) để `PUT /datasets/{D}`. Ghi thường không bao giờ tự tạo bảng đích. Không bao giờ đặt `delta.setTransactionRetentionDuration`. Schema Silver: `transaction_id, subscriber_id, msisdn: string; amount: decimal(18,2); status, province_code: string; event_time: timestamp; dt: date`; quarantine thêm `_dq_execution_id`, `_dq_errors array<string>`, `_dq_candidate_version`, `_dq_quarantined_at`.

## `RestoreProbe`

```bash
spark-submit --class dqplatform.jobs.RestoreProbe ... dq-jobs-0.1.0.jar \
  --silver <uri> --quarantine <uri> --scheduler-stop-reference <ref> --output /tmp/findings.json
```

Đọc chỉ-đọc snapshot hiện tại và mọi SetTransaction `dq:` còn giữ của hai bảng, gắn partition `dt` từng token đã chạm (từ add/remove action, hoặc `partition_values` trong `userMetadata` cho overwrite rỗng). Xuất mảng các trang `RestoreFindings` (liên tiếp, tối đa 2000 token và ~900 KiB mỗi trang, `final_page` ở trang cuối) để `POST /datasets/{D}/restore/findings`. Không sửa hay xoá gì trên Delta. Bảng đổi trong lúc quét thì dừng và quét lại với `scan_id` mới; thất bại exit `2`.

## `Maintenance`

```bash
spark-submit --class dqplatform.jobs.Maintenance ... dq-jobs-0.1.0.jar \
  --retention-file retention.json --candidate-root <uri> --output removed.json
```

Đầu vào là phản hồi của `GET /datasets/{D}/retention` (operator). Chỉ xoá thư mục `raw`/`classified` không `pinned` dưới `--candidate-root`; không bao giờ đụng bảng đích hay đường dẫn ngoài root; không VACUUM. Đầu ra là danh sách đã xoá để operator `POST /datasets/{D}/cleanup-records`. Chạy trong cùng slot của dataset (qua launcher) để không đua với job ghi.

## `CompatibilityProbe`

`--root`, `--output`, `--manifest-output`. Tự tạo bảng tổng hợp dưới `--root` riêng, kiểm `versionAsOf`, `replaceWhere` cô lập partition, overwrite rỗng, token `txnAppId/txnVersion` trùng không thêm dữ liệu, tra cứu token + `userMetadata`, thuộc tính retention vắng mặt; chỉ in `status: PASS` kèm hash manifest khi mọi kiểm tra qua, lỗi thì exit `1`. Thường chạy bởi [scripts/run_acceptance.py](../../scripts/run_acceptance.py). Manifest gắn với **hash jar thật**: đổi client thì phải sinh và accept manifest mới.

## Build và test

```bash
mvn -f jvm/pom.xml -B package    # test các module; jar mỏng + target/lib
mvn -f jvm/pom.xml -B test
```

| Spec | Kiểm gì |
|---|---|
| `BronzeToSilverTransactionsSpec` | Toàn bộ job với service giả và Delta tạm: ALLOW/DENY, exit code, chạy lại |
| `AuditTableSpec` | Audit đọc đúng version đã pin, không ghi |
| `JobsToolsSpec` | `Maintenance`, `RestoreProbe`, `BootstrapTables` |
| `CompatibilityProbeSpec` | Probe tương thích |

## Thêm một job mới

1. Tạo object trong `dqplatform.jobs`, viết `run(spark, args, dq): Int` để test được, giữ exit code `0/3/2`.
2. Dùng `startWrite → evaluate → submit → publish` (hoặc `startAudit`); không tự ghi bảng đích.
3. Đăng ký dataset/suite/policy/purpose qua registry ([suites/](../../suites), [RULE_SPEC_V2.md](../../docs/RULE_SPEC_V2.md)).
4. Chạy qua [scripts/run_dataset_job.py](../../scripts/run_dataset_job.py) để dùng chung slot với job cùng cặp bảng.
