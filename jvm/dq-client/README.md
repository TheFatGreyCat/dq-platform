# dq-client (SDK DQ cho Spark)

Thư viện Scala mà mọi job Spark nhúng vào để chạy **write gate** và **audit** theo API v2 của [dq-service](../../dq-service/README.md), đo bằng [Deequ](https://github.com/awslabs/deequ). Job Scala gọi trực tiếp; job Python gọi qua py4j bằng [spark/jobs/dq_client.py](../../spark/jobs/dq_client.py) và `PyBridge`. Cả hai dùng chung một bộ đo, một bộ phân loại dòng và một publisher.

Stack: Scala 2.12.18, Spark 3.5.1 (`provided`), Deequ `2.0.21-spark-3.5`, Delta 3.2.0, Java release 11. Jar phân phối **mỏng** (không shade): `dq-client-0.1.0.jar` cùng jar phụ thuộc riêng, để hash từng jar trong compat manifest có nghĩa.

SDK **không quyết định gì**: không chọn suite/revision/policy, không tính ngưỡng, không có `status`/`severity`/verdict. Nó đo, ghi candidate bất biến, gửi số đo và ghi theo đúng quyết định đã lưu của server. Lỗi service, cấu hình hay runtime làm job dừng (fail-closed); không có chế độ monitor hay bỏ qua DQ.

## Luồng ghi

```scala
val dq     = DQClient.fromSession(spark)
val handle = dq.startWrite(Map("dt" -> "2026-10-17"), parameters, eventTime)
val eval   = dq.evaluate(handle, transformedDf)
val acc    = dq.submit(eval)                 // AcceptedResponse: decision ALLOW/DENY
if (acc.path("decision").path("decision").asText == "ALLOW") dq.publish(eval, acc)
```

1. **`startWrite`**: đọc registration, chụp snapshot hai bảng đích (`DeltaRuntime.snapshot`) làm `target_bases`, `POST /executions` rồi `POST .../attempts`. Plan đã đóng băng, đường dẫn candidate do server cấp. `Idempotency-Key` suy ra từ `DQ_RUN_KEY`, nên retry cùng lần chạy không tạo execution mới. Handle lưu ở `spark.dq.state.dir`.
2. **`evaluate`**: kiểm runtime profile (5 conf Spark) và hash jar so với plan **trước khi** materialize; lệch thì kết thúc attempt (`RUNTIME_MISMATCH`, `process_stop` UNCONFIRMED vì process còn chạy) và ném lỗi. Sau đó ghi candidate `raw` dạng Delta ở đường dẫn của attempt (`errorifexists`, không bao giờ overwrite), đọc lại đúng version, đo từng stage (`RAW`, `CANDIDATE`, `CLEAN`), phân loại dòng ra bảng `classified` riêng (`Classify`), đọc lại, rồi viết report ra file cục bộ dưới dạng bytes chuẩn tắc. Số đếm là số nguyên chính xác, không làm tròn theo tỉ lệ. Lỗi đo của từng rule thành `MeasurementResult` `ERROR` (kèm `client_error` có mã), không bao giờ là `OK` với giá trị null.
3. **`submit`**: `POST .../reports` đúng bytes đã lưu. Mất kết nối thì `GET .../report` và so hash; chưa lưu thì gửi lại **cùng bytes** một lần. Không chạy lại transform sau khi server đã chấp nhận.
4. **`publish`**: `DeltaPublisher` ghi từ snapshot `classified` đã được duyệt, không chạy lại transform.

Audit: `startAudit(input, parameters, eventTime)` → `evaluateAudit(handle, df)` (đọc `versionAsOf` đã pin) → `submit`. Không ghi bảng nào.

`failAttempt(handle, error)` kết thúc attempt khi job lỗi trước report.

## Các file

| File | Vai trò |
|---|---|
| [Protocol.scala](src/main/scala/dqplatform/client/Protocol.scala) | Kiểu dây v3: `SnapshotRef`, `RuntimeVersions`, `ClientError`, parse nghiêm ngặt (khoá lạ bị từ chối) |
| [Canonical.scala](src/main/scala/dqplatform/client/Canonical.scala) | JSON chuẩn tắc và hash; dùng chung vector kiểm thử với Python |
| [PlanRules.scala](src/main/scala/dqplatform/client/PlanRules.scala) | Đọc rule đã biên dịch từ `plan.stage_bindings` (`CompiledRule`); client không đọc suite thô |
| [Measure.scala](src/main/scala/dqplatform/client/Measure.scala) | Mỗi rule thành một Deequ `Check` chỉ để **đo** (assertion luôn đúng), gồm rule schema typed từ schema thật của bảng đã lưu |
| [Classify.scala](src/main/scala/dqplatform/client/Classify.scala) | Gắn cột `_dq_errors` (id rule `QUARANTINE` vi phạm, sắp xếp, không trùng) cho mọi dòng; `clean()` bỏ cột `_dq_*` |
| [DeltaRuntime.scala](src/main/scala/dqplatform/client/DeltaRuntime.scala) | Bọc nội bộ Delta 3.2.0: snapshot, `table_id`, schema, SetTransaction, commit đã lưu |
| [RuntimeProbe.scala](src/main/scala/dqplatform/client/RuntimeProbe.scala) | Phiên bản và SHA-256 của 5 jar (qua CodeSource, không phải version text); profile 5 conf; thiếu CodeSource thì lỗi |
| [ServiceClient.scala](src/main/scala/dqplatform/client/ServiceClient.scala) | `ServiceApi.request(method, path, query, body, idempotencyKey)`; `HttpServiceApi` dùng HTTP/1.1, mã hoá từng segment đường dẫn; lỗi chỉ mang `code` + `status`, không chứa body/URI/token |
| [DQClient.scala](src/main/scala/dqplatform/client/DQClient.scala) | `DQClient`, `DQConfig`, `ExecutionHandle`, `CandidateEvaluation` |
| [DeltaPublisher.scala](src/main/scala/dqplatform/client/DeltaPublisher.scala) | Ghi quarantine rồi Silver, báo outcome, đối soát |
| [PyBridge.scala](src/main/scala/dqplatform/client/PyBridge.scala) | Cầu py4j: JSON vào/ra, DataFrame truyền qua `df._jdf` |

## Cấu hình

| Nguồn | Khoá | Ý nghĩa |
|---|---|---|
| Spark conf | `spark.dq.service.url`, `spark.dq.dataset`, `spark.dq.compat.manifest.id`, `spark.dq.state.dir` | Bắt buộc. `state.dir` là path local hoặc Hadoop URI (`s3a://...`); trên k8s phải là object store để handle/report của attempt sống qua restart pod driver |
| Spark conf | `spark.dq.purpose` (mặc định `default`), `spark.dq.service.timeout.sec` (mặc định `10`) | |
| env | `DQ_API_TOKEN` | Token job. Không đặt trong Spark conf vì sẽ hiện trên Spark UI |
| env | `DQ_RUN_KEY` | **Bắt buộc**, ổn định cho một lần gọi logic (launcher đặt); dùng làm khoá idempotency |
| env | `DQ_IMAGE_DIGEST` | Tuỳ chọn, ghi vào attempt (không nằm trong manifest được xác minh) |

## Ghi Delta (`DeltaPublisher`)

- Mỗi destination có một transaction token `txnAppId = dq:<execution_id>:<destination>`, `txnVersion = 1` và `userMetadata` JSON chuẩn tắc `{classified_table_id, classified_version, decision_id, destination, execution_id, partition_values, plan_hash, report_hash}`; `metadata_hash = sha256(userMetadata)`.
- Trước khi ghi, publisher tự `GET` execution/report từ service để kiểm `ALLOW`, `plan_hash`, `report_hash`, snapshot `classified`; không tin cờ do Python truyền. Kiểm URI, `table_id`, schema, version và file của candidate; thiếu hoặc hỏng thì `CANDIDATE_UNAVAILABLE`, không ghi bảng đích.
- Báo `IN_PROGRESS` (kèm intent) **trước** khi `.write`. **Quarantine trước**: append các dòng có `_dq_errors` khác rỗng cùng `_dq_execution_id`, `_dq_candidate_version`, `_dq_quarantined_at` (= `plan.execution_timestamp`). Quarantine 0 dòng do server đặt `NOT_APPLICABLE`, không ghi gì. Chỉ khi quarantine thành công (hoặc không áp dụng) mới ghi Silver: projection sạch giữ `dt` kiểu `date`, `overwrite` với `replaceWhere` = predicate server (`dt = DATE '<ngày>'`); partition rỗng vẫn thực sự xoá partition đó. Partition khác giữ nguyên.
- Lỗi mơ hồ quanh `.write` (mạng, storage) → destination `UNKNOWN` rồi đối soát ngay bằng log Delta; không tự thử ghi lại. Đối soát `EXACT` tìm đúng SetTransaction + `userMetadata` trong commit còn giữ; khi đã có commit exact thì chỉ báo, không ghi lại dù base đã đổi. Cùng token khác nội dung là `PLAN_MISMATCH`. Base đổi so với plan thì dừng (`BASE_CHANGED`), không ghi đè dữ liệu của người khác.
- Quy tắc phục hồi sau sự cố (operator, `TXN_ONLY`, đóng không giải quyết được) ở [dq-service/README.md](../../dq-service/README.md#kết-quả-ghi-và-publication) và [scripts/dq_operator.py](../../scripts/dq_operator.py).

## Vòng đời cache và tài nguyên

`CandidateEvaluation.release()` chỉ gỡ cache/temp view do **chính SDK** tạo (`sessionState.catalog.dropTempView`, không `spark.catalog.dropTempView` để tránh unpersist DataFrame của người dùng); nó không xoá file candidate hay pin. File candidate do `Maintenance` dọn theo retention.

## Test

```bash
mvn -f jvm/pom.xml -B test           # cả ba module; Spark local + Delta, không cần service hay MinIO
mvn -f jvm/pom.xml -B -pl dq-client -am test
```

| Spec | Kiểm gì |
|---|---|
| `ProtocolSpec` | Parse nghiêm ngặt; hash chuẩn tắc theo `protocol-v3.json` (cùng file pytest đọc) |
| `MeasureSpec`, `DeequSemanticsSpec` | **Ghim hành vi Deequ** mà SDK dựa vào (null, row-level, `customSql`, số đo); nâng Deequ làm đổi hành vi thì đỏ |
| `CandidateSnapshotSpec` | Candidate bất biến: raw/classified/clean/quarantine đếm đúng, `_dq_errors` đúng, thiếu file → `CANDIDATE_UNAVAILABLE` |
| `DQClientSpec` | Luồng start/evaluate/submit với service giả, retry mất kết nối, lệch runtime |
| `DeltaPublisherSpec` | Ghi, thử lại, đối soát, base đổi, partition khác không bị đụng, trên Delta thật cục bộ |
| `HttpServiceApiSpec` | HTTP thật tới server giả: header, mã hoá đường dẫn, phân loại lỗi, không rò body |

Test dùng bảng Delta tạm cho mỗi suite (`SparkTestSession`), không có lake dùng chung; `DeltaPublisherSpec` và `DQClientSpec` cần Spark local nên chậm hơn.

## Thêm một `kind` rule mới

1. Phía service: `models.py` (class check vào union `Check`, `ROW_LEVEL_KINDS`, `METRIC_NAMES`) và biên dịch plan (`plan.py`).
2. `Measure.scala`: ánh xạ sang Deequ (hoặc đo schema), tên metric và kiểu giá trị theo server.
3. Test trong `DeequSemanticsSpec`/`MeasureSpec` (hành vi, gồm null) và vector hash trong `protocol-v3.json`.
4. Cập nhật [docs/RULE_SPEC_V2.md](../../docs/RULE_SPEC_V2.md).

## Những điều dễ vấp

- **Null:** `hasPattern` của Deequ coi null là không khớp; client thêm `.where("col IS NOT NULL")` với `FilteredRowOutcome.TRUE`. `sql_predicate` phải tự null-safe vì kết quả null bị tính là vi phạm.
- **Quarantine chỉ rule row-level ở stage `CANDIDATE`:** rule không có kết quả theo dòng không gắn được `_dq_errors`; thiếu kết quả dòng của Deequ cho một rule quarantine thì phân loại là `INCOMPLETE` (service `DENY`), không đoán.
- **`${batch}`** là placeholder do SDK bind vào temp view của batch đang đo; tên view ngẫu nhiên không vào hash.
- **Cột `_dq_*`** dành riêng; schema nghiệp vụ không được dùng.
- **HTTP/1.1:** `HttpServiceApi` ép HTTP/1.1 vì `HttpClient` mặc định thử h2c khi POST.
- **Xung đột classpath:** jar phụ thuộc phân phối riêng (không shade); Spark, Scala, Hadoop core, Jackson là `provided`; Deequ loại trừ Spark/Scala/Breeze/Glue.
