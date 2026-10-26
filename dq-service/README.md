# dq-service (control plane)

FastAPI + PostgreSQL 16. Service **không chạm dữ liệu**: Deequ chạy trong Spark ([jvm/dq-client](../jvm/dq-client/README.md)), service giữ registry bất biến, đóng băng plan, nhận report, quyết định gate và theo dõi kết quả ghi. API ở tiền tố `/api/v2`, lỗi có dạng `{error:{code,message,field?}}` (message là template an toàn, không echo input).

## Cấu trúc

| File | Vai trò |
|---|---|
| [app.py](app.py) | Route, middleware giới hạn body 1 MiB, alert. `create_app(store, token_registry, anomaly_client, restore_mode=, alert_webhook_url=)` nhận phụ thuộc từ ngoài; `app_from_env()` đọc biến môi trường, không có side effect khi import |
| [models.py](models.py) | Pydantic (`extra=forbid`): suite `dq-rules/v3`, policy, registration, request/report/outcome |
| [canonical.py](canonical.py), [plan.py](plan.py) | JSON chuẩn tắc, hash, biên dịch `ExecutionPlan` đóng băng |
| [policy.py](policy.py) | `evaluate(plan, report)`: hàm thuần, ra `ALLOW`/`DENY` (ghi) hoặc `PASS`/`VIOLATION`/`ERROR` (audit) |
| [budget.py](budget.py) | Ước lượng kích thước report tối đa so với 1 MiB, khi activate và khi tạo plan |
| [auth.py](auth.py) | Token registry, principal, grant |
| [anomaly.py](anomaly.py) | Chọn lịch sử bị chặn, gọi worker, quan sát advisory |
| [errors.py](errors.py) | Mã lỗi API và mã `client_error` |
| [store.py](store.py), [schema.py](schema.py) | Giao dịch PostgreSQL (SQLAlchemy Core). SQL ở [postgres/migrations/002_execution_v2.sql](../postgres/migrations/002_execution_v2.sql) là schema chuẩn; service **không** tự tạo/migrate bảng, thiếu schema version 2 thì không khởi động |
| [start.sh](start.sh) | Supervisor trong container: worker anomaly (`127.0.0.1:8081`) + uvicorn (`8000`); một process chết thì container dừng |

## Cấu hình

| Biến | Ý nghĩa |
|---|---|
| `DATABASE_URL` hoặc `DATABASE_URL_FILE` | URL PostgreSQL (`postgresql+psycopg://...`); một trong hai bắt buộc |
| `DQ_TOKEN_REGISTRY_FILE` | **Bắt buộc.** Đường dẫn [token registry](#token-registry) |
| `DQ_ANOMALY_WORKER_URL` | Mặc định `http://127.0.0.1:8081` |
| `DQ_RESTORE_MODE` | `1` = mở marker đối soát khi khởi động, xem [Restore](#restore-sau-khi-khôi-phục-postgresql) |
| `DQ_ALERT_WEBHOOK_URL` | Tuỳ chọn. Rỗng thì không gửi alert |

Khởi chạy: `uvicorn app:app_from_env --factory`. `GET /health` công khai, trả trạng thái process, DB và worker.

## Token registry

File JSON (mount 0600), nguồn quyền duy nhất; **chỉ chứa SHA-256 của token**, không có token cleartext:

```json
{"principals": [{
  "principal": "ci-editor",
  "token_sha256": "<64 hex thường>",
  "grants": [{"dataset_id": "silver.transactions", "role": "editor", "workflow": null, "purpose": null}]
}]}
```

- Vai trò: `viewer`, `editor`, `approver`, `operator`, `job`. Cả bốn vai trò đầu đều đọc được dataset được cấp. `job` bắt buộc có `workflow` + `purpose` và chỉ truy cập execution của đúng purpose đó; vai trò khác phải để hai trường này `null`.
- Không có token toàn cục, admin hay wildcard. Dataset đầu tiên được tạo bởi `editor` có grant đúng `dataset_id` trong file; service lưu dataset và grant trong cùng transaction.
- Digest trùng, token rỗng hoặc file sai shape thì service không khởi động. Khi khởi động, grant trong file được đồng bộ vào `principal_grants`; bỏ grant khỏi file là mất quyền ngay dù lịch sử còn nguyên.
- Thiếu/sai token: 401. Thiếu quyền: 403. Resource thuộc dataset khác: 404.

## Registry (bất biến)

| Đối tượng | Route (prefix `/api/v2`) | Quyền |
|---|---|---|
| Dataset | `PUT/GET /datasets/{D}` | PUT: `editor` |
| Compat manifest | `POST /compat-manifests`, `GET /compat-manifests/{M}` | `editor` (POST) |
| Acceptance | `PUT /datasets/{D}/compat-manifests/{M}/acceptance` | `approver` |
| Suite | `POST /datasets/{D}/suites`, `GET .../suites/{S}` | `editor` |
| Revision suite / policy | `POST .../suites/{S}/revisions`, `.../policy-revisions`; `GET .../{R}` | `editor` |
| Purpose | `PUT /datasets/{D}/purposes/{W}/{P}`, `GET`, `GET .../events` | `editor` |
| Binding | `PUT .../purposes/{W}/{P}/binding`, `POST .../retire` | `approver` |

- **Dataset registration**: `owner`, bảng `silver`/`quarantine` (URI, `table_id`, schema hash), `candidate_root`, `allowed_storage_roots`, `business_schema`, `partition_schema` (duy nhất `dt:date`), `history_context`, `retention`. Cột `_dq_*` bị cấm trong schema nghiệp vụ; `delta.setTransactionRetentionDuration` phải vắng mặt (SetTransaction không giới hạn). Nội dung giống hệt thì `PUT` trả 200 không tăng gì.
- **Revision** chỉ thêm, không sửa. Cùng nội dung thì trả bản gốc (200, giữ người tạo ban đầu), nội dung mới trả 201. Mỗi `POST` cần header `Idempotency-Key`.
- **Compat manifest** (`dq-compat-v1`): phiên bản Spark/Scala/Java/Hadoop và hash 5 jar (`delta_spark`, `delta_storage`, `hadoop_aws`, `deequ`, `dq_client`) do `CompatibilityProbe` sinh. Chỉ dùng được cho dataset sau khi một `approver` xác nhận (kèm `harness_reference` không rỗng). Service không tự chạy lại bằng chứng; đây là xác nhận của con người.
- **Purpose** thuộc một workflow: `WRITE_GATE` (mỗi dataset đúng một purpose, không retire được) hoặc `AUDIT_TABLE` (nhiều purpose). Purpose mới chưa có binding.
- **Binding**: `{expected_binding_version, suite_id, suite_revision_id, policy_revision_id, reason}`, so sánh-và-đặt theo `binding_version` (lệch: 409 `BINDING_CONFLICT`). Service kiểm policy khai báo đúng tập `rule_id` của suite, `QUARANTINE` chỉ cho rule row-level ở stage `CANDIDATE`, đúng workflow, và đủ ngân sách payload. Mỗi thay đổi ghi một `binding_event` (append-only).
- **Quy tắc self-approval**: revision suite **chứa SQL** (`sql_predicate`/`sql_metric`) không được kích hoạt bởi chính người đã tạo nó (403 `SELF_APPROVAL_FORBIDDEN`). Một principal được phép kiêm nhiều vai trò; đây là ràng buộc tự duyệt duy nhất. Revision trùng nội dung do người khác POST vẫn giữ người tạo ban đầu.

## Vòng đời execution

```
POST /executions ──202──▶ execution PLANNED/RUNNING (plan đóng băng)
POST /executions/{E}/attempts ──201──▶ attempt RUNNING (+ đường dẫn candidate raw/classified do server cấp)
POST /executions/{E}/reports ──200──▶ report bất biến + gate
        ├─ WRITE_GATE: DENY  → COMPLETED, destination NOT_APPLICABLE
        │               ALLOW → REPORTED, ghi qua write-outcomes
        └─ AUDIT_TABLE: assessment PASS / VIOLATION → COMPLETED; ERROR (đo bắt buộc lỗi) → FAILED
PUT /executions/{E}/write-outcomes/{quarantine|silver} ──▶ publication
```

- **Execution** (job có grant): server kiểm quyền, restore marker, purpose `ACTIVE` có binding, manifest đã accepted, tham số (tên khai báo, giá trị chỉ `[A-Za-z0-9_:.+ -]`, tối đa 1024 byte), `target_bases`/`input_snapshot` khớp registration, rồi **đóng băng plan**: registration, binding, revision suite/policy, rule đã biên dịch (hash, `resolved_check`, action, required), runtime profile (5 conf Spark), manifest, snapshot base, history anomaly, predicate ghi `dt = DATE '<ngày>'`, retention. Không có cách ghi đè suite/policy/action từ client. Execution không scan, không chạy Spark.
- **Attempt**: ordinal tăng dần, chỉ một attempt `RUNNING`; thay attempt trước khi có report phải khai báo thật `process_stop` của attempt cũ (`CONFIRMED`/`UNCONFIRMED`). Sau khi có report không tạo attempt mới. `attempts/{A}/fail`, `terminate` (trước report), `attempts/{A}/stop-confirmation` (mọi attempt không còn `RUNNING`; xác nhận process thật đã thoát, không suy từ timeout). Chưa `CONFIRMED` thì pin candidate không được nhả.
- **Report**: gửi lại y hệt report đã chấp nhận luôn trả đúng response cũ (cả khi execution đã terminal); report khác sau khi đã chấp nhận: 409. Service kiểm đúng tập rule, `runtime_conf`/`runtime_versions` khớp plan, hash đo, loại giá trị, scope/stage, tham chiếu snapshot do chính server cấp, số đếm nguyên; field thừa hoặc `NaN`/Inf: 422 và **không** có decision nào được tạo. Report không có `status`, `severity`, `message` hay ngưỡng từ client; lỗi runtime chỉ là `client_error` có mã và tham số có giới hạn.
- **Đọc**: `GET /executions/{E}` (plan, attempts, outcome hiện tại), `/decision`, `/report`, `/rules`, `/metrics`, `/anomalies` (202 khi chưa có report); `GET /datasets/{D}/executions` (phân trang, lọc `state`).

## Cách gate quyết định

Logic ở [policy.py](policy.py), so sánh bằng `Decimal`/số nguyên, server tự tính lại `matches` của rule schema. Không có policy mặc định ngầm.

| Trường hợp | Kết quả |
|---|---|
| Rule `BLOCK` vi phạm assertion | `DENY` (ghi) / `VIOLATION` (audit) |
| Rule `OBSERVE` vi phạm | Chỉ cảnh báo (reason `RULE_VIOLATION_OBSERVE`) |
| Rule `QUARANTINE` vi phạm | Không phải veto; dòng vào quarantine, gate xét tổng thể ở dòng dưới |
| Đo lỗi, rule `required: true` | Veto (`REQUIRED_MEASUREMENT_ERROR`) |
| Đo lỗi, rule `required: false` | Cảnh báo, trừ khi phân loại dòng `INCOMPLETE` (vẫn `DENY`) |
| `candidate_rows < min_candidate_rows` | `DENY` (`MIN_CANDIDATE_ROWS`) |
| `valid_rows < min_clean_rows` | `DENY` (`MIN_CLEAN_ROWS`) |
| `quarantine_rows / candidate_rows > max_quarantine_ratio` | `DENY` (`QUARANTINE_RATIO_EXCEEDED`); `candidate_rows = 0` chỉ hợp lệ khi phân loại `COMPLETE` và quarantine 0 |
| Client báo `errors` | Cảnh báo (`CLIENT_ERROR`) |

Audit: không rule nào vi phạm → `PASS`; có rule vi phạm assertion → `VIOLATION`; rule bắt buộc đo lỗi → `ERROR` (execution `FAILED`, lỗi kỹ thuật). Audit không có decision (`GET .../decision` trả 409 `NOT_APPLICABLE`).

Giới hạn kích thước: body tối đa **1 MiB** (413 `PAYLOAD_TOO_LARGE`, đếm cả khi stream không có `Content-Length`). `budget.py` ước lượng kích thước report tối đa của cấu hình; nếu có thể vượt 1 MiB thì activate/tạo plan bị từ chối `INVALID_CONFIG` (field `payload_budget`) ngay từ đầu.

## Kết quả ghi và publication

`PUT /executions/{E}/write-outcomes/{destination}` với body `{state, expected_outcome_version, intent, commit, rows, reason, error, recovery}`, so sánh-và-đặt theo `outcome_version`. Mỗi destination (`quarantine` trước, rồi `silver`) có intent `dq:<execution_id>:<destination>` / version `1`.

| Chuyển | Điều kiện chính |
|---|---|
| `NOT_STARTED → IN_PROGRESS` | Execution `REPORTED` + `ALLOW`, intent đúng, quarantine trước Silver |
| `IN_PROGRESS → SUCCEEDED` (`EXACT`) | Commit khớp bảng, version, `metadata_hash` của nội dung bất biến |
| `IN_PROGRESS → FAILED` | Lỗi chắc chắn **kèm bằng chứng vắng commit**; lỗi mạng/không rõ → `UNKNOWN`, không phải `FAILED` |
| `IN_PROGRESS → UNKNOWN` | Mơ hồ, hoặc operator sau khi writer đã dừng |
| `UNKNOWN → SUCCEEDED` (`EXACT`) | Có commit exact đã xác minh; không ghi lại dữ liệu |
| `UNKNOWN → SUCCEEDED` (`TXN_ONLY`) | Chỉ operator: có token SetTransaction nhưng không đối chiếu được commit; `commit.version`/`metadata_hash` null; **không** cập nhật con trỏ publication |
| `UNKNOWN → FAILED` | Chứng minh vắng mặt trên log còn đủ, token không có, writer đã dừng (`recovery` bắt buộc) |
| `FAILED/UNKNOWN → IN_PROGRESS` | Resume cùng token/nội dung: base vẫn như plan, còn trong `retry_window_seconds`, writer đã dừng |

`NOT_APPLICABLE` chỉ server tự đặt (audit, `DENY`, quarantine 0 dòng); client không đặt được. Request giống hệt trạng thái đã lưu trả kết quả cũ (không tăng version). Sau `CLOSED_UNRESOLVED` mọi cập nhật là 409 `STATE_CONFLICT`.

Trạng thái tổng `publication_state`: `NOT_APPLICABLE`, `SUCCEEDED` (cả hai thành công/không áp dụng), `PARTIAL` (có destination đã commit), `UNKNOWN`, `IN_PROGRESS`, `FAILED`, `NOT_STARTED`. Execution `REPORTED` giữ nguyên khi `FAILED`/`PARTIAL`/`UNKNOWN` để còn phục hồi; chỉ cặp thành công, `abandon` hoặc `close-unresolved` mới đưa tới trạng thái cuối.

- `POST /executions/{E}/publication/abandon`: bỏ cuộc khi không còn `IN_PROGRESS`/`UNKNOWN` (nếu quarantine đã commit thì giữ `PARTIAL`, **không** xoá quarantine).
- `POST /executions/{E}/publication/close-unresolved` (`operator`): đóng các destination `UNKNOWN` khi log hết hạn không còn token (`UNRESOLVABLE_LOG_EXPIRED`) hoặc bằng chứng mâu thuẫn (`CONFLICTING_EVIDENCE`); execution thành `FAILED` + `resolution: CLOSED_UNRESOLVED`, không mở lại. Còn token → 409 `COMMIT_EVIDENCE_PRESENT`.
- `GET /datasets/{D}/publications?dt=YYYY-MM-DD`: `current` (con trỏ chỉ tiến, chỉ cập nhật khi **cả hai** destination đều `EXACT` và Silver đúng version), `pending`, `unresolved_execution_id`, `restore_orphans`.

Con trỏ publication không đảm bảo consumer thấy partition ổn định nếu còn marker chưa giải quyết; luôn đọc kèm `unresolved_execution_id` và `restore_orphans`.

## Restore sau khi khôi phục PostgreSQL

Đặt `DQ_RESTORE_MODE=1` khi khởi động lại service trên bản PostgreSQL đã restore (phải bỏ cờ ở lần khởi động sau, vì cờ còn bật sẽ mở một incident mới sau khi incident cũ đã hoàn tất).

- Khởi động mở **một** marker đối soát cho mỗi dataset (khởi động lặp lại khi marker còn mở không tạo thêm). Khi marker còn mở: ghi execution/attempt/report/outcome của job trả 503 `RESTORE_RECONCILING`; GET vẫn đọc được; operator vẫn thực hiện các chuyển phục hồi.
- `RestoreProbe` ([jvm/dq-jobs](../jvm/dq-jobs/README.md)) quét chỉ-đọc hai bảng đích, sinh các trang findings; `POST /datasets/{D}/restore/findings` nhận từng trang `{scan_id, page, final_page, scheduler_stop_reference, scans, tokens}` (liên tiếp, tối đa 2000 token và 1 MiB mỗi trang; chỉ trang cuối liên tiếp mới tính là phủ đủ). Token thuộc execution đã biết → bằng chứng đối soát; token không biết → marker orphan theo từng partition (hoặc `DATASET` nếu thiếu thông tin partition), không bao giờ thành execution mới.
- `GET /datasets/{D}/restore` xem marker và lịch sử. `POST .../restore/complete` (`{reason, reconcile_reference}`) chỉ thành công khi đủ cả hai scan, không còn destination `IN_PROGRESS`/`UNKNOWN` của execution đã biết, và bảng đích không lùi version so với PostgreSQL (409 nếu không).
- Marker orphan `PARTITION` tự xoá khi có publication exact mới hơn cho partition đó; marker `DATASET` chỉ xoá bằng `POST /datasets/{D}/unresolved/{marker}/clear` (operator, kèm lý do). Không có gì trong Delta bị sửa hay xoá.

## Retention và cleanup

`retention` của registration: `candidate_duration_seconds`, `report_duration_seconds`, `retry_window_seconds` (bắt buộc dương cho WRITE_GATE), `transaction_metadata_duration_seconds`, `cleanup_enabled`. `null` nghĩa là không giới hạn.

- `GET /datasets/{D}/retention` (operator) liệt kê candidate: `{uri, execution_id, attempt_id, retain_until, pinned, reason}`; `pinned` với lý do `CLEANUP_DISABLED`, `EXECUTION_ACTIVE`, `WRITER_UNCONFIRMED`, `REFERENCED`, `NOT_EXPIRED`.
- `Maintenance` ([jvm/dq-jobs](../jvm/dq-jobs/README.md)) chỉ xoá thư mục candidate không pin dưới `candidate_root`; sau đó `POST /datasets/{D}/cleanup-records` ghi vết (từ chối nếu đang pin). Service không xoá metadata hay lịch sử bảng đích và không VACUUM Silver/quarantine.

## Anomaly advisory

Policy `anomaly.enabled: false` thì không gọi worker và không có observation. Khi bật (`max_points`, `lower`, `upper`, `bootstrap`): lúc tạo plan, service chọn và **đóng băng** lịch sử (execution đã publish thành công/audit đã hoàn tất, còn eligible, cùng định nghĩa metric và `history_context`) trong plan; khi nhận report gọi worker **một lần theo lô**, ngoài transaction, timeout tổng 5 giây. Kết quả `NORMAL`/`ANOMALY`/`INSUFFICIENT_HISTORY`/`ERROR` được lưu bất biến cùng report; worker lỗi/chậm chỉ cho observation `ERROR`, **không bao giờ** làm đổi decision. Worker bind `127.0.0.1`, cùng network namespace với service; không có fallback Python.

Eligibility của điểm lịch sử là bảng sự kiện append-only: `POST /executions/{E}/metrics/{O}/eligibility` (`approver`, `{eligible, reason}`) thêm sự kiện, không sửa baseline đã đóng băng của execution khác.

## Alert

Webhook chỉ gửi sau khi report đã commit, nội dung là template an toàn (dataset, execution, decision/assessment, mã reason và rule id); retry y hệt không gửi lại; lỗi webhook chỉ log mã `ALERT_DELIVERY_FAILED`, không rollback.

## Test

```bash
pip install -r dq-service/requirements-dev.txt
python -m pytest dq-service/tests
```

Test tích hợp (`dq-service/tests/integration`) cần PostgreSQL 16 thật và tự skip nếu thiếu biến:

```bash
DQ_TEST_DATABASE_URL=postgresql+psycopg://user:pass@host:5432/postgres \
DQ_TEST_ANOMALY_WORKER_URL=http://127.0.0.1:8081 \
python -m pytest dq-service/tests
```

`DQ_TEST_DATABASE_URL` trỏ tới server mà user có quyền `CREATE DATABASE` (mỗi session dùng database mới); `DQ_TEST_ANOMALY_WORKER_URL` trỏ tới worker JVM thật đang chạy.

## Lưu ý khi sửa

- Thêm `kind`: sửa `models.py` (class check, `ROW_LEVEL_KINDS`, `METRIC_NAMES`), `Measure.scala` và `PlanRules.scala` phía JVM, rồi [docs/RULE_SPEC_V2.md](../docs/RULE_SPEC_V2.md).
- Đổi hành vi đo hoặc canonical hash: cập nhật vector chuẩn ở `jvm/dq-client/src/test/resources/protocol-v3.json`, được cả pytest lẫn ScalaTest đọc.
- Suite lưu với alias `assert`, client Scala đọc đúng dạng đó.
- Dữ liệu bảng `dq_*` cũ nếu còn trong database không được dùng bởi v2 và không bị migration đụng tới.
