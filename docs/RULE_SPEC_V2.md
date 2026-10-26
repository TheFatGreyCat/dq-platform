# Rule spec v3 (`dq-rules/v3`) và policy

Tên file giữ nguyên (`RULE_SPEC_V2.md`); nội dung mô tả định dạng **v3**. Gồm hai tài liệu JSON tách rời, mỗi cái là revision bất biến trong registry của [dq-service](../dq-service/README.md):

- **Suite** (`dq-rules/v3`): *đo cái gì* (rule, scope, stage, `check`).
- **Policy**: *làm gì khi đo xong* (action và `required` cho từng rule, các ngưỡng toàn batch).

Server là nơi duy nhất quyết định đạt/không đạt: client JVM chỉ **đo** và gửi số. Nguồn sự thật của định dạng là [dq-service/models.py](../dq-service/models.py) (kiểm lúc `POST revisions`) và [PlanRules.scala](../jvm/dq-client/src/main/scala/dqplatform/client/PlanRules.scala) / [Measure.scala](../jvm/dq-client/src/main/scala/dqplatform/client/Measure.scala) (khi job chạy). Đổi một bên thì đổi cả hai.

Suite và policy được nạp riêng bằng `POST /api/v2/datasets/{D}/suites/{S}/revisions` và `.../policy-revisions`, rồi gắn vào purpose bằng binding (xem [dq-service](../dq-service/README.md#registry-bất-biến)). [suites/silver_transactions.json](../suites/silver_transactions.json) là envelope mẫu cho script nạp:

```jsonc
{ "dataset": "silver.transactions", "workflow": "WRITE_GATE", "purpose": "default",
  "suite": { /* dq-rules/v3 */ }, "policy": { /* xem Policy */ } }
```

Mọi object đều `extra=forbid`: field lạ bị từ chối 422. Số thực là **chuỗi decimal chuẩn tắc** (`"0.97"`, không `1.0`, không số mũ), số nguyên là JSON int (bool bị từ chối).

## Suite

```jsonc
{
  "format": "dq-rules/v3",
  "workflow": "WRITE_GATE",                  // tuỳ chọn; nếu có phải khớp stage của rule
  "description": "...",
  "parameters": {                            // dữ kiện job phải truyền (tên -> mô tả)
    "partition_start": "đầu ngày dt"
  },
  "rules": [ /* 1..500 rule, xem bên dưới */ ]
}
```

- `rule_id` duy nhất trong suite; `${batch}` là tên dành riêng, không khai trong `parameters`.
- Mọi `${x}` và `{"param": x}` phải được khai trong `parameters`. Tập giá trị job truyền lúc tạo execution phải **đúng bằng** tập đã khai (không thiếu, không thừa).
- Toàn bộ rule cùng một họ stage, khớp workflow (bảng bên dưới).

## Rule

```jsonc
{
  "rule_id": "msisdn_format",       // [a-z][a-z0-9_]*, <= 128; là giá trị trong cột _dq_errors
  "dimension": "validity",          // completeness | uniqueness | validity | accuracy | consistency | timeliness
  "scope": "PARTITION",             // PARTITION | TABLE
  "stage": "CANDIDATE",             // RAW | CANDIDATE | CLEAN | AUDIT_SNAPSHOT
  "description": "...",             // tuỳ chọn
  "check": { "kind": "pattern", "column": "msisdn", "regex": "^84[0-9]{9}$",
             "assert": { "op": ">=", "value": "0.97" } }
}
```

Rule **không** có `severity` hay `row_level`: hậu quả khi vi phạm nằm ở policy.

### Workflow và stage

| Workflow | Stage hợp lệ | Đo trên |
|---|---|---|
| `WRITE_GATE` | `RAW` | Snapshot `raw` của candidate (schema thật đã lưu; dùng cho rule schema) |
| | `CANDIDATE` | Snapshot `classified` của candidate, **mọi dòng**, đã bỏ cột `_dq_*` |
| | `CLEAN` | Phần của `classified` có `_dq_errors` rỗng, đã bỏ cột `_dq_*` |
| `AUDIT_TABLE` | `AUDIT_SNAPSHOT` | Snapshot Silver đã pin (cộng predicate partition nếu audit một partition) |

Một suite chỉ chứa stage của đúng một workflow. Số dòng "sạch" tối thiểu nên đặt bằng rule `size` ở stage `CLEAN` và/hoặc `min_clean_rows` trong policy.

## `assert`

```jsonc
{ "op": ">=", "value": "0.97" }                        // op: >= > <= < ==
{ "op": "between", "min": 10000, "max": 500000 }       // hai đầu đều tính là đạt
{ "op": "==", "value": { "param": "source_row_count" } }   // lấy từ suite parameter
```

Giá trị là decimal chuẩn tắc hoặc int64; `between` có `min <= max` khi cả hai là literal. So sánh bằng `Decimal`/số nguyên ở server. Với `complete`, `range`, `in_set`, bỏ `assert` nghĩa là `== "1"` (mọi dòng phải đạt); `unique`, `pattern`, `sql_predicate` cho phép bỏ `assert` (không có assertion thì không bao giờ vi phạm, chỉ đo).

## Các `kind`

| kind | Field | Đo gì | Metric / kiểu | Row-level |
|---|---|---|---|---|
| `size` | `assert` | Số dòng | `Size` / số nguyên | không |
| `complete` | `column`, `assert`? | Tỉ lệ không null | `Completeness` / decimal | có |
| `completeness` | `column`, `assert` | Tỉ lệ không null | `Completeness` | có |
| `unique` | `columns[]`, `assert`? | Tỉ lệ giá trị (tổ hợp cột) xuất hiện đúng 1 lần | `Uniqueness` | có |
| `pattern` | `column`, `regex`, `assert`? | Tỉ lệ khớp regex trên dòng **không null** | `Compliance` | có |
| `range` | `column`, `min`, `max`, `include_min`?, `include_max`?, `assert`? | Giá trị trong khoảng; null được tính là đạt | `Compliance` | có |
| `in_set` | `column`, `values[]`, `assert`? | Giá trị thuộc tập; null được tính là đạt | `Compliance` | có |
| `mean` | `column`, `assert` | Trung bình | `Mean` | không |
| `sql_predicate` | `predicate`, `columns[]`, `assert`? | Tỉ lệ dòng thỏa điều kiện SQL | `Compliance` | có |
| `sql_metric` | `sql`, `assert` | Một số do câu SQL trả về, đọc từ `${batch}` | `SqlMetric` | không |
| `schema_columns` | `columns[]`, `exact`? | Tập cột thật của bảng (`exact`: không thừa) | schema evidence | không |
| `schema_type` | `column`, `type` | Họ kiểu của cột (`string`, `boolean`, `int`, `long`, `float`, `double`, `decimal`, `date`, `timestamp`) | schema evidence | không |

- Kind **row-level** (cột "có") là những kind được dùng với action `QUARANTINE`.
- Tên metric và kiểu giá trị do **server** quy định (client không chọn). Rule schema không có metric: client gửi bằng chứng typed (`actual_columns` hoặc `column` + `actual_type`) lấy từ schema đã lưu của snapshot; server tự tính `matches`, nên client không thể khai "khớp".
- **Null:** mỗi rule đo một việc; null là việc của rule completeness. `pattern`, `range`, `in_set` bỏ qua null (Deequ coi null trong `hasPattern` là không khớp, client chỉnh lại cho khớp quy ước này). `sql_predicate` phải tự null-safe, ví dụ `event_time IS NULL OR (...)`, vì predicate trả null bị tính là vi phạm. Các hành vi này được ghim bằng [DeequSemanticsSpec](../jvm/dq-client/src/test/scala/dqplatform/client/DeequSemanticsSpec.scala).
- Đo không được (cột không tồn tại, SQL sai, tràn số...) cho kết quả `measurement_status: ERROR` kèm `client_error` có mã; giá trị `OK` nhưng null/NaN/Inf bị từ chối cả report (422).

## Suite parameter

Rule cần dữ kiện của từng lần chạy dùng `${x}` (trong `predicate`/`sql`) hoặc `{"param": "x"}` (trong `assert`, `min`, `max`). Job chỉ truyền **dữ kiện**; ngưỡng nằm trong suite:

```jsonc
"predicate": "event_time IS NULL OR (event_time >= TIMESTAMP '${partition_start}' AND event_time < TIMESTAMP '${partition_end}')"
"sql":       "SELECT CAST(max(event_time) >= TIMESTAMP '${partition_end}' - INTERVAL 60 MINUTES AS DOUBLE) FROM ${batch}"
"assert":    { "op": "==", "value": { "param": "source_row_count" } }
```

- Server thay `${x}` và `{"param"}` **một lần** khi tạo plan (plan chứa giá trị đã thay, kèm `resolved_check` và hash). `${batch}` giữ nguyên là placeholder; SDK bind nó vào một temp view và tên view không đi vào hash.
- Giá trị tối đa 1024 byte, chỉ gồm `[A-Za-z0-9_:.+ -]` (đủ cho ngày giờ và số). Giá trị được ghép thẳng vào SQL nên mọi ký tự khác, kể cả `'` và `\`, bị chặn.
- Regex chạy bằng Java (`java.util.regex`). Service chỉ kiểm cú pháp gần đúng bằng Python `re` và từ chối named group kiểu Python `(?P<name>...)`; cú pháp chỉ Java có (như `\p{L}`) hiện bị từ chối, hãy viết lại bằng lớp ký tự.
- `sql_predicate`/`sql_metric` là Spark SQL chạy trên cluster nên revision chứa chúng **không được kích hoạt bởi chính người tạo**.

```python
# Python
dq.start_write({"dt": "2026-10-17"}, {"partition_start": "2026-10-17 00:00:00", ...}, "2026-10-17T00:00:00Z")
```

```scala
// Scala
dq.startWrite(Map("dt" -> "2026-10-17"), Map("partition_start" -> "2026-10-17 00:00:00", ...), eventTime)
```

## Policy

```jsonc
{
  "rules": [                                   // đúng tập rule_id của suite, mỗi rule một dòng
    { "rule_id": "msisdn_format",       "action": "QUARANTINE", "required": true },
    { "rule_id": "row_count_min",       "action": "BLOCK",      "required": true },
    { "rule_id": "amount_mean",         "action": "OBSERVE",    "required": false }
  ],
  "min_candidate_rows": 1000,                  // mặc định 1
  "min_clean_rows": 1,                         // mặc định 1
  "max_quarantine_ratio": "0.05",              // decimal trong [0, 1]
  "anomaly": { "enabled": false }
}
```

| Action | Khi rule vi phạm assertion |
|---|---|
| `BLOCK` | Veto: `DENY` (ghi) / assessment `VIOLATION` (audit) |
| `QUARANTINE` | Không phải veto riêng. Dòng vi phạm vào quarantine; gate xét tổng bằng `max_quarantine_ratio` và `min_clean_rows`. Chỉ cho rule row-level, `scope: PARTITION`, `stage: CANDIDATE` (service kiểm lúc activate) |
| `OBSERVE` | Chỉ cảnh báo, không ảnh hưởng quyết định |

| `required` | Khi rule **đo không được** |
|---|---|
| `true` | Veto (`DENY`/assessment `ERROR`) |
| `false` | Cảnh báo (`OPTIONAL_MEASUREMENT_ERROR`); riêng phân loại dòng không hoàn tất vẫn `DENY` |

Ngưỡng toàn batch (ghi): `candidate_rows < min_candidate_rows` → `DENY`; `valid_rows < min_clean_rows` → `DENY`; `quarantine_rows / candidate_rows > max_quarantine_ratio` → `DENY`; `candidate_rows = 0` chỉ hợp lệ khi phân loại hoàn tất và quarantine bằng 0 (khi đó ghi một partition rỗng và quarantine không áp dụng). `ALLOW` khi không có veto. Thiếu policy cho dataset thì không có fallback: không có binding thì không tạo được execution. Bảng lý do (`reasons[].code`) ở [dq-service](../dq-service/README.md#cách-gate-quyết-định).

### `anomaly` (advisory)

```jsonc
{ "enabled": true, "max_points": 30, "lower": "0.8", "upper": "1.2", "bootstrap": true }
```

`0 < lower <= upper`, `max_points` 1..200. Với mỗi metric số của rule, worker so `giá trị hiện tại / giá trị liền trước` (chiến lược `RelativeRateOfChangeStrategy` bậc 1 của Deequ) với `[lower, upper]` trên lịch sử đã đóng băng trong plan. Kết quả `NORMAL`, `ANOMALY`, `INSUFFICIENT_HISTORY`, `ERROR` chỉ để tham khảo, **không bao giờ** đổi `ALLOW`/`DENY`. `bootstrap: true` cho phép điểm `INSUFFICIENT_HISTORY` làm lịch sử hợp lệ cho các lần sau. `{ "enabled": false }` thì không gọi worker và không có observation.

## Hash

Mỗi rule trong plan mang hai hash: `metric_definition_hash` (khuôn, không gồm giá trị tham số thật; khoá chuỗi lịch sử anomaly) và `measurement_definition_hash` (đã thay tham số). Cả hai bỏ `rule_id`, assertion, action, `required`, nên đổi ngưỡng không cắt đứt chuỗi lịch sử. Report phải mang đúng hash của plan; hash lệch bị từ chối.

## Kết quả gửi lên service

Mỗi rule một object đủ khoá (khoá không áp dụng là `null`):

```jsonc
{ "rule_id": "msisdn_format", "measurement_definition_hash": "sha256:...", "scope": "PARTITION", "stage": "CANDIDATE",
  "kind": "pattern", "measurement_status": "OK", "metric_name": "Compliance", "metric_value": "0.9744",
  "schema_evidence": null, "error": null, "failed_rows": null }
```

`measurement_status` chỉ có `OK`/`ERROR`. Không có `status` đạt/không, `severity`, `threshold`, `message`. Hợp đồng đầy đủ của report, plan và outcome ở [dq-service](../dq-service/README.md#vòng-đời-execution); vector hash chuẩn tắc dùng chung Python/Scala ở `jvm/dq-client/src/test/resources/protocol-v3.json`.
