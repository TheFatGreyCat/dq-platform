"""
Wire objects v3 (suite, policy, registration, execution, report, outcome, restore).

Mọi object `extra=forbid`. Số thực là chuỗi decimal canonical, số nguyên là int (bool bị từ chối).
Client JVM (jvm/dq-client Protocol.scala) diễn giải cùng định dạng -> hai bên đổi cùng nhau.
"""
from __future__ import annotations

import re
from decimal import Decimal
from typing import Annotated, Literal, Union

from pydantic import (AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field, StrictBool, StrictInt,
                      field_validator, model_validator)

from errors import ClientError

FORMAT = "dq-rules/v3"
MANIFEST_FORMAT = "dq-compat-v1"
DIMENSIONS = ("completeness", "uniqueness", "validity", "accuracy", "consistency", "timeliness")
Dimension = Literal["completeness", "uniqueness", "validity", "accuracy", "consistency", "timeliness"]
Workflow = Literal["WRITE_GATE", "AUDIT_TABLE"]
Scope = Literal["PARTITION", "TABLE"]
Stage = Literal["RAW", "CANDIDATE", "CLEAN", "AUDIT_SNAPSHOT"]
WORKFLOW_STAGES = {"WRITE_GATE": frozenset({"RAW", "CANDIDATE", "CLEAN"}), "AUDIT_TABLE": frozenset({"AUDIT_SNAPSHOT"})}
Action = Literal["BLOCK", "QUARANTINE", "OBSERVE"]

# kind có kết quả theo dòng -> được phép action QUARANTINE
ROW_LEVEL_KINDS = frozenset({"complete", "completeness", "unique", "pattern", "range", "in_set", "sql_predicate"})
METRIC_NAMES = {"size": "Size", "complete": "Completeness", "completeness": "Completeness", "unique": "Uniqueness",
                "pattern": "Compliance", "range": "Compliance", "in_set": "Compliance", "sql_predicate": "Compliance",
                "mean": "Mean", "sql_metric": "SqlMetric"}
# parameter dành riêng: client tự điền tên temp view của batch
BATCH_PARAM = "batch"
RUNTIME_CONF = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.ansi.enabled": "false",
    "spark.sql.caseSensitive": "false",
    "spark.sql.legacy.timeParserPolicy": "EXCEPTION",
    "spark.sql.sources.partitionOverwriteMode": "static",
}
INT64_MAX = 2**63 - 1
_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_DECIMAL = r"^-?(0|[1-9][0-9]*)(\.[0-9]*[1-9])?$"
_NAME = r"^[A-Za-z_][A-Za-z0-9_]*$"
_UUID = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
_HASH = r"^sha256:[0-9a-f]{64}$"


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


# ---------- scalar types ----------
def _not_neg_zero(v: str) -> str:
    if v == "-0":
        raise ValueError("decimal canonical không có -0")
    return v


Dec = Annotated[str, Field(pattern=_DECIMAL, max_length=384), AfterValidator(_not_neg_zero)]
Int64 = Annotated[StrictInt, Field(ge=-(2**63), le=INT64_MAX)]
Count = Annotated[StrictInt, Field(ge=0, le=INT64_MAX)]
Uuid = Annotated[str, Field(pattern=_UUID)]
Hash = Annotated[str, Field(pattern=_HASH)]
Ident = Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,128}$")]
RuleId = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=128)]
Text = Annotated[str, Field(max_length=2048)]
Reason = Annotated[str, Field(min_length=1, max_length=2048)]
Reference = Annotated[str, Field(min_length=1, max_length=2048)]
Column = Annotated[str, Field(min_length=1, max_length=128)]
Snippet = Annotated[str, Field(min_length=1, max_length=4096)]


def _dec(v: str) -> Decimal:
    return Decimal(v)


# ---------- assertion ----------
class ParamRef(_Model):
    param: Annotated[str, Field(pattern=_NAME, max_length=128)]


Bound = Union[Dec, Int64, ParamRef]


def _literal(b: Bound) -> Decimal | None:
    return None if isinstance(b, ParamRef) else Decimal(b)


class Assertion(_Model):
    op: Literal[">=", ">", "<=", "<", "==", "between"]
    value: Bound | None = None
    min: Bound | None = None
    max: Bound | None = None

    @model_validator(mode="after")
    def _shape(self) -> "Assertion":
        if self.op == "between":
            if self.min is None or self.max is None or self.value is not None:
                raise ValueError("op 'between' chỉ nhận 'min' và 'max'")
            lo, hi = _literal(self.min), _literal(self.max)
            if lo is not None and hi is not None and lo > hi:
                raise ValueError("between: min > max")
        elif self.value is None or self.min is not None or self.max is not None:
            raise ValueError(f"op '{self.op}' chỉ nhận 'value'")
        return self

    def param_refs(self) -> set[str]:
        return {b.param for b in (self.value, self.min, self.max) if isinstance(b, ParamRef)}


def _default_assertion() -> Assertion:
    return Assertion(op="==", value="1")


# ---------- check theo kind ----------
class SizeCheck(_Model):
    kind: Literal["size"]
    assert_: Assertion = Field(alias="assert")


class CompleteCheck(_Model):
    kind: Literal["complete"]
    column: Column
    assert_: Assertion = Field(default_factory=_default_assertion, alias="assert")


class CompletenessCheck(_Model):
    kind: Literal["completeness"]
    column: Column
    assert_: Assertion = Field(alias="assert")


class UniqueCheck(_Model):
    kind: Literal["unique"]
    columns: list[Column] = Field(min_length=1, max_length=100)
    assert_: Assertion | None = Field(default=None, alias="assert")


class PatternCheck(_Model):
    kind: Literal["pattern"]
    column: Column
    regex: Snippet
    assert_: Assertion | None = Field(default=None, alias="assert")

    @field_validator("regex")
    @classmethod
    def _compiles(cls, v: str) -> str:
        # regex do Java thực thi; Python re chỉ kiểm tra gần đúng. Chặn cú pháp chỉ Python có.
        if "(?P" in v:
            raise ValueError("named group dạng (?P<name>...) là cú pháp Python; Java dùng (?<name>...)")
        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(f"regex không hợp lệ: {e}") from e
        return v


class RangeCheck(_Model):
    kind: Literal["range"]
    column: Column
    min: Bound
    max: Bound
    include_min: StrictBool = True
    include_max: StrictBool = True
    assert_: Assertion = Field(default_factory=_default_assertion, alias="assert")

    @model_validator(mode="after")
    def _ordered(self) -> "RangeCheck":
        lo, hi = _literal(self.min), _literal(self.max)
        if lo is not None and hi is not None and lo > hi:
            raise ValueError("range: min > max")
        return self


class InSetCheck(_Model):
    kind: Literal["in_set"]
    column: Column
    values: list[Annotated[str, Field(max_length=256)]] = Field(min_length=1, max_length=1000)
    assert_: Assertion = Field(default_factory=_default_assertion, alias="assert")


class MeanCheck(_Model):
    kind: Literal["mean"]
    column: Column
    assert_: Assertion = Field(alias="assert")


class SqlPredicateCheck(_Model):
    kind: Literal["sql_predicate"]
    predicate: Snippet
    columns: list[Column] = Field(min_length=1, max_length=100)
    assert_: Assertion | None = Field(default=None, alias="assert")


class SqlMetricCheck(_Model):
    kind: Literal["sql_metric"]
    sql: Snippet
    assert_: Assertion = Field(alias="assert")

    @field_validator("sql")
    @classmethod
    def _reads_batch(cls, v: str) -> str:
        if "${" + BATCH_PARAM + "}" not in v:
            raise ValueError("sql_metric phải đọc từ ${batch} (temp view của batch đang validate)")
        return v


class SchemaColumnsCheck(_Model):
    kind: Literal["schema_columns"]
    columns: list[Column] = Field(min_length=1, max_length=1000)
    exact: StrictBool = False


class SchemaTypeCheck(_Model):
    kind: Literal["schema_type"]
    column: Column
    type: Literal["string", "boolean", "int", "long", "float", "double", "decimal", "date", "timestamp"]


Check = Annotated[
    Union[SizeCheck, CompleteCheck, CompletenessCheck, UniqueCheck, PatternCheck, RangeCheck, InSetCheck,
          MeanCheck, SqlPredicateCheck, SqlMetricCheck, SchemaColumnsCheck, SchemaTypeCheck],
    Field(discriminator="kind"),
]


def check_param_refs(check: BaseModel) -> set[str]:
    refs: set[str] = set()
    a = getattr(check, "assert_", None)
    if a is not None:
        refs |= a.param_refs()
    for b in (getattr(check, "min", None), getattr(check, "max", None)):
        if isinstance(b, ParamRef):
            refs.add(b.param)
    for text in (getattr(check, "predicate", None), getattr(check, "sql", None)):
        if text:
            refs |= set(_PLACEHOLDER.findall(text))
    return refs


# ---------- suite ----------
class Rule(_Model):
    rule_id: RuleId
    dimension: Dimension
    check: Check
    scope: Scope
    stage: Stage
    description: Text | None = None


class Suite(_Model):
    format: Literal["dq-rules/v3"]
    workflow: Workflow | None = None
    description: Text | None = None
    parameters: dict[Annotated[str, Field(pattern=_NAME, max_length=128)], Text] = Field(default_factory=dict, max_length=100)
    rules: list[Rule] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def _consistent(self) -> "Suite":
        ids = [r.rule_id for r in self.rules]
        dup = sorted({i for i in ids if ids.count(i) > 1})
        if dup:
            raise ValueError(f"rule_id bị trùng: {', '.join(dup)}")
        if BATCH_PARAM in self.parameters:
            raise ValueError(f"'{BATCH_PARAM}' là parameter dành riêng")
        families = {w for w, st in WORKFLOW_STAGES.items() for r in self.rules if r.stage in st}
        if len(families) != 1 or (self.workflow is not None and families != {self.workflow}):
            raise ValueError("stage của rule không khớp workflow (WRITE_GATE: RAW/CANDIDATE/CLEAN; AUDIT_TABLE: AUDIT_SNAPSHOT)")
        for r in self.rules:
            undeclared = check_param_refs(r.check) - set(self.parameters) - {BATCH_PARAM}
            if undeclared:
                raise ValueError(f"rule {r.rule_id} dùng parameter chưa khai báo: {', '.join(sorted(undeclared))}")
        return self

    @property
    def contains_sql(self) -> bool:
        return any(r.check.kind in ("sql_predicate", "sql_metric") for r in self.rules)


# ---------- policy ----------
class PolicyRule(_Model):
    rule_id: RuleId
    action: Action
    required: StrictBool


class AnomalyDisabled(_Model):
    enabled: Literal[False]


class AnomalyEnabled(_Model):
    enabled: Literal[True]
    max_points: Annotated[StrictInt, Field(ge=1, le=200)]
    lower: Dec
    upper: Dec
    bootstrap: StrictBool

    @model_validator(mode="after")
    def _bounds(self) -> "AnomalyEnabled":
        if not (0 < _dec(self.lower) <= _dec(self.upper)):
            raise ValueError("anomaly: cần 0 < lower <= upper")
        return self


class Policy(_Model):
    rules: list[PolicyRule] = Field(max_length=500)
    min_candidate_rows: Count = 1
    min_clean_rows: Count = 1
    max_quarantine_ratio: Dec
    anomaly: Union[AnomalyDisabled, AnomalyEnabled]

    @field_validator("max_quarantine_ratio")
    @classmethod
    def _ratio(cls, v: str) -> str:
        if not (0 <= _dec(v) <= 1):
            raise ValueError("max_quarantine_ratio phải trong [0,1]")
        return v

    @model_validator(mode="after")
    def _unique(self) -> "Policy":
        ids = [r.rule_id for r in self.rules]
        if len(set(ids)) != len(ids):
            raise ValueError("rule_id bị trùng trong policy")
        return self


def check_policy_for_suite(suite: Suite, policy: Policy) -> None:
    """Tương thích suite-policy lúc activate: ID khớp tuyệt đối; QUARANTINE chỉ rule row-level stage CANDIDATE."""
    by_id = {r.rule_id: r for r in suite.rules}
    if {p.rule_id for p in policy.rules} != set(by_id):
        raise ValueError("policy phải khai báo đúng tập rule_id của suite")
    for p in policy.rules:
        r = by_id[p.rule_id]
        if p.action == "QUARANTINE" and not (r.check.kind in ROW_LEVEL_KINDS and r.stage == "CANDIDATE"
                                             and r.scope == "PARTITION"):
            raise ValueError(f"rule {p.rule_id}: QUARANTINE chỉ cho rule row-level ở stage CANDIDATE")


def check_parameter_values(suite: Suite, values: dict[str, str]) -> None:
    """Giá trị truyền vào phải đúng tập tham số đã khai báo."""
    if set(values) != set(suite.parameters):
        raise ValueError("parameters phải đúng tập đã khai báo")


class SeedFile(_Model):
    """suites/*.json: envelope cho `dq_operator.py seed`, không phải startup seed."""
    dataset: Ident
    workflow: Workflow
    purpose: Ident
    suite: Suite
    policy: Policy


# ---------- snapshot / registration ----------
_URI = re.compile(r"^(s3a|s3|file)://([^/@?#\\]*)(/[^?#\\]*)?$", re.IGNORECASE)


def _check_uri(v: str) -> str:
    m = _URI.match(v)
    if not m or any(ord(c) < 32 or ord(c) == 127 for c in v):
        raise ValueError("URI không hợp lệ (cần s3a/s3/file, không credentials/query/fragment)")
    path = m.group(3) or "/"
    low = path.lower()
    if re.search(r"%(2e|2f|5c|00)", low) or any(seg in ("..", ".") for seg in path.split("/")):
        raise ValueError("URI chứa traversal hoặc ký tự mã hoá cấm")
    if "//" in path:
        raise ValueError("URI chứa segment rỗng")
    return v


Uri = Annotated[str, Field(max_length=2048), AfterValidator(_check_uri)]


class SnapshotRef(_Model):
    uri: Uri
    table_id: Uuid
    version: Count
    schema_hash: Hash
    set_transaction_retention: None = None


class TableRef(_Model):
    uri: Uri
    table_id: Uuid
    schema_hash: Hash
    partition_schema_hash: Hash
    set_transaction_retention: None = None


class SchemaColumn(_Model):
    name: Column
    type: Annotated[str, Field(min_length=1, max_length=128)]
    nullable: StrictBool


class PartitionColumn(SchemaColumn):
    name: Literal["dt"]
    type: Literal["date"]


class Retention(_Model):
    candidate_duration_seconds: Annotated[StrictInt, Field(gt=0, le=INT64_MAX)] | None
    report_duration_seconds: Annotated[StrictInt, Field(gt=0, le=INT64_MAX)] | None
    retry_window_seconds: Annotated[StrictInt, Field(gt=0, le=INT64_MAX)] | None
    transaction_metadata_duration_seconds: Annotated[StrictInt, Field(gt=0, le=INT64_MAX)] | None
    cleanup_enabled: StrictBool


def _under(uri: str, root: str) -> bool:
    u, r = uri.lower().rstrip("/"), root.lower().rstrip("/")
    return u == r or u.startswith(r + "/")


class Registration(_Model):
    owner: Annotated[str, Field(min_length=1, max_length=128)]
    silver: TableRef
    quarantine: TableRef
    candidate_root: Uri
    allowed_storage_roots: list[Uri] = Field(min_length=1, max_length=20)
    business_schema: list[SchemaColumn] = Field(min_length=1, max_length=1000)
    partition_schema: list[PartitionColumn] = Field(min_length=1, max_length=1)
    history_context: Ident
    retention: Retention

    @model_validator(mode="after")
    def _consistent(self) -> "Registration":
        folded = [c.name.lower() for c in self.business_schema]
        if len(set(folded)) != len(folded):
            raise ValueError("business_schema có tên cột trùng sau case-fold")
        if any(n.startswith("_dq_") for n in folded):
            raise ValueError("business_schema không được dùng cột dành riêng _dq_*")
        if self.silver.table_id == self.quarantine.table_id or self.silver.uri.lower() == self.quarantine.uri.lower():
            raise ValueError("silver và quarantine phải là hai bảng khác nhau")
        for uri in (self.silver.uri, self.quarantine.uri, self.candidate_root):
            if not any(_under(uri, root) for root in self.allowed_storage_roots):
                raise ValueError("URI nằm ngoài allowed_storage_roots")
        return self


# ---------- execution ----------
def _param_bytes(v: str) -> str:
    if len(v.encode()) > 1024:
        raise ValueError("quá 1024 bytes")
    return v


SafeValue = Annotated[str, Field(pattern=r"^[A-Za-z0-9_:.+ -]*$"), AfterValidator(_param_bytes)]
Parameters = dict[Annotated[str, Field(pattern=_NAME, max_length=128)], SafeValue]


class PartitionValues(_Model):
    dt: Annotated[str, Field(pattern=r"^\d{4}-\d{2}-\d{2}$")]

    @field_validator("dt")
    @classmethod
    def _real_date(cls, v: str) -> str:
        from datetime import date
        date.fromisoformat(v)
        return v


class TargetBases(_Model):
    silver: SnapshotRef
    quarantine: SnapshotRef


class ExecutionRequest(_Model):
    dataset_id: Ident
    workflow: Workflow
    purpose: Ident
    event_time: AwareDatetime
    parameters: Parameters = Field(max_length=100)
    history_context: Ident
    partition_values: PartitionValues | None
    input_snapshot: SnapshotRef | None
    target_bases: TargetBases | None
    compat_manifest_id: Uuid
    previous_execution_id: Uuid | None = None
    event_origin: Literal["SCHEDULED", "BACKFILL", "REPAIR"] = "SCHEDULED"

    @model_validator(mode="after")
    def _by_workflow(self) -> "ExecutionRequest":
        if self.workflow == "WRITE_GATE":
            if self.partition_values is None or self.target_bases is None or self.input_snapshot is not None:
                raise ValueError("WRITE_GATE cần partition_values + target_bases và không có input_snapshot")
        elif self.input_snapshot is None or self.target_bases is not None:
            raise ValueError("AUDIT_TABLE cần input_snapshot và không có target_bases")
        return self


class ProcessStop(_Model):
    confirmed_stopped: StrictBool
    reference: Reference | None = None

    @model_validator(mode="after")
    def _ref(self) -> "ProcessStop":
        if self.confirmed_stopped and not (self.reference and self.reference.strip()):
            raise ValueError("confirmed_stopped=true cần reference")
        return self


ErrorCategory = Annotated[str, Field(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")]


class AttemptCreate(_Model):
    runner_identity: Annotated[str, Field(min_length=1, max_length=128)]
    image_digest: Annotated[str, Field(max_length=128)] | None = None
    previous_attempt_id: Uuid | None = None
    previous_stop: ProcessStop | None = None


class AttemptFail(_Model):
    error_category: ErrorCategory
    error: ClientError
    process_stop: ProcessStop


class TerminateRequest(_Model):
    state: Literal["FAILED", "CANCELLED"]
    reason: Reason
    error: ClientError | None = None
    process_stop: ProcessStop | None = None


class StopConfirmation(_Model):
    reference: Reference


# ---------- report ----------
class RuntimeVersions(_Model):
    spark_version: Annotated[str, Field(min_length=1, max_length=64)]
    scala_version: Annotated[str, Field(min_length=1, max_length=64)]
    java_major: Annotated[StrictInt, Field(ge=1, le=1000)]
    hadoop_version: Annotated[str, Field(min_length=1, max_length=64)]
    jars: "Jars"


class Jars(_Model):
    delta_spark: Hash
    delta_storage: Hash
    hadoop_aws: Hash
    deequ: Hash
    dq_client: Hash


RuntimeVersions.model_rebuild()


class CompatManifest(RuntimeVersions):
    format: Literal["dq-compat-v1"]


class SchemaColumnsEvidence(_Model):
    actual_columns: list[Column] = Field(max_length=1000)
    matches: StrictBool


class SchemaTypeEvidence(_Model):
    column: Column
    actual_type: Annotated[str, Field(min_length=1, max_length=128)]
    matches: StrictBool


class MeasurementResult(_Model):
    rule_id: RuleId
    measurement_definition_hash: Hash
    scope: Scope
    stage: Stage
    kind: Literal["size", "complete", "completeness", "unique", "pattern", "range", "in_set", "mean",
                  "sql_predicate", "sql_metric", "schema_columns", "schema_type"]
    measurement_status: Literal["OK", "ERROR"]
    metric_name: Literal["Size", "Completeness", "Uniqueness", "Compliance", "Mean", "SqlMetric"] | None = None
    metric_value: Union[Int64, Dec, None] = None
    schema_evidence: Union[SchemaColumnsEvidence, SchemaTypeEvidence, None] = None
    error: ClientError | None = None
    failed_rows: Count | None = None

    @model_validator(mode="after")
    def _shape(self) -> "MeasurementResult":
        numeric = (self.metric_name, self.metric_value)
        if self.measurement_status == "ERROR":
            if self.error is None or any(v is not None for v in (*numeric, self.schema_evidence)):
                raise ValueError("kết quả ERROR chỉ mang error")
        elif self.error is not None:
            raise ValueError("kết quả OK không mang error")
        elif self.kind in ("schema_columns", "schema_type"):
            want = SchemaColumnsEvidence if self.kind == "schema_columns" else SchemaTypeEvidence
            if not isinstance(self.schema_evidence, want) or any(v is not None for v in numeric):
                raise ValueError("kết quả schema chỉ mang schema_evidence đúng kind")
        else:
            if self.schema_evidence is not None or self.metric_name != METRIC_NAMES[self.kind] or self.metric_value is None:
                raise ValueError("kết quả số cần đúng metric_name của kind và metric_value")
            if isinstance(self.metric_value, int) != (self.metric_name == "Size"):
                raise ValueError("Size là integer, metric còn lại là decimal string")
        return self


class _ReportCommon(_Model):
    attempt_id: Uuid
    dataset_id: Ident
    plan_hash: Hash
    suite_revision_id: Uuid
    policy_revision_id: Uuid
    engine: Annotated[str, Field(min_length=1, max_length=128)]
    interpreter_version: Annotated[str, Field(min_length=1, max_length=128)]
    runtime_conf: dict[str, str]
    runtime_versions: RuntimeVersions
    parameters: Parameters = Field(max_length=100)
    results: list[MeasurementResult] = Field(min_length=1, max_length=500)

    @field_validator("runtime_conf")
    @classmethod
    def _conf(cls, v: dict[str, str]) -> dict[str, str]:
        if v != RUNTIME_CONF:
            raise ValueError("runtime_conf phải đúng 5 key profile")
        return v

    @field_validator("results")
    @classmethod
    def _unique_results(cls, v: list[MeasurementResult]) -> list[MeasurementResult]:
        ids = [r.rule_id for r in v]
        if len(set(ids)) != len(ids):
            raise ValueError("rule_id kết quả bị trùng")
        return v


class WriteReport(_ReportCommon):
    raw_reference: SnapshotRef
    classified_reference: SnapshotRef | None
    business_schema_hash: Hash
    classified_schema_hash: Hash | None
    classification_status: Literal["COMPLETE", "INCOMPLETE"]
    candidate_rows: Count
    valid_rows: Count | None
    quarantine_rows: Count | None
    clean_selector: Literal["size(_dq_errors)=0"] | None
    quarantine_selector: Literal["size(_dq_errors)>0"] | None
    errors: list[ClientError] = Field(max_length=50)

    @model_validator(mode="after")
    def _classification(self) -> "WriteReport":
        complete = (self.classified_reference, self.classified_schema_hash, self.valid_rows, self.quarantine_rows,
                    self.clean_selector, self.quarantine_selector)
        if self.classification_status == "COMPLETE":
            if any(v is None for v in complete):
                raise ValueError("COMPLETE cần classified_reference, schema hash, counts và selectors")
            if self.valid_rows + self.quarantine_rows != self.candidate_rows:
                raise ValueError("valid_rows + quarantine_rows phải bằng candidate_rows")
        else:
            if not self.errors:
                raise ValueError("INCOMPLETE cần errors")
            if (self.classified_reference is None) != (self.classified_schema_hash is None):
                raise ValueError("classified_reference và classified_schema_hash đi cùng nhau")
            if (self.valid_rows is None) != (self.quarantine_rows is None):
                raise ValueError("valid_rows và quarantine_rows đi cùng nhau")
        return self


class AuditReport(_ReportCommon):
    audit_reference: SnapshotRef
    measured_rows: Count
    classification_status: Literal["NOT_APPLICABLE"]


# ---------- outcome ----------
class Intent(_Model):
    txn_app_id: Annotated[str, Field(pattern=r"^dq:[0-9a-f-]{36}:(silver|quarantine)$")]
    txn_version: Literal[1]


class Commit(_Model):
    table_id: Uuid
    version: Count | None
    metadata_hash: Hash | None
    evidence: Literal["EXACT", "TXN_ONLY"]

    @model_validator(mode="after")
    def _evidence(self) -> "Commit":
        exact = self.evidence == "EXACT"
        if exact != (self.version is not None and self.metadata_hash is not None) or \
                (not exact and (self.version is not None or self.metadata_hash is not None)):
            raise ValueError("EXACT cần version+metadata_hash; TXN_ONLY cả hai null")
        return self


class Absence(_Model):
    table_id: Uuid
    checked_through_version: Count


class TxnEvidence(_Model):
    table_id: Uuid
    snapshot_version: Count
    txn_app_id: Annotated[str, Field(pattern=r"^dq:[0-9a-f-]{36}:(silver|quarantine)$")]
    txn_version: Count
    last_updated: Int64 | None = None


class Recovery(_Model):
    absence: Absence | None = None
    current_base_version: Count
    writer_stop: ProcessStop | None = None
    txn_evidence: TxnEvidence | None = None


class OutcomeRequest(_Model):
    state: Literal["IN_PROGRESS", "SUCCEEDED", "FAILED", "UNKNOWN"]
    expected_outcome_version: Count
    intent: Intent | None = None
    commit: Commit | None = None
    rows: Count | None = None
    reason: Reason | None = None
    error: ClientError | None = None
    recovery: Recovery | None = None

    @model_validator(mode="after")
    def _by_state(self) -> "OutcomeRequest":
        if (self.state == "SUCCEEDED") != (self.commit is not None):
            raise ValueError("commit chỉ có (và bắt buộc) khi state=SUCCEEDED")
        if self.state == "IN_PROGRESS" and self.intent is None:
            raise ValueError("IN_PROGRESS cần intent")
        if self.state == "FAILED" and self.error is None:
            raise ValueError("FAILED cần error")
        if self.commit is not None and self.commit.evidence == "TXN_ONLY":
            r = self.recovery
            if r is None or r.txn_evidence is None or r.writer_stop is None or not r.writer_stop.confirmed_stopped \
                    or self.reason is None:
                raise ValueError("TXN_ONLY cần recovery.txn_evidence, writer_stop xác nhận và reason")
        return self


class AbandonRequest(_Model):
    reason: Reason
    error: ClientError | None = None


class CloseEvidence(_Model):
    destination: Literal["silver", "quarantine"]
    table_id: Uuid
    snapshot_version: Count
    checked_through_version: Count | None
    txn_present: StrictBool


class ConfirmedStop(ProcessStop):
    confirmed_stopped: Literal[True]
    reference: Reference


class PublicationClose(_Model):
    reason_code: Literal["UNRESOLVABLE_LOG_EXPIRED", "CONFLICTING_EVIDENCE"]
    reason: Reason
    writer_stop: ConfirmedStop
    evidence: list[CloseEvidence] = Field(min_length=1, max_length=2)

    @field_validator("evidence")
    @classmethod
    def _distinct(cls, v: list[CloseEvidence]) -> list[CloseEvidence]:
        if len({e.destination for e in v}) != len(v):
            raise ValueError("evidence trùng destination")
        return v


# ---------- restore ----------
class RestoreScan(_Model):
    destination: Literal["silver", "quarantine"]
    table_id: Uuid
    snapshot_version: Count


class RestoreToken(_Model):
    destination: Literal["silver", "quarantine"]
    txn_app_id: Annotated[str, Field(min_length=1, max_length=256)]
    txn_version: Count
    commit_version: Count | None
    partition_values: PartitionValues | None


class RestoreFindings(_Model):
    scan_id: Uuid
    page: Count
    final_page: StrictBool
    scheduler_stop_reference: Reference
    scans: list[RestoreScan] = Field(min_length=2, max_length=2)
    tokens: list[RestoreToken] = Field(max_length=2000)

    @field_validator("scans")
    @classmethod
    def _both(cls, v: list[RestoreScan]) -> list[RestoreScan]:
        if {s.destination for s in v} != {"silver", "quarantine"}:
            raise ValueError("scans phải có đủ silver và quarantine")
        return v


class ClearRequest(_Model):
    reason: Reason
    reconcile_reference: Reference


class RestoreComplete(ClearRequest):
    pass


# ---------- registry requests ----------
class SuiteCreate(_Model):
    name: Annotated[str, Field(min_length=1, max_length=128)]
    workflow: Workflow
    description: Text | None


class RevisionValidationRequest(_Model):
    suite: Suite
    policy: Policy | None = None


class PurposeRegister(_Model):
    description: Text | None


class BindingRequest(_Model):
    expected_binding_version: Count
    suite_id: Uuid
    suite_revision_id: Uuid
    policy_revision_id: Uuid
    reason: Reason


class RetireRequest(_Model):
    expected_binding_version: Count
    reason: Reason


class ManifestAcceptance(_Model):
    harness_reference: Reference


class EligibilityRequest(_Model):
    eligible: StrictBool
    reason: Reason


class CleanupRecord(_Model):
    """Operator attestation that an expired, unpinned candidate directory was removed by maintenance."""
    execution_id: Uuid
    attempt_id: Uuid
    uri: Uri
    stop_reference: Reference
