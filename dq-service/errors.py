"""
Mã lỗi API + client_error có kiểu. Mọi message là template cố định: không bao giờ chứa input của client,
exception gốc hay giá trị dòng dữ liệu.
"""
from __future__ import annotations

import re
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_serializer

# ---------- API error codes (phụ lục B) ----------
STATUS: dict[str, int] = {
    "UNAUTHENTICATED": 401,
    "FORBIDDEN": 403,
    "SELF_APPROVAL_FORBIDDEN": 403,
    "NOT_FOUND": 404,
    **{c: 409 for c in (
        "RUNTIME_MISMATCH", "CONTENT_CONFLICT", "PLAN_MISMATCH", "ATTEMPT_CONFLICT", "STATE_CONFLICT",
        "BASE_CHANGED", "RETRY_WINDOW_EXPIRED", "WRITER_UNCONFIRMED", "OUTCOME_UNRESOLVED",
        "TARGET_NOT_INITIALIZED", "COMMIT_EVIDENCE_PRESENT", "BINDING_CONFLICT", "PURPOSE_RETIRED",
        "NOT_APPLICABLE")},
    "PAYLOAD_TOO_LARGE": 413,
    "INVALID_CONFIG": 422,
    "INVALID_REPORT": 422,
    "RESTORE_RECONCILING": 503,
    "SERVICE_UNAVAILABLE": 503,
}

MESSAGES: dict[str, str] = {
    "UNAUTHENTICATED": "Thiếu hoặc sai thông tin xác thực.",
    "FORBIDDEN": "Không đủ quyền cho thao tác này.",
    "SELF_APPROVAL_FORBIDDEN": "Người tạo revision có SQL không được tự kích hoạt.",
    "NOT_FOUND": "Không tìm thấy tài nguyên.",
    "RUNTIME_MISMATCH": "Runtime của client khác runtime đã pin trong plan.",
    "CONTENT_CONFLICT": "Nội dung xung đột với bản đã lưu.",
    "PLAN_MISMATCH": "Report không khớp plan đã pin.",
    "ATTEMPT_CONFLICT": "Attempt không còn là attempt đang hoạt động.",
    "STATE_CONFLICT": "Trạng thái hiện tại không cho phép thao tác.",
    "BASE_CHANGED": "Base của destination đã đổi so với plan.",
    "RETRY_WINDOW_EXPIRED": "Đã quá cửa sổ retry; chỉ được reconcile hoặc đóng.",
    "WRITER_UNCONFIRMED": "Chưa xác nhận writer cũ đã dừng.",
    "OUTCOME_UNRESOLVED": "Outcome ghi còn chưa xác định.",
    "TARGET_NOT_INITIALIZED": "Bảng đích chưa được khởi tạo.",
    "COMMIT_EVIDENCE_PRESENT": "Vẫn còn bằng chứng commit; không thể đóng như không có commit.",
    "BINDING_CONFLICT": "binding_version không khớp.",
    "PURPOSE_RETIRED": "Purpose đã retired.",
    "NOT_APPLICABLE": "Không áp dụng cho workflow/trạng thái này.",
    "PAYLOAD_TOO_LARGE": "Body vượt giới hạn 1 MiB.",
    "INVALID_CONFIG": "Cấu hình hoặc request không hợp lệ.",
    "INVALID_REPORT": "Report không hợp lệ.",
    "RESTORE_RECONCILING": "Dataset đang đối soát sau restore; thao tác ghi tạm bị chặn.",
    "SERVICE_UNAVAILABLE": "Dịch vụ tạm thời không khả dụng.",
}
assert STATUS.keys() == MESSAGES.keys()

_SAFE_SEGMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_FIELD_MAX = 256


def safe_field(field: str | None) -> str | None:
    """Field path chỉ gồm tên định danh/chỉ số; segment lạ thành '*', không bao giờ echo input."""
    if field is None:
        return None
    parts = [p if (_SAFE_SEGMENT.match(p) or p.isdigit()) else "*" for p in field.split(".")]
    return ".".join(parts)[:_FIELD_MAX]


class ApiError(Exception):
    def __init__(self, code: str, field: str | None = None):
        super().__init__(code)
        self.code = code
        self.status = STATUS[code]
        self.message = MESSAGES[code]
        self.field = safe_field(field)

    def body(self) -> dict[str, Any]:
        err: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.field:
            err["field"] = self.field
        return {"error": err}


def from_validation_error(exc: ValidationError, code: str = "INVALID_CONFIG") -> ApiError:
    """Lỗi validate Pydantic -> ApiError chỉ mang field path an toàn của lỗi đầu tiên.
    Không dùng msg/input/ctx của Pydantic (có thể chứa giá trị client)."""
    first = exc.errors(include_input=False, include_context=False, include_url=False)[0]
    loc = [str(p) for p in first["loc"]]
    if first["type"] == "extra_forbidden" and loc:
        loc[-1] = "*"  # tên field lạ là input của client
    return ApiError(code, ".".join(loc) or None)


# ---------- client_error có kiểu theo code ----------
RULE_ID = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=128)]
COLUMN = Annotated[str, Field(min_length=1, max_length=128)]
FQCN = Annotated[str, Field(pattern=r"^[A-Za-z_$][A-Za-z0-9_$.]*$", max_length=256)]
RUNTIME_VALUE = Annotated[str, Field(pattern=r"^[A-Za-z0-9_.:+/-]{1,128}$")]

RUNTIME_COMPONENTS = (
    "spark.sql.session.timeZone", "spark.sql.ansi.enabled", "spark.sql.caseSensitive",
    "spark.sql.legacy.timeParserPolicy", "spark.sql.sources.partitionOverwriteMode",
    "spark_version", "scala_version", "java_major", "hadoop_version",
    "jars.delta_spark", "jars.delta_storage", "jars.hadoop_aws", "jars.deequ", "jars.dq_client",
)


class _Params(BaseModel):
    model_config = ConfigDict(extra="forbid")

    @model_serializer(mode="wrap")
    def _omit_none(self, handler):
        return {k: v for k, v in handler(self).items() if v is not None}


class _RuleExc(_Params):
    rule_id: RULE_ID | None = None
    exception_class: FQCN


class _CastParams(_Params):
    rule_id: RULE_ID
    column: COLUMN | None = None
    exception_class: FQCN


class _ColumnParams(_Params):
    rule_id: RULE_ID
    column: COLUMN


class _Exc(_Params):
    exception_class: FQCN


class _OptExc(_Params):
    exception_class: FQCN | None = None


class _RuntimeParams(_Params):
    component: Literal[RUNTIME_COMPONENTS]  # type: ignore[valid-type]
    expected: RUNTIME_VALUE
    actual: RUNTIME_VALUE


# Pydantic cần class tĩnh (annotations phân giải lúc import) -> khai báo tường minh
class SqlAnalysisError(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: Literal["SQL_ANALYSIS_ERROR"]
    params: _RuleExc


class UnclassifiedError(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: Literal["UNCLASSIFIED_ENGINE_ERROR"]
    params: _RuleExc


class CastOverflowError(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: Literal["CAST_OVERFLOW"]
    params: _CastParams


class ColumnNotFoundError(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: Literal["COLUMN_NOT_FOUND"]
    params: _ColumnParams


class DriverOomError(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: Literal["DRIVER_OOM"]
    params: _Exc


class ExecutorLostError(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: Literal["EXECUTOR_LOST"]
    params: _Exc


class StorageUnavailableError(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: Literal["STORAGE_UNAVAILABLE"]
    params: _OptExc


class ServiceUnavailableError(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: Literal["SERVICE_UNAVAILABLE"]
    params: _OptExc


class CandidateUnavailableError(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: Literal["CANDIDATE_UNAVAILABLE"]
    params: _OptExc


class RuntimeMismatchError(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: Literal["RUNTIME_MISMATCH"]
    params: _RuntimeParams


ClientError = Annotated[
    Union[SqlAnalysisError, UnclassifiedError, CastOverflowError, ColumnNotFoundError, DriverOomError,
          ExecutorLostError, StorageUnavailableError, ServiceUnavailableError, CandidateUnavailableError,
          RuntimeMismatchError],
    Field(discriminator="code"),
]
CLIENT_ERROR_CODES = (
    "SQL_ANALYSIS_ERROR", "CAST_OVERFLOW", "COLUMN_NOT_FOUND", "DRIVER_OOM", "EXECUTOR_LOST",
    "STORAGE_UNAVAILABLE", "SERVICE_UNAVAILABLE", "RUNTIME_MISMATCH", "CANDIDATE_UNAVAILABLE",
    "UNCLASSIFIED_ENGINE_ERROR",
)


def check_client_error_refs(err: Any, *, rule_ids: set[str], columns: set[str], field: str = "error") -> None:
    """rule_id phải thuộc plan, column phải thuộc registered schema; lỗi -> INVALID_REPORT."""
    params = err.params
    rid = getattr(params, "rule_id", None)
    if rid is not None and rid not in rule_ids:
        raise ApiError("INVALID_REPORT", f"{field}.params.rule_id")
    col = getattr(params, "column", None)
    if col is not None and col not in columns:
        raise ApiError("INVALID_REPORT", f"{field}.params.column")


def describe_client_error(err: Any) -> str:
    """Chuỗi ngắn cho alert/log: code + rule_id nếu có (tên đã được kiểm theo plan)."""
    rid = getattr(err.params, "rule_id", None)
    return f"{err.code}" + (f" rule={rid}" if rid else "")
