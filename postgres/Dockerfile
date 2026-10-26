# Image migration schema (k8s: Argo CD Sync hook Job). Build context: thư mục gốc repo.
# init.sql + migrations/002 đều idempotent (IF NOT EXISTS, 002 chạy lại là no-op) nên mỗi lần sync chạy lại an toàn.
FROM postgres:16-alpine@sha256:721873c34ceb9f8d8fc265984940dc982404c105f19ad51be9fdc5970a6080ea
COPY postgres/init.sql /dq-sql/init.sql
COPY postgres/migrations/ /dq-sql/migrations/
WORKDIR /dq-sql
USER postgres
# PSQL_URL (libpq URI) đến từ Secret qua env, không nằm trong argv
ENTRYPOINT ["sh", "-c", "exec psql \"$PSQL_URL\" -v ON_ERROR_STOP=1 -q -f init.sql"]
