#!/usr/bin/env python3
"""Acceptance harness (stdlib). Cấp Compose project `dqaccept-<run-id>` cô lập; không bao giờ tự `down -v`.

  python scripts/run_acceptance.py --stage compatibility --run-id dq-wp1-a --output /tmp/dq-wp1-a
"""
import argparse, hashlib, hmac, json, os, re, secrets, subprocess, sys, time, urllib.request, urllib.error
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
E2E_DT = "2026-10-20"
DENY_DT = "2026-10-18"
OTHER_DT = "2026-10-19"
EXPECTED_QUARANTINE = [{"id": 1, "errors": ["amount_range", "event_time_in_partition", "value_complete"]},
                       {"id": 2, "errors": ["value_complete"]}, {"id": 3, "errors": ["amount_range"]}]
BUCKET = "dqprobe"
FIXTURE_DATASET = "acceptance.fixture"
SPARK_BUILD_DONE = "dqplatform.jobs.CompatibilityProbe"
PG_USER = "dq"
WORKER_CP = "/opt/dq-worker/dq-anomaly-worker-0.1.0.jar:/opt/dq-worker/lib/*"


class Stop(Exception):
    def __init__(self, status, reason):
        self.status, self.reason = status, reason


def sh(cmd, env=None, check=True, timeout=1800, stdin=None):
    p = subprocess.run(cmd, cwd=ROOT, env={**os.environ, **(env or {})}, capture_output=True, text=True,
                       timeout=timeout, input=stdin)
    if check and p.returncode != 0:
        raise Stop("FAIL", f"command failed exit={p.returncode}: {' '.join(cmd[:6])}")
    return p


class Run:
    def __init__(self, a):
        self.id, self.out, self.project = a.run_id, a.output, f"dqaccept-{a.run_id}"
        self.sec = os.path.join(a.output, "secrets")
        self.evidence = {}
        self.redact = []
        self.env = {}

    def p(self, *names):
        return os.path.join(self.out, *names)

    def write(self, rel, text, mode=0o644):
        path = self.p(rel)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
        with os.fdopen(fd, "w") as f:
            f.write(text)
        os.chmod(path, mode)

    def scrub(self, text):
        for s in self.redact:
            text = text.replace(s, "***")
        return text

    def log(self, name, *procs):
        self.write(f"logs/{name}.log", self.scrub("\n".join(f"$ exit={q.returncode}\n{q.stdout}\n{q.stderr}" for q in procs)))

    def compose(self, *args, **kw):
        return sh(["docker", "compose", "-p", self.project, *args], env=self.env, **kw)

    # ---------- setup ----------
    def prepare(self):
        os.makedirs(self.out, exist_ok=True)
        marker = self.p("ownership.json")
        if os.listdir(self.out) and not os.path.exists(marker):
            raise Stop("FAIL", "output dir không rỗng và không thuộc harness")
        if os.path.exists(marker) and json.load(open(marker)).get("run_id") != self.id:
            raise Stop("FAIL", "output dir thuộc run-id khác")
        if self.compose("ls", "-a", "--format", "json", check=False).stdout.count(self.project):
            raise Stop("FAIL", f"project {self.project} đã tồn tại; dùng run-id khác")
        self.write("ownership.json", json.dumps({"run_id": self.id, "project": self.project}))
        for d in ("secrets", "logs", "probe"):
            os.makedirs(self.p(d), mode=0o700 if d == "secrets" else 0o755, exist_ok=True)
        os.chmod(self.p("probe"), 0o777)  # user spark (uid 185) trong container ghi kết quả
        os.chmod(self.sec, 0o700)

        tok = {r: secrets.token_urlsafe(32) for r in ("editor", "approver", "operator", "viewer", "job")}
        self.tok = tok
        pg, mu, mp = (secrets.token_urlsafe(24) for _ in range(3))
        self.redact = [pg, mu, mp, *tok.values()]
        grants = {
            r: [{"dataset_id": FIXTURE_DATASET, "role": r, "workflow": None, "purpose": None}]
            for r in ("editor", "approver", "operator", "viewer")}
        grants["job"] = [{"dataset_id": FIXTURE_DATASET, "role": "job", "workflow": w, "purpose": p}
                         for w, p in (("WRITE_GATE", "default"), ("AUDIT_TABLE", "nightly"))]
        registry = {"principals": [{"principal": f"acceptance-{r}", "grants": grants[r],
                                    "token_sha256": hashlib.sha256(t.encode()).hexdigest()} for r, t in tok.items()]}
        files = {"postgres_password": pg, "minio_user": mu, "minio_password": mp,
                 "database_url": f"postgresql+psycopg://{PG_USER}:{pg}@postgres:5432/dq",
                 "token_registry.json": json.dumps(registry), "job_token": tok["job"],
                 **{f"token_{r}": t for r, t in tok.items()}}
        for n, v in files.items():
            # dq-service (root trong container) đọc được 0600 và bắt buộc registry 0600; Spark chạy uid 185 nên
            # các secret khác cần đọc được qua bind mount; thư mục secrets 0700 chặn user khác trên host
            self.write(f"secrets/{n}", v, 0o600 if n == "token_registry.json" else 0o644)
        self.s3 = (mu, mp)

        spark_img = re.findall(r"^FROM (apache/spark\S+)", open(os.path.join(ROOT, "spark/Dockerfile")).read(), re.M)[-1].split()[0]
        v = sh(["docker", "run", "--rm", spark_img, "java", "-cp", "/opt/spark/jars/*", "org.apache.hadoop.util.VersionInfo"], check=False)
        m = re.match(r"Hadoop (\S+)", v.stdout)
        if not m:
            raise Stop("BLOCKED", "không xác định được Hadoop version từ Spark image")
        hadoop = m.group(1)
        self.write("hadoop_version", hadoop + "\n")
        minio = sh(["docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}",
                    "ghcr.io/coollabsio/minio:RELEASE.2025-10-15T17-29-55Z"], check=False)
        if minio.returncode:
            raise Stop("BLOCKED", "MinIO image chưa có sẵn để pin digest")
        self.env = {"DQ_SECRETS_DIR": self.sec, "HADOOP_VERSION": hadoop, "MINIO_IMAGE": minio.stdout.strip()}
        self.evidence["hadoop_version"] = hadoop
        self.evidence["images"] = {"spark_base": spark_img, "minio": self.env["MINIO_IMAGE"]}
        self.write("ownership.json", json.dumps({
            "run_id": self.id, "project": self.project, "created_at": datetime.now(timezone.utc).isoformat(),
            "resources": "containers, networks, named volumes (pgdata, miniodata) and images of compose project " + self.project,
            "teardown": f"docker compose -p {self.project} down -v"}, indent=2))

    # ---------- compatibility ----------
    def s3_put_bucket(self, port):
        """PUT bucket bằng SigV4 stdlib (image MinIO không có mc)."""
        mu, mp = self.s3
        host = f"127.0.0.1:{port}"
        now = datetime.now(timezone.utc)
        amz, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
        ph = hashlib.sha256(b"").hexdigest()
        canon = f"PUT\n/{BUCKET}\n\nhost:{host}\nx-amz-content-sha256:{ph}\nx-amz-date:{amz}\n\nhost;x-amz-content-sha256;x-amz-date\n{ph}"
        scope = f"{day}/us-east-1/s3/aws4_request"
        sts = f"AWS4-HMAC-SHA256\n{amz}\n{scope}\n{hashlib.sha256(canon.encode()).hexdigest()}"
        k = ("AWS4" + mp).encode()
        for part in (day, "us-east-1", "s3", "aws4_request"):
            k = hmac.new(k, part.encode(), hashlib.sha256).digest()
        sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
        req = urllib.request.Request(f"http://{host}/{BUCKET}", method="PUT", headers={
            "x-amz-date": amz, "x-amz-content-sha256": ph,
            "Authorization": f"AWS4-HMAC-SHA256 Credential={mu}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}"})
        try:
            urllib.request.urlopen(req, timeout=10)
        except urllib.error.HTTPError as e:
            if e.code != 409:  # 409 = bucket đã tồn tại
                raise Stop("FAIL", f"tạo bucket thất bại http={e.code}")

    def host_port(self, service, port):
        out = self.compose("port", service, str(port)).stdout.strip()
        return int(out.rsplit(":", 1)[1])

    def compatibility(self):
        b = self.compose("--profile", "jobs", "build", "spark", "dq-service", check=False, timeout=3600)
        self.log("build", b)
        if b.returncode:
            raise Stop("FAIL", "docker build thất bại (xem logs/build.log)")
        for svc, key in (("spark", "dq-demo/spark-deequ:3.5.1"), ("dq-service", "dq-demo/dq-service:0.2")):
            d = sh(["docker", "image", "inspect", "--format", "{{.Id}}", key]).stdout.strip()
            self.evidence["images"][svc] = d
        u = self.compose("up", "-d", "--wait", "postgres", "minio", check=False, timeout=600)
        self.log("up", u)
        if u.returncode:
            raise Stop("FAIL", "không khởi động được postgres/minio")
        ports = {"postgres": self.host_port("postgres", 5432), "minio": self.host_port("minio", 9000)}
        self.evidence["host_ports"] = ports
        for _ in range(30):
            try:
                self.s3_put_bucket(ports["minio"])
                break
            except (urllib.error.URLError, ConnectionError):
                time.sleep(2)
        else:
            raise Stop("FAIL", "MinIO không phản hồi")

        q = self.compose("exec", "-T", "postgres", "psql", "-U", PG_USER, "-d", "dq", "-Atc",
                         "SELECT version FROM schema_versions ORDER BY version", check=False)
        self.log("postgres-schema", q)
        if "2" not in q.stdout.split():
            raise Stop("FAIL", "postgres init chưa áp dụng schema_versions 2")
        self.evidence["postgres_schema_versions"] = q.stdout.split()

        probe = self.compose(
            "run", "--rm", "-T", "-v", f"{self.p('probe')}:/probe", "spark",
            "/opt/spark/bin/spark-submit", "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
            "--class", SPARK_BUILD_DONE, "/opt/dq/jars/dq-jobs-0.1.0.jar",
            "--root", f"s3a://{BUCKET}/probe/{self.id}", "--output", "/probe/compatibility.json",
            "--manifest-output", "/probe/manifest.json", check=False, timeout=1800)
        self.log("spark-probe", probe)
        if probe.returncode:
            raise Stop("FAIL", f"CompatibilityProbe exit={probe.returncode}")
        comp = json.load(open(self.p("probe", "compatibility.json")))
        if comp.get("status") != "PASS":
            raise Stop("FAIL", "CompatibilityProbe không báo PASS")
        for n in ("compatibility.json", "manifest.json"):
            os.replace(self.p("probe", n), self.p(n))
        self.evidence["compatibility"] = "compatibility.json"
        self.evidence["manifest"] = "manifest.json"

        w = self.compose("run", "--rm", "-T", "--no-deps", "--entrypoint", "java", "dq-service",
                         "-cp", WORKER_CP, "dqplatform.worker.AnomalyWorker", "--probe", check=False, timeout=300)
        self.log("worker-probe", w)
        try:
            ok = w.returncode == 0 and json.loads(w.stdout.strip().splitlines()[-1]).get("status") == "PASS"
        except (ValueError, IndexError):
            ok = False
        if not ok:
            raise Stop("FAIL", "worker probe không PASS")
        self.write("worker-probe.json", w.stdout.strip().splitlines()[-1] + "\n")
        self.evidence["worker_probe"] = "worker-probe.json"

    # ---------- control ----------
    def http(self, role, method, path, body=None, key=None, ok=(200, 201, 202)):
        """HTTP tới dq-service qua host port; lỗi chỉ ghi status+error code (không body, không token)."""
        h = {"Content-Type": "application/json"}
        if role:
            h["Authorization"] = f"Bearer {self.tok[role]}"
        if key:
            h["Idempotency-Key"] = key
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.base_url + path, data=data, method=method, headers=h)
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                status, raw = r.status, r.read()
        except urllib.error.HTTPError as e:
            status, raw = e.code, e.read()
        payload = json.loads(raw) if raw else None
        if status not in ok:
            code = (payload or {}).get("error", {}).get("code", "?") if isinstance(payload, dict) else "?"
            raise Stop("FAIL", f"{method} {path} -> http {status} {code}")
        return status, payload

    def spark_run(self, label, cls, args, dq=False, purpose="default"):
        """spark-submit trong container spark; job token đọc từ secret file ngay trong container (không lên argv)."""
        confs = ["--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000"]
        if dq:
            confs += ["--conf", f"spark.dq.dataset={FIXTURE_DATASET}", "--conf", f"spark.dq.purpose={purpose}",
                      "--conf", f"spark.dq.compat.manifest.id={self.manifest_id}",
                      "--conf", "spark.dq.state.dir=/e2e/state"]
        p = self.compose(
            "run", "--rm", "--no-deps", "-T", "-v", f"{self.p('e2e')}:/e2e",
            "-e", f"DQ_RUN_KEY={self.id}-{label}", "-e", f"DQ_RUN_STATE_FILE=/e2e/{label}.run.json",
            "spark", "sh", "-c", 'DQ_API_TOKEN=$(cat /run/secrets/job_token) exec "$@"', "_",
            "/opt/spark/bin/spark-submit", *confs, "--class", cls, "/opt/dq/jars/dq-jobs-0.1.0.jar", *args,
            check=False, timeout=1800)
        self.log(f"spark-{label}", p)
        last = None
        for ln in reversed(p.stdout.splitlines()):
            try:
                last = json.loads(ln)
                break
            except ValueError:
                continue
        return p.returncode, last

    def fixture(self, label, *args):
        code, out = self.spark_run(label, "dqplatform.jobs.AcceptanceFixture", list(args))
        if code != 0 or out is None:
            raise Stop("FAIL", f"AcceptanceFixture {args[0]} exit={code}")
        return out

    def put_evidence(self, name, value):
        self.write(f"e2e/{name}.json", json.dumps(value, indent=2))
        self.evidence.setdefault("e2e", {})[name] = f"e2e/{name}.json"

    def control(self):
        os.makedirs(self.p("e2e", "state"), exist_ok=True)
        os.chmod(self.p("e2e"), 0o777)  # user spark (uid 185) ghi registration/state
        os.chmod(self.p("e2e", "state"), 0o777)
        self.root_uri = f"s3a://{BUCKET}/e2e/{self.id}"
        u = self.compose("up", "-d", "--wait", "dq-service", check=False, timeout=600)
        self.log("up-service", u)
        if u.returncode:
            raise Stop("FAIL", "không khởi động được dq-service")
        self.base_url = f"http://127.0.0.1:{self.host_port('dq-service', 8000)}"
        self.evidence["host_ports"]["dq-service"] = int(self.base_url.rsplit(":", 1)[1])
        self.http(None, "GET", "/health", ok=(200,))

        code, boot = self.spark_run("bootstrap", "dqplatform.jobs.AcceptanceFixture",
                                    ["bootstrap", "--root", self.root_uri, "--dataset", FIXTURE_DATASET,
                                     "--registration-output", "/e2e/registration.json"])
        if code != 0:
            raise Stop("FAIL", f"AcceptanceFixture bootstrap exit={code}")
        reg = json.load(open(self.p("e2e", "registration.json")))
        self.http("editor", "PUT", f"/api/v2/datasets/{FIXTURE_DATASET}", reg["registration"], ok=(200, 201))
        self.base_facts = self.fixture("verify-base", "verify", "--root", self.root_uri, "--dt", E2E_DT)

        manifest = json.load(open(self.p("manifest.json")))
        _, m = self.http("editor", "POST", "/api/v2/compat-manifests", manifest, key=f"{self.id}-manifest")
        self.manifest_id = m["manifest_id"]
        self.http("approver", "PUT", f"/api/v2/datasets/{FIXTURE_DATASET}/compat-manifests/{self.manifest_id}/acceptance",
                  {"harness_reference": f"run_acceptance:{self.id}:compatibility"}, key=f"{self.id}-accept", ok=(200,))

        seed = json.load(open(os.path.join(ROOT, "suites", "acceptance_fixture.json")))
        if seed["dataset"] != FIXTURE_DATASET:
            raise Stop("FAIL", "suites/acceptance_fixture.json dataset không khớp harness")
        audit_suite = {"format": "dq-rules/v3", "workflow": "AUDIT_TABLE", "description": None, "parameters": {}, "rules": [
            {"rule_id": "audit_rows", "dimension": "completeness", "scope": "TABLE", "stage": "AUDIT_SNAPSHOT",
             "check": {"kind": "size", "assert": {"op": ">=", "value": 1}}},
            {"rule_id": "audit_amount_range", "dimension": "validity", "scope": "TABLE", "stage": "AUDIT_SNAPSHOT",
             "check": {"kind": "range", "column": "amount", "min": "0", "max": "100"}}]}
        audit_policy = {"rules": [{"rule_id": "audit_rows", "action": "BLOCK", "required": True},
                                  {"rule_id": "audit_amount_range", "action": "BLOCK", "required": True}],
                        "max_quarantine_ratio": "0", "anomaly": {"enabled": False}}
        self.bindings = {}
        for wf, purpose, suite, policy in (("WRITE_GATE", "default", seed["suite"], seed["policy"]),
                                           ("AUDIT_TABLE", "nightly", audit_suite, audit_policy)):
            d = f"/api/v2/datasets/{FIXTURE_DATASET}"
            tag = f"{self.id}-{wf}"
            _, s = self.http("editor", "POST", f"{d}/suites", {"name": f"acceptance-{wf.lower()}", "workflow": wf,
                                                               "description": None}, key=f"{tag}-suite")
            _, rev = self.http("editor", "POST", f"{d}/suites/{s['suite_id']}/revisions", suite, key=f"{tag}-rev")
            _, pol = self.http("editor", "POST", f"{d}/suites/{s['suite_id']}/policy-revisions", policy, key=f"{tag}-pol")
            self.http("editor", "PUT", f"{d}/purposes/{wf}/{purpose}", {"description": None}, ok=(200, 201))
            _, b = self.http("approver", "PUT", f"{d}/purposes/{wf}/{purpose}/binding", {
                "expected_binding_version": 0, "suite_id": s["suite_id"], "suite_revision_id": rev["revision_id"],
                "policy_revision_id": pol["revision_id"], "reason": "acceptance"}, key=f"{tag}-bind", ok=(200,))
            self.bindings[wf] = {"suite_id": s["suite_id"], "suite_revision_id": rev["revision_id"],
                                 "policy_revision_id": pol["revision_id"], "binding_version": b["binding_version"]}
        self.seed = seed
        self.put_evidence("control", {"dataset": FIXTURE_DATASET, "root": self.root_uri, "manifest_id": self.manifest_id,
                                      "bindings": self.bindings, "base_facts": self.base_facts})

    # ---------- end-to-end ----------
    def check(self, cond, what):
        if not cond:
            raise Stop("FAIL", f"assert: {what}")

    def write_job(self, label, dt, variant, rows=100, expect_exit=0):
        code, out = self.spark_run(label, "dqplatform.jobs.AcceptanceFixture",
                                   ["run", "--dt", dt, "--variant", variant, "--rows", str(rows)], dq=True)
        self.check(code == expect_exit and out is not None, f"{label}: exit={code}, mong đợi {expect_exit}")
        return out

    def facts(self, dt, publication=None):
        a = ["verify", "--root", self.root_uri, "--dt", dt]
        if publication:
            a += ["--silver-version", str(publication["silver"]["commit_version"])]
            if publication["quarantine_commit_version"] is not None:
                a += ["--quarantine-version", str(publication["quarantine_commit_version"])]
        return self.fixture(f"verify-{dt}-{secrets.token_hex(3)}", *a)

    def publication(self, dt):
        _, p = self.http("viewer", "GET", f"/api/v2/datasets/{FIXTURE_DATASET}/publications?dt={dt}", ok=(200,))
        return p

    def end_to_end(self):
        d = f"/api/v2/datasets/{FIXTURE_DATASET}"
        base_other = self.base_facts["silver_other"]
        self.check(base_other == [f"9001|other|5.00|{OTHER_DT} 08:00:00|{OTHER_DT}",
                                  f"9002|other|6.00|{OTHER_DT} 09:00:00|{OTHER_DT}"], "partition khác có đúng 9001/9002")
        ev = {}

        # scenario 1: 100/3 -> ALLOW, Silver 4..100, quarantine 1,2,3, partition khác nguyên vẹn
        r1 = self.write_job("good-20", E2E_DT, "good")
        pub = self.publication(E2E_DT)
        cur = pub["current"]
        self.check(r1["decision"] == "ALLOW" and cur and cur["execution_id"] == r1["execution_id"], "S1 ALLOW + pointer")
        self.check(r1["publication"]["silver"]["evidence"] == "EXACT" and
                   r1["publication"]["quarantine"]["evidence"] == "EXACT", "S1 hai destination EXACT")
        f1 = self.facts(E2E_DT, cur)
        self.check(f1["silver_ids"] == list(range(4, 101)), "S1 Silver IDs 4..100")
        self.check(f1["quarantine"] == EXPECTED_QUARANTINE, "S1 quarantine IDs 1,2,3 + _dq_errors sorted")
        self.check(f1["silver_other"] == base_other, "S1 partition 2026-10-19 không đổi")
        self.check(cur["quarantine_rows"] == 3, "S1 quarantine_rows=3")
        ev["scenario_1"] = {"execution_id": r1["execution_id"], "publication": cur, "facts": f1}

        # scenario 2: 100/6 -> DENY (exit 3), version hai bảng đích không đổi
        before = self.facts(E2E_DT)
        r2 = self.write_job("deny-18", DENY_DT, "deny", expect_exit=3)
        after = self.facts(E2E_DT)
        self.check(r2["decision"] == "DENY", "S2 decision DENY")
        for k in ("silver_latest_version", "quarantine_latest_version", "silver_total"):
            self.check(before[k] == after[k], f"S2 {k} không đổi")
        deny_dt = self.facts(DENY_DT)
        self.check(deny_dt["silver_ids"] == [] and deny_dt["quarantine"] == [], "S2 không có dòng nào của dt DENY")
        self.check(self.publication(DENY_DT)["current"] is None, "S2 không có publication pointer")
        ev["scenario_2"] = {"execution_id": r2["execution_id"], "before": before, "after": after}

        # scenario 10: audit chỉ đọc snapshot đã pin, kết quả PASS, không ghi bảng nào
        pre = self.facts(E2E_DT)
        code, a = self.spark_run("audit", "dqplatform.jobs.AuditTable", [], dq=True, purpose="nightly")
        self.check(code == 0 and a and a.get("assessment") == "PASS", f"S10 audit PASS (exit={code})")
        _, rep = self.http("viewer", "GET", f"/api/v2/executions/{a['execution_id']}/report", ok=(200,))
        audited = rep["report"]
        self.check(audited["measured_rows"] == pre["silver_total"] and
                   audited["audit_reference"]["version"] == pre["silver_latest_version"], "S10 đo đúng snapshot đã pin")
        self.check("raw_reference" not in audited and audited["classification_status"] == "NOT_APPLICABLE", "S10 không candidate")
        post = self.facts(E2E_DT)
        self.check((pre["silver_latest_version"], pre["quarantine_latest_version"]) ==
                   (post["silver_latest_version"], post["quarantine_latest_version"]), "S10 audit không ghi bảng")
        ev["scenario_10"] = {"execution_id": a["execution_id"], "measured_rows": audited["measured_rows"],
                             "audit_version": audited["audit_reference"]["version"]}

        # scenario 11: policy bật anomaly (0.8..1.2); 100 -> 130 dòng => ANOMALY nhưng gate vẫn ALLOW
        bw = self.bindings["WRITE_GATE"]
        policy = json.loads(json.dumps(self.seed["policy"]))
        policy["anomaly"] = {"enabled": True, "max_points": 20, "lower": "0.8", "upper": "1.2", "bootstrap": True}
        _, pol = self.http("editor", "POST", f"{d}/suites/{bw['suite_id']}/policy-revisions", policy,
                           key=f"{self.id}-pol-anomaly")
        self.http("approver", "PUT", f"{d}/purposes/WRITE_GATE/default/binding", {
            "expected_binding_version": bw["binding_version"], "suite_id": bw["suite_id"],
            "suite_revision_id": bw["suite_revision_id"], "policy_revision_id": pol["revision_id"],
            "reason": "enable anomaly"}, key=f"{self.id}-bind-anomaly", ok=(200,))
        runs = {}
        for dt, rows in (("2026-10-21", 100), ("2026-10-22", 130)):
            r = self.write_job(f"good-{dt[-2:]}", dt, "good", rows=rows)
            self.check(r["decision"] == "ALLOW", f"S11 {dt} decision ALLOW")
            _, an = self.http("viewer", "GET", f"/api/v2/executions/{r['execution_id']}/anomalies", ok=(200,))
            runs[dt] = {"execution_id": r["execution_id"], "anomalies": an["items"]}
            self.check(an["items"], f"S11 {dt} có anomaly observations")
            pf = self.facts(dt, self.publication(dt)["current"])
            self.check(pf["silver_ids"] == list(range(4, rows + 1)), f"S11 {dt} Silver IDs 4..{rows}")
        self.check(not any(o["status"] == "ANOMALY" for o in runs["2026-10-21"]["anomalies"]), "S11 ngày 21 không ANOMALY")
        self.check(any(o["status"] == "ANOMALY" and o["metric_name"] == "Size" for o in runs["2026-10-22"]["anomalies"]),
                   "S11 ngày 22 có ANOMALY trên Size")
        ev["scenario_11"] = runs
        self.put_evidence("end-to-end", ev)

    def finish(self, status, reason=None):
        self.evidence["teardown"] = f"docker compose -p {self.project} down -v"
        self.evidence["output"] = self.out
        res = {"status": status, "run_id": self.id, "evidence": self.evidence}
        if reason:
            res["reason"] = reason
        try:
            self.write("result.json", json.dumps(res, indent=2))
        except OSError:
            pass
        print(json.dumps(res))
        return 0 if status == "PASS" else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True, choices=["compatibility", "control", "end-to-end", "all"])
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--output", required=True)
    a = ap.parse_args()
    if not re.fullmatch(r"[a-z0-9-]+", a.run_id) or not os.path.isabs(a.output):
        print(json.dumps({"status": "FAIL", "run_id": a.run_id, "reason": "run-id [a-z0-9-]+ và --output tuyệt đối"}))
        return 1
    r = Run(a)
    try:
        r.prepare()
        r.compatibility()  # control/end-to-end dựa trên stack + manifest của chính run này
        if a.stage == "compatibility":
            return r.finish("PASS")
        r.control()
        if a.stage in ("end-to-end", "all"):
            r.end_to_end()
        return r.finish("PASS")
    except Stop as e:
        return r.finish(e.status, e.reason)
    except subprocess.TimeoutExpired as e:
        return r.finish("FAIL", f"timeout: {' '.join(map(str, e.cmd[:5]))}")


if __name__ == "__main__":
    sys.exit(main())
