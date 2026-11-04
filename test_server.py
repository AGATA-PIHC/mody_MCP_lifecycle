import base64
import json
from types import SimpleNamespace as NS

import httpx
import pytest
from starlette.testclient import TestClient

import server
from server import Filter, build_query

COLS = {"scope", "period", "pred_class", "truth_class"}
TBL = "mody_dev.geo_commodity_classification.inference_log"


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("ALLOWED_CATALOGS", "mody_dev")
    monkeypatch.setenv("MAX_ROWS", "100")


class FakeWs:
    def __init__(self):
        col = NS(name="scope", type_text="string", nullable=True, comment="production atau shadow")
        table = NS(full_name=TBL, name="inference_log", table_type=NS(value="MANAGED"),
                   data_source_format=NS(value="ICEBERG"), owner="sp", comment=None, columns=[col])
        self.catalogs = NS(list=lambda: [NS(name="mody_dev", comment=None), NS(name="main", comment="x")])
        self.tables = NS(get=lambda full_name: table)
        self.sent = {}

        def execute_statement(**kw):
            self.sent = kw
            return NS(status=NS(state=server.StatementState.SUCCEEDED, error=None),
                      manifest=NS(schema=NS(columns=[NS(name="scope")])),
                      result=NS(data_array=[["production"], ["a|b\nc" + "x" * 300]]))

        self.statement_execution = NS(execute_statement=execute_statement)


@pytest.fixture
def fake(monkeypatch):
    w = FakeWs()
    monkeypatch.setattr(server, "ws", lambda: w)
    monkeypatch.setenv("DATABRICKS_SQL_WAREHOUSE_ID", "wh")
    return w


def test_query_uses_only_parameter_markers():
    sql, params = build_query(
        TBL, COLS,
        columns=["period", "pred_class"],
        filters=[Filter(column="scope", op="=", value="x'; DROP TABLE t; --"),
                 Filter(column="pred_class", op="in", value=["padi", "jagung"]),
                 Filter(column="truth_class", op="IS NOT NULL")],
        order_by="period", desc=True, limit=50,
    )
    assert sql == (
        "SELECT IDENTIFIER(:c0), IDENTIFIER(:c1) FROM IDENTIFIER(:tbl)"
        " WHERE IDENTIFIER(:f0) = :v0 AND IDENTIFIER(:f1) IN (:v1_0, :v1_1) AND IDENTIFIER(:f2) IS NOT NULL"
        " ORDER BY IDENTIFIER(:o) DESC LIMIT 50"
    )
    assert "DROP" not in sql
    assert params["v0"] == "x'; DROP TABLE t; --"
    assert params["tbl"] == TBL and params["c0"] == "`period`"
    assert params["v1_0"] == "padi" and params["v1_1"] == "jagung"


@pytest.mark.parametrize("kwargs, msg", [
    ({"columns": ["nope"]}, "tidak ada"),
    ({"order_by": "x; DROP"}, "tidak ada"),
    ({"filters": [Filter(column="scope", op="LIKE", value="a")]}, "tidak didukung"),
    ({"filters": [Filter(column="scope", op="=")]}, "satu value"),
    ({"filters": [Filter(column="scope", op="IN", value=[])]}, "1 sampai 100"),
])
def test_query_rejects_bad_input(kwargs, msg):
    with pytest.raises(ValueError, match=msg):
        build_query(TBL, COLS, **kwargs)


def test_limit_clamped(monkeypatch):
    monkeypatch.setenv("MAX_ROWS", "10")
    assert build_query(TBL, COLS, limit=999)[0].endswith("LIMIT 10")
    assert build_query(TBL, COLS, limit=0)[0].endswith("LIMIT 1")


@pytest.mark.parametrize("name", ["inference_log", "a.b", "a..c", "main.s.t"])
def test_full_name_validated(name):
    with pytest.raises(ValueError):
        server.split_full_name(name)


def test_list_catalogs_filtered(fake):
    out = server.list_catalogs()
    assert "mody_dev" in out and "main" not in out


def test_list_schemas_rejects_other_catalog(fake):
    with pytest.raises(ValueError, match="tidak diizinkan"):
        server.list_schemas("main")


def test_describe_table(fake):
    out = server.describe_table(TBL)
    assert "format: ICEBERG" in out
    assert "| scope | string | yes | production atau shadow |" in out


def test_query_table_runs_and_formats(fake):
    out = server.query_table(TBL, filters=[Filter(column="scope", op="=", value="production")], limit=5)
    assert fake.sent["warehouse_id"] == "wh"
    assert "IDENTIFIER(:f0) = :v0" in fake.sent["statement"]
    assert out.startswith("2 baris")
    assert "a\\|b c" in out and "..." in out  # pipe di-escape, newline dibuang, sel panjang dipotong


def test_query_table_rejects_unknown_column(fake):
    with pytest.raises(ValueError, match="tidak ada"):
        server.query_table(TBL, columns=["pred_class"])


def test_query_failure_reported(fake):
    fake.statement_execution.execute_statement = lambda **kw: NS(
        status=NS(state=server.StatementState.FAILED, error=NS(message="TABLE_OR_VIEW_NOT_FOUND")))
    with pytest.raises(RuntimeError, match="FAILED: TABLE_OR_VIEW_NOT_FOUND"):
        server.preview_table(TBL)


KEY = "k" * 32


def test_bearer_auth():
    with pytest.raises(SystemExit):
        server.create_app("")
    with TestClient(server.create_app(KEY), base_url="http://127.0.0.1:8000") as client:
        assert client.post("/mcp").status_code == 401
        assert client.post("/mcp", headers={"Authorization": "Bearer wrong"}).status_code == 401
        ok = client.post(
            "/mcp",
            headers={"Authorization": f"Bearer {KEY}", "Accept": "application/json, text/event-stream"},
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        )
        assert ok.status_code == 200
        names = {t["name"] for t in ok.json()["result"]["tools"]}
        assert names == {
            "list_catalogs", "list_schemas", "list_tables", "describe_table", "preview_table", "query_table",
            "search_runs", "get_run", "list_model_versions", "list_job_runs", "get_job_run", "get_task_output",
            "github_list_files", "github_read_file", "github_create_branch", "github_commit_files", "github_create_pr", "github_update_pr",
            "github_pr_status", "github_ci_log",
        }


@pytest.mark.parametrize("overrides, msg", [
    ({"key": "short"}, "MCP_API_KEY"),
    ({"ALLOWED_CATALOGS": ""}, "ALLOWED_CATALOGS"),
    ({"ALLOWED_CATALOGS": " , "}, "ALLOWED_CATALOGS"),
    ({"ALLOWED_EXPERIMENT_PREFIX": ""}, "ALLOWED_EXPERIMENT_PREFIX"),
    ({"ALLOWED_JOB_PREFIX": " "}, "ALLOWED_JOB_PREFIX"),
    ({"GITHUB_BRANCH_PREFIX": ""}, "GITHUB_BRANCH_PREFIX"),
    ({"GITHUB_BRANCH_PREFIX": "ma"}, "GITHUB_BRANCH_PREFIX"),
])
def test_config_fails_closed(monkeypatch, overrides, msg):
    overrides = dict(overrides)
    key = overrides.pop("key", KEY)
    for k, v in overrides.items():
        monkeypatch.setenv(k, v)
    with pytest.raises(SystemExit, match=msg):
        server.create_app(key)


def test_auth_checks_non_http_scopes():
    sent = []

    async def app(scope, receive, send):
        raise AssertionError("tidak boleh sampai app")

    async def send(msg):
        sent.append(msg)

    import asyncio
    asyncio.run(server.BearerAuth(app, KEY)({"type": "websocket", "headers": []}, None, send))
    assert sent[0]["status"] == 401


def test_tool_call_audited_without_values(fake, caplog):
    import asyncio
    caplog.set_level("INFO", logger="mody_mcp.audit")
    with pytest.raises(Exception):  # noqa: B017
        asyncio.run(server.mcp.call_tool("list_schemas", {"catalog": "rahasia_pii"}))
    rec = json.loads(caplog.records[-1].getMessage())
    assert rec["tool"] == "list_schemas" and rec["arg_keys"] == ["catalog"] and rec["outcome"] != "ok"
    assert "rahasia_pii" not in caplog.text


def test_in_values_capped():
    with pytest.raises(ValueError, match="1 sampai 100"):
        build_query(TBL, COLS, filters=[Filter(column="scope", op="IN", value=["x"] * 101)])


# ---------- mlflow ----------

EXP = "/Shared/mody/geo_commodity_classification"
MODEL = "mody_dev.geo_commodity_classification.commodity_classifier"
T0 = 1_792_988_074_000


def kv(**items):
    return [NS(key=k, value=v) for k, v in items.items()]


def mlflow_run(run_id, experiment_id="1"):
    return NS(
        info=NS(run_id=run_id, run_name=f"train_{run_id}", status=NS(value="FINISHED"),
                start_time=T0, end_time=T0 + 125_000, experiment_id=experiment_id),
        data=NS(metrics=kv(macro_f1=0.71234, accuracy=0.8),
                params=kv(lr="0.001"),
                tags=kv(git_branch="agent/x", track="pipeline", **{"mlflow.user": "u"})),
    )


@pytest.fixture
def mlflow_fake(fake):
    experiments = {"1": NS(experiment_id="1", name=EXP), "2": NS(experiment_id="2", name="/Shared/other/exp")}
    fake.search_args = {}

    def search_runs(**kw):
        fake.search_args = kw
        return iter([mlflow_run("r1"), mlflow_run("r2")])

    fake.experiments = NS(
        get_by_name=lambda name: NS(experiment=next(e for e in experiments.values() if e.name == name)),
        get_experiment=lambda eid: NS(experiment=experiments[eid]),
        search_runs=search_runs,
        get_run=lambda run_id: NS(run=mlflow_run(run_id, experiment_id="2" if run_id == "foreign" else "1")),
    )
    fake.registered_models = NS(get=lambda name, include_aliases: NS(aliases=[NS(alias_name="champion", version_num=6)]))
    fake.model_versions = NS(list=lambda name: iter([
        NS(version=str(v), run_id=f"r{v}", status=NS(value="READY"), created_at=T0, comment=None) for v in (5, 6, 7)
    ]))
    return fake


def test_search_runs(mlflow_fake):
    out = server.search_runs("/Workspace" + EXP, filter="tags.git_branch = 'agent/x'",
                             order_by="metrics.macro_f1 DESC", metrics=["macro_f1"], limit=1)
    assert mlflow_fake.search_args == {"experiment_ids": ["1"], "filter": "tags.git_branch = 'agent/x'",
                                       "order_by": ["metrics.macro_f1 DESC"], "max_results": 1}
    assert out.startswith(f"1 run di {EXP}")
    assert "| r1 | train_r1 | FINISHED | 2026-10-26 04:14 UTC " in out and "| pipeline | - | agent/x | - | macro_f1=0.7123 |" in out


@pytest.mark.parametrize("path", ["/Shared/other/exp", "/Shared/mody/../other/exp"])
def test_search_runs_rejects_other_experiment(mlflow_fake, path):
    with pytest.raises(ValueError, match="tidak diizinkan"):
        server.search_runs(path)


def test_get_run(mlflow_fake):
    out = server.get_run("r1")
    assert f"- experiment: {EXP}" in out and "durasi: 2m05s" in out
    assert "| lr | 0.001 |" in out and "| macro_f1 | 0.71234 |" in out
    assert "| git_branch | agent/x |" in out and "mlflow.user" not in out


def test_get_run_rejects_run_from_other_experiment(mlflow_fake):
    with pytest.raises(ValueError, match="tidak diizinkan"):
        server.get_run("foreign")


def test_list_model_versions(mlflow_fake):
    lines = server.list_model_versions(MODEL, limit=2).splitlines()
    assert lines[0] == f"2 versi {MODEL}"
    assert lines[3].startswith("| 7 | - | r7 | READY |")
    assert lines[4].startswith("| 6 | champion | r6 | READY |")


def test_list_model_versions_rejects_other_catalog(mlflow_fake):
    with pytest.raises(ValueError, match="tidak diizinkan"):
        server.list_model_versions("main.s.model")


# ---------- jobs ----------


def job_state(result):
    return NS(result_state=NS(value=result), life_cycle_state=NS(value="TERMINATED"), state_message="msg")


@pytest.fixture
def jobs_fake(fake):
    jobs = {1: "mody_train_dev", 2: "airbyte_sync"}
    task = NS(task_key="train", run_id=11, attempt_number=0, state=job_state("FAILED"),
              start_time=T0, end_time=T0 + 61_000)
    run = NS(run_id=100, job_id=1, state=job_state("FAILED"), start_time=T0, end_time=T0 + 61_000,
             trigger=NS(value="ONE_TIME"), run_page_url="https://x/run/100", tasks=[task],
             repair_history=[NS(type=NS(value="ORIGINAL"), state=job_state("FAILED")),
                             NS(type=NS(value="REPAIR"), state=job_state("FAILED"))],
             job_parameters=[NS(name="env", value=None, default="dev"),
                             NS(name="model_label", value="15A", default="")])

    fake.jobs = NS(
        get=lambda job_id: NS(settings=NS(name=jobs[job_id])),
        list=lambda name: iter([NS(job_id=j) for j, n in jobs.items() if n == name]),
        list_runs=lambda job_id, limit: iter([run]),
        get_run=lambda run_id, include_history: NS(**{**vars(run), "job_id": 2 if run_id == 999 else 1}),
        get_run_output=lambda run_id: NS(
            error="<span class='ansi-red-fg'>ValueError</span>: kolom hilang",
            error_trace="x" * 5000 + "\x1b[0;31mEND\x1b[0m",
            metadata=NS(job_id=2 if run_id == 999 else 1, state=job_state("FAILED")),
        ),
    )
    return fake


def test_list_job_runs(jobs_fake):
    out = server.list_job_runs("mody_train_dev")
    assert out.startswith("1 run mody_train_dev")
    assert "| 100 | FAILED | 2026-10-26 04:14 UTC " in out and "| 1m01s | ONE_TIME | model_label=15A |" in out


@pytest.mark.parametrize("name, msg", [("airbyte_sync", "tidak diizinkan"), ("mody_missing", "tidak ditemukan")])
def test_list_job_runs_rejects(jobs_fake, name, msg):
    with pytest.raises(ValueError, match=msg):
        server.list_job_runs(name)


def test_get_job_run(jobs_fake):
    out = server.get_job_run(100)
    assert out.startswith("## mody_train_dev run 100")
    assert "| env | dev |" in out and "| model_label | 15A |" in out  # default dipakai kalau value kosong
    assert "| train | 11 | 0 | FAILED |" in out
    assert "- repair: 1x, attempt sebelumnya: FAILED" in out


def test_get_task_output(jobs_fake):
    out = server.get_task_output(11)
    assert "ValueError: kolom hilang" in out and "<span" not in out
    assert out.count("x") <= server.TRACE_TAIL and "END" in out and "\x1b" not in out


@pytest.mark.parametrize("call", [lambda: server.get_job_run(999), lambda: server.get_task_output(999)])
def test_job_run_from_other_job_rejected(jobs_fake, call):
    with pytest.raises(ValueError, match="tidak diizinkan"):
        call()


# ---------- github ----------

REPO = "AGATA-PIHC/ml-ops-databricks"
YML = "usecases/geo_commodity_classification/usecase.yml"


class FakeGitHub:
    """REST API GitHub palsu. Mencatat setiap request yang mengubah data."""

    def __init__(self):
        self.branches = {"main": "sha-main"}
        self.writes = []
        self.job_branch = "agent/tune"
        self.pr = {"number": 7, "title": "tune", "state": "open", "draft": True, "merged": False, "mergeable": True,
                   "html_url": "https://github.com/pr/7",
                   "head": {"ref": "agent/tune", "sha": "abc123def4567890", "repo": {"full_name": REPO}},
                   "base": {"ref": "main"}}

    def handle(self, request: httpx.Request) -> httpx.Response:
        prefix = f"/repos/{REPO}"
        assert request.url.path.startswith(prefix) and request.headers["authorization"] == "Bearer tok"
        path, method = request.url.path.removeprefix(prefix), request.method
        body = json.loads(request.content) if request.content else None
        if method != "GET":
            self.writes.append((method, path, body))

        if path.startswith("/git/ref/heads/"):
            sha = self.branches.get(path.removeprefix("/git/ref/heads/"))
            return httpx.Response(200, json={"object": {"sha": sha}}) if sha else httpx.Response(404, json={"message": "Not Found"})
        if path.startswith("/git/commits/") and method == "GET":
            return httpx.Response(200, json={"tree": {"sha": "tree-base"}})
        routes = {
            ("POST", "/git/trees"): {"sha": "tree-new"},
            ("POST", "/git/commits"): {"sha": "commit-new-0123456789", "html_url": "https://github.com/c/1"},
            ("POST", "/git/refs"): {},
            ("PATCH", "/git/refs/heads/agent/tune"): {},
            ("POST", "/pulls"): {"number": 8, "html_url": "https://github.com/pr/8"},
            ("GET", "/pulls/7"): self.pr,
            ("PATCH", "/pulls/7"): self.pr,
            ("GET", "/contents/usecases"): [
                {"type": "dir", "path": "usecases/geo_commodity_classification", "size": 0}],
            ("GET", "/contents/.github/workflows"): [
                {"type": "file", "path": ".github/workflows/ci.yml", "size": 10}],
            ("GET", f"/contents/{YML}"): {"encoding": "base64", "content": base64.b64encode(b"epochs: 50\n").decode()},
            ("GET", "/commits/abc123def4567890/check-runs"): {"check_runs": [
                {"name": "test", "status": "completed", "conclusion": "failure", "id": 99}]},
        }
        if (method, path) == ("GET", "/actions/jobs/99"):
            return httpx.Response(200, json={"id": 99, "head_branch": self.job_branch})
        if (method, path) == ("GET", "/actions/jobs/99/logs"):
            return httpx.Response(200, text="2026-11-03T01:02:03.4567890Z \x1b[31mFAILED\x1b[0m test_x\n")
        if (method, path) in routes:
            return httpx.Response(200, json=routes[(method, path)])
        return httpx.Response(404, json={"message": "Not Found"})


@pytest.fixture
def github(monkeypatch):
    fake = FakeGitHub()
    client = httpx.Client(base_url=server.GITHUB_API, transport=httpx.MockTransport(fake.handle),
                          headers={"Authorization": "Bearer tok"})
    monkeypatch.setattr(server, "gh", lambda: client)
    monkeypatch.setenv("GITHUB_REPO", REPO)
    monkeypatch.delenv("GITHUB_ALLOWED_PATHS", raising=False)
    return fake


@pytest.mark.parametrize("path, ok", [
    (YML, True),
    ("usecases/geo_commodity_classification/rules.yml", True),
    ("src/mody_ml/usecases/geo_commodity/model.py", True),
    ("tests/unit/test_model.py", True),
    ("notebooks/training/geo_commodity_classification/train_template.py", True),
    ("tests/unit/conftest.py", False),
    ("tests/unit/helper.py", False),
    ("tests/unit/sub/test_x.py", False),
    ("src/mody_ml/usecases/geo_commodity/__init__.py", False),
    ("src/mody_ml/usecases/a/b/c.py", False),
    ("usecases/a/b/usecase.yml", False),
    ("usecases/geo_commodity_classification/model_card.md", False),
    ("src/mody_ml/lifecycle/model/registry.py", False),
    ("resources/jobs/train.yml", False),
    ("conf/env/dev.yml", False),
    ("databricks.yml", False),
    (".github/workflows/ci.yml", False),
])
def test_writable_paths(path, ok):
    assert server.is_writable(path) is ok


def test_github_dir_denied_even_if_allowed(monkeypatch):
    monkeypatch.setenv("GITHUB_ALLOWED_PATHS", "*")
    assert server.is_writable("x.py") and not server.is_writable(".github/workflows/ci.yml")
    monkeypatch.setenv("GITHUB_ALLOWED_PATHS", "*,*/*,*/*/*")
    assert not server.is_writable("conftest.py") and not server.is_writable("src/pkg/__init__.py")


@pytest.mark.parametrize("path", ["/etc/passwd", "src/../.github/x", "src//x", "src/x?ref=y", "src/x#y", "./src"])
def test_unsafe_paths_rejected(path):
    with pytest.raises(ValueError, match="tidak valid"):
        server.check_relative(path)


@pytest.mark.parametrize("branch", ["main", "agent/", "feature/x", "agent/../main", "agent/a b"])
def test_branch_rejected(branch):
    with pytest.raises(ValueError, match="tidak diizinkan"):
        server.check_branch(branch)


def test_list_files_marks_editable(github):
    out = server.github_list_files("usecases")
    assert "| dir | usecases/geo_commodity_classification | - | no |" in out
    assert "| file | .github/workflows/ci.yml | 10 | no |" in server.github_list_files(".github/workflows")


def test_read_file(github):
    out = server.github_read_file(YML)
    assert f"## {YML} (main, editable: yes)" in out and "epochs: 50" in out


def test_create_branch(github):
    assert server.github_create_branch("agent/new") == "Branch agent/new dibuat dari main (sha-main)"
    assert github.writes == [("POST", "/git/refs", {"ref": "refs/heads/agent/new", "sha": "sha-main"})]


@pytest.mark.parametrize("branch, msg", [("agent/tune", "sudah ada"), ("main", "tidak diizinkan")])
def test_create_branch_rejected(github, branch, msg):
    github.branches["agent/tune"] = "sha-tune"
    with pytest.raises(ValueError, match=msg):
        server.github_create_branch(branch)
    assert github.writes == []


def test_commit_creates_branch_from_main(github):
    out = server.github_commit_files("agent/new", "tune lr", [server.FileChange(path=YML, content="epochs: 80\n")])
    assert "(branch baru)" in out
    (_, _, tree), (_, _, commit), (_, ref_path, ref) = github.writes
    assert tree == {"base_tree": "tree-base",
                    "tree": [{"path": YML, "mode": "100644", "type": "blob", "content": "epochs: 80\n"}]}
    assert commit["parents"] == ["sha-main"]
    assert ref_path == "/git/refs" and ref == {"ref": "refs/heads/agent/new", "sha": "commit-new-0123456789"}


def test_commit_updates_existing_branch_without_force(github):
    github.branches["agent/tune"] = "sha-tune"
    server.github_commit_files("agent/tune", "hapus", [server.FileChange(path="tests/unit/test_old.py")])
    (_, _, tree), (_, _, commit), (method, path, ref) = github.writes
    assert tree["tree"][0]["sha"] is None  # hapus file
    assert commit["parents"] == ["sha-tune"]
    assert (method, path, ref["force"]) == ("PATCH", "/git/refs/heads/agent/tune", False)


@pytest.mark.parametrize("branch, files, msg", [
    ("main", [server.FileChange(path=YML, content="x")], "tidak diizinkan"),
    ("agent/x", [server.FileChange(path="src/mody_ml/lifecycle/a.py", content="x")], "tidak boleh diubah"),
    ("agent/x", [server.FileChange(path=".github/workflows/ci.yml", content="x")], "tidak boleh diubah"),
    ("agent/x", [server.FileChange(path=YML, content="a"), server.FileChange(path=YML, content="b")], "hanya sekali"),
    ("agent/x", [], "minimal satu"),
    ("agent/x", [server.FileChange(path=f"usecases/u{i}/a.yml", content="x") for i in range(21)], "Maksimal"),
    ("agent/x", [server.FileChange(path=YML, content="x" * 500_001)], "Maksimal"),
])
def test_commit_rejected_before_any_write(github, branch, files, msg):
    with pytest.raises(ValueError, match=msg):
        server.github_commit_files(branch, "m", files)
    assert github.writes == []


def test_create_pr_is_draft_to_main(github):
    assert server.github_create_pr("agent/tune", "Tune", "hipotesis") == "PR #8 dibuka (draft): https://github.com/pr/8"
    assert github.writes == [("POST", "/pulls", {"head": "agent/tune", "base": "main", "title": "Tune",
                                                  "body": "hipotesis", "draft": True})]


def test_update_pr(github):
    server.github_update_pr(7, body="macro_f1 0.70")
    assert github.writes == [("PATCH", "/pulls/7", {"body": "macro_f1 0.70"})]


@pytest.mark.parametrize("head", [{"ref": "feature/x", "repo": {"full_name": REPO}},
                                  {"ref": "agent/x", "repo": {"full_name": "other/fork"}}])
def test_update_pr_rejects_non_agent_pr(github, head):
    github.pr["head"] = {**github.pr["head"], **head}
    with pytest.raises(ValueError):
        server.github_update_pr(7, body="x")
    assert github.writes == []


def test_pr_status_and_ci_log(github):
    out = server.github_pr_status(7)
    assert "- state: open (draft)" in out and "| test | completed | failure | 99 |" in out
    log = server.github_ci_log(99)
    assert "FAILED test_x" in log and "2026-11-03T" not in log and "\x1b" not in log


def test_pr_status_rejects_non_agent_pr(github):
    github.pr["head"] = {**github.pr["head"], "ref": "feature/x"}
    with pytest.raises(ValueError, match="tidak diizinkan"):
        server.github_pr_status(7)


@pytest.mark.parametrize("branch", ["main", "feature/x", None])
def test_ci_log_rejects_non_agent_job(github, branch):
    github.job_branch = branch
    with pytest.raises(ValueError, match="tidak diizinkan"):
        server.github_ci_log(99)


def test_github_error_reported(github):
    with pytest.raises(server.GitHubError, match="404 Not Found"):
        server.github_read_file("src/missing.py")
