"""mody-mcp: MCP server untuk agent Mody (Streamable HTTP).

Databricks (Unity Catalog, MLflow, Jobs) read-only. GitHub: baca repo, commit ke branch agent, kelola PR agent.

Spesifikasi: docs/2026-10-30-mody-mcp-generik-design.md
"""

from __future__ import annotations

import base64
import fnmatch
import hmac
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from functools import cache
from itertools import islice
from typing import Any

import httpx
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    ExecuteStatementRequestOnWaitTimeout,
    StatementParameterListItem,
    StatementState,
)
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel

HOST = os.environ.get("HOST", "127.0.0.1")  # remote: set HOST eksplisit, taruh di belakang proxy TLS
PORT = int(os.environ.get("PORT", "8000"))
MAX_CELL = 200
OPS = {"=", "!=", "<", "<=", ">", ">=", "IN", "IS NULL", "IS NOT NULL"}
MAX_IN_VALUES = 100
MIN_API_KEY_LEN = 32

audit = logging.getLogger("mody_mcp.audit")


class AuditedFastMCP(FastMCP):
    """Satu baris JSON per tool call untuk SIEM. Hanya nama argumen, bukan nilainya (bisa berisi PII/kode)."""

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        start, outcome = time.monotonic(), "ok"
        try:
            return await super().call_tool(name, arguments)
        except Exception as e:
            outcome = type(e).__name__
            raise
        finally:
            audit.info(json.dumps({"event": "tool_call", "tool": name, "arg_keys": sorted(arguments or {}),
                                   "outcome": outcome, "ms": round((time.monotonic() - start) * 1000)}))


mcp = AuditedFastMCP("mody-mcp", host=HOST, port=PORT, stateless_http=True, json_response=True)


@cache
def ws() -> WorkspaceClient:
    return WorkspaceClient()  # baca DATABRICKS_HOST/TOKEN (atau CLIENT_ID/SECRET) dari env


def allowed_catalogs() -> set[str]:
    return {c.strip() for c in os.environ.get("ALLOWED_CATALOGS", "").split(",") if c.strip()}


def max_rows() -> int:
    return int(os.environ.get("MAX_ROWS", "100"))


def check_catalog(catalog: str) -> None:
    allowed = allowed_catalogs()
    if catalog not in allowed:
        raise ValueError(f"Catalog '{catalog}' tidak diizinkan")


def split_full_name(full_name: str) -> list[str]:
    parts = full_name.split(".")
    if len(parts) != 3 or not all(parts):
        raise ValueError(f"full_name harus catalog.schema.table, misalnya mody_dev.core.ref_experiment. Didapat: '{full_name}'")
    check_catalog(parts[0])
    return parts


def cell(value: Any) -> str:
    text = "-" if value is None or value == "" else str(value)
    text = text.replace("|", "\\|").replace("\n", " ")
    return text if len(text) <= MAX_CELL else text[:MAX_CELL] + "..."


def md_table(headers: list[str], rows: list[list[Any]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)


def columns_table(columns: list[Any] | None) -> str:
    if not columns:
        return "(kolom tidak tersedia dari Unity Catalog)"
    return md_table(
        ["column", "type", "nullable", "comment"],
        [[c.name, c.type_text, "yes" if c.nullable else "no", c.comment] for c in columns],
    )


# ---------- metadata ----------


@mcp.tool()
def list_catalogs() -> str:
    """Daftar catalog Unity Catalog yang bisa diakses. Mulai dari sini kalau belum tahu nama catalog."""
    allowed = allowed_catalogs()
    rows = [[c.name, c.comment] for c in ws().catalogs.list() if c.name in allowed]
    return md_table(["catalog", "comment"], rows)


@mcp.tool()
def list_schemas(catalog: str) -> str:
    """Daftar schema di satu catalog, dengan komentarnya."""
    check_catalog(catalog)
    rows = [[s.name, s.comment] for s in ws().schemas.list(catalog_name=catalog)]
    return f"Schema di {catalog} ({len(rows)}):\n" + md_table(["schema", "comment"], rows)


@mcp.tool()
def list_tables(catalog: str, schema: str, include_columns: bool = False) -> str:
    """Daftar tabel di satu schema. include_columns=True menambahkan kolom tiap tabel (output lebih panjang)."""
    check_catalog(catalog)
    tables = list(ws().tables.list(catalog_name=catalog, schema_name=schema, omit_columns=not include_columns))
    out = f"Tabel di {catalog}.{schema} ({len(tables)}):\n" + md_table(
        ["table", "type", "comment"],
        [[t.name, t.table_type.value if t.table_type else None, t.comment] for t in tables],
    )
    if include_columns:
        out += "".join(f"\n\n### {t.name}\n{columns_table(t.columns)}" for t in tables)
    return out


@mcp.tool()
def describe_table(full_name: str) -> str:
    """Struktur lengkap satu tabel: tipe, format, owner, komentar, dan semua kolom. full_name = catalog.schema.table."""
    split_full_name(full_name)
    t = ws().tables.get(full_name=full_name)
    return (
        f"## {t.full_name}\n"
        f"- type: {cell(t.table_type.value if t.table_type else None)}, "
        f"format: {cell(t.data_source_format.value if t.data_source_format else None)}\n"
        f"- owner: {cell(t.owner)}\n"
        f"- comment: {cell(t.comment)}\n\n" + columns_table(t.columns)
    )


# ---------- data ----------


class Filter(BaseModel):
    column: str
    op: str
    value: str | int | float | bool | list[str | int | float | bool] | None = None


def build_query(
    full_name: str,
    known_columns: set[str],
    columns: list[str] | None = None,
    filters: list[Filter] | None = None,
    order_by: str | None = None,
    desc: bool = False,
    limit: int = 20,
) -> tuple[str, dict[str, str]]:
    """Susun SELECT bertemplate. Semua nama dan nilai dari agent jadi parameter marker, tidak pernah ditempel ke SQL."""
    params: dict[str, str] = {"tbl": full_name}

    def ident(name: str, key: str) -> str:
        if name not in known_columns:
            raise ValueError(f"Kolom '{name}' tidak ada. Kolom yang sah: {sorted(known_columns)}")
        params[key] = f"`{name}`"
        return f"IDENTIFIER(:{key})"

    select = ", ".join(ident(c, f"c{i}") for i, c in enumerate(columns)) if columns else "*"
    sql = f"SELECT {select} FROM IDENTIFIER(:tbl)"

    where = []
    for i, f in enumerate(filters or []):
        op = f.op.upper().strip()
        if op not in OPS:
            raise ValueError(f"op '{f.op}' tidak didukung. Pilihan: {sorted(OPS)}")
        col = ident(f.column, f"f{i}")
        if op in ("IS NULL", "IS NOT NULL"):
            where.append(f"{col} {op}")
        elif op == "IN":
            values = f.value if isinstance(f.value, list) else [f.value]
            if not values or len(values) > MAX_IN_VALUES:
                raise ValueError(f"op IN butuh list berisi 1 sampai {MAX_IN_VALUES} value")
            keys = [f"v{i}_{j}" for j in range(len(values))]
            params.update({k: str(v) for k, v in zip(keys, values)})
            where.append(f"{col} IN ({', '.join(':' + k for k in keys)})")
        else:
            if f.value is None or isinstance(f.value, list):
                raise ValueError(f"op {op} butuh satu value")
            params[f"v{i}"] = str(f.value)
            where.append(f"{col} {op} :v{i}")
    if where:
        sql += " WHERE " + " AND ".join(where)

    if order_by:
        sql += f" ORDER BY {ident(order_by, 'o')}" + (" DESC" if desc else "")

    sql += f" LIMIT {clamp(limit)}"
    return sql, params


def run_query(sql: str, params: dict[str, str]) -> str:
    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
    if not warehouse_id:
        raise ValueError("DATABRICKS_SQL_WAREHOUSE_ID belum diset di server")
    # ponytail: semua parameter dikirim sebagai STRING, Databricks SQL meng-cast implisit
    # (period >= '2026-09-01', score > '0.5'). Tambah tipe per parameter kalau ada cast yang gagal.
    resp = ws().statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        parameters=[StatementParameterListItem(name=k, value=v) for k, v in params.items()],
        wait_timeout="30s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
        row_limit=max_rows(),
    )
    state = resp.status.state if resp.status else None
    if state != StatementState.SUCCEEDED:
        error = resp.status.error.message if resp.status and resp.status.error else "tanpa pesan"
        raise RuntimeError(f"Query {state.value if state else 'UNKNOWN'}: {error}")
    headers = [c.name for c in resp.manifest.schema.columns]
    rows = (resp.result.data_array if resp.result else None) or []
    return f"{len(rows)} baris\n" + md_table(headers, rows)


def query(full_name: str, **kwargs: Any) -> str:
    split_full_name(full_name)
    table = ws().tables.get(full_name=full_name)
    known = {c.name for c in table.columns or []}
    return run_query(*build_query(full_name, known, **kwargs))


@mcp.tool()
def preview_table(full_name: str, limit: int = 20) -> str:
    """Contoh baris dari satu tabel. full_name = catalog.schema.table. limit dibatasi MAX_ROWS server."""
    return query(full_name, limit=limit)


@mcp.tool()
def query_table(
    full_name: str,
    columns: list[str] | None = None,
    filters: list[Filter] | None = None,
    order_by: str | None = None,
    desc: bool = False,
    limit: int = 20,
) -> str:
    """Ambil baris tertentu dari satu tabel, read-only.

    columns: kolom yang diambil, kosong = semua.
    filters: digabung AND. op salah satu =, !=, <, <=, >, >=, IN, IS NULL, IS NOT NULL.
      Contoh: [{"column": "scope", "op": "=", "value": "production"},
               {"column": "pred_class", "op": "IN", "value": ["padi", "jagung"]}]
    order_by + desc: urutan. limit dibatasi MAX_ROWS server.
    Pakai describe_table dulu untuk tahu nama kolom yang sah.
    """
    return query(full_name, columns=columns, filters=filters, order_by=order_by, desc=desc, limit=limit)


# ---------- helper ----------


def ts(ms: int | None) -> str | None:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if ms else None


def duration(start_ms: int | None, end_ms: int | None) -> str | None:
    if not start_ms or not end_ms:
        return None
    minutes, seconds = divmod(int((end_ms - start_ms) / 1000), 60)
    return f"{minutes}m{seconds:02d}s"


def clamp(limit: int) -> int:
    return max(1, min(int(limit), max_rows()))


# ---------- mlflow ----------

RUN_TAGS = ("track", "model_version_label", "git_branch", "git_commit")


def experiment_prefix() -> str:
    return os.environ.get("ALLOWED_EXPERIMENT_PREFIX", "/Shared/mody/")


def normalize_experiment(path: str) -> str:
    """API MLflow menyimpan nama tanpa awalan /Workspace, jadi keduanya disamakan."""
    return path.removeprefix("/Workspace")


def check_experiment(name: str) -> None:
    prefix = normalize_experiment(experiment_prefix())
    path = normalize_experiment(name)
    if prefix and (not path.startswith(prefix) or ".." in path.split("/")):
        raise ValueError(f"Experiment '{name}' tidak diizinkan. Harus berawalan '{prefix}'")


@mcp.tool()
def search_runs(
    experiment_path: str,
    filter: str = "",
    order_by: str = "",
    metrics: list[str] | None = None,
    limit: int = 10,
) -> str:
    """Daftar run MLflow di satu experiment, terbaru dulu.

    experiment_path: misalnya /Workspace/Shared/mody/geo_commodity_classification.
    filter: sintaks MLflow, misalnya "tags.git_branch = 'agent/x'" atau "metrics.macro_f1 > 0.7".
    order_by: misalnya "metrics.macro_f1 DESC". Kosong = start_time terbaru.
    metrics: metrik yang ditampilkan, kosong = semua (dipotong kalau panjang).
    """
    check_experiment(experiment_path)
    experiment = ws().experiments.get_by_name(normalize_experiment(experiment_path)).experiment
    runs = islice(
        ws().experiments.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter=filter or None,
            order_by=[order_by] if order_by else None,
            max_results=clamp(limit),
        ),
        clamp(limit),
    )
    rows = []
    for r in runs:
        tags = {t.key: t.value for t in r.data.tags or []}
        values = {m.key: m.value for m in r.data.metrics or []}
        shown = metrics or sorted(values)
        rows.append([
            r.info.run_id, r.info.run_name, r.info.status.value if r.info.status else None, ts(r.info.start_time),
            *(tags.get(k) for k in RUN_TAGS),
            ", ".join(f"{k}={values[k]:.4g}" for k in shown if k in values),
        ])
    return f"{len(rows)} run di {experiment.name}\n" + md_table(
        ["run_id", "run_name", "status", "start", *RUN_TAGS, "metrics"], rows
    )


@mcp.tool()
def get_run(run_id: str) -> str:
    """Detail satu run MLflow: params, metrics, dan tags (tanpa tag sistem mlflow.*)."""
    run = ws().experiments.get_run(run_id).run
    experiment = ws().experiments.get_experiment(run.info.experiment_id).experiment
    check_experiment(experiment.name)
    params = sorted((p.key, p.value) for p in run.data.params or [])
    metrics = sorted((m.key, m.value) for m in run.data.metrics or [])
    tags = sorted((t.key, t.value) for t in run.data.tags or [] if not t.key.startswith("mlflow."))
    return (
        f"## run {run.info.run_id} ({cell(run.info.run_name)})\n"
        f"- experiment: {experiment.name}\n"
        f"- status: {cell(run.info.status.value if run.info.status else None)}, "
        f"start: {cell(ts(run.info.start_time))}, durasi: {cell(duration(run.info.start_time, run.info.end_time))}\n\n"
        f"### params ({len(params)})\n{md_table(['key', 'value'], params)}\n\n"
        f"### metrics ({len(metrics)})\n{md_table(['key', 'value'], metrics)}\n\n"
        f"### tags ({len(tags)})\n{md_table(['key', 'value'], tags)}"
    )


@mcp.tool()
def list_model_versions(full_name: str, limit: int = 20) -> str:
    """Versi model Unity Catalog beserta alias (misalnya champion), terbaru dulu. full_name = catalog.schema.model.

    run_id tiap versi bisa dipakai di get_run untuk melihat metrik versi tersebut.
    """
    split_full_name(full_name)
    model = ws().registered_models.get(full_name, include_aliases=True)
    aliases: dict[int, list[str]] = {}
    for a in model.aliases or []:
        aliases.setdefault(int(a.version_num), []).append(a.alias_name)
    versions = sorted(ws().model_versions.list(full_name), key=lambda v: int(v.version), reverse=True)
    rows = [
        [v.version, ", ".join(aliases.get(int(v.version), [])), v.run_id,
         v.status.value if v.status else None, ts(v.created_at), v.comment]
        for v in versions[: clamp(limit)]
    ]
    return f"{len(rows)} versi {full_name}\n" + md_table(
        ["version", "aliases", "run_id", "status", "created", "comment"], rows
    )


# ---------- jobs ----------

TRACE_TAIL = 3000
MARKUP = re.compile(r"<[^>]+>|\x1b\[[0-9;]*[A-Za-z]")  # tag HTML dan kode warna ANSI dari notebook


def plain(text: str | None) -> str:
    return MARKUP.sub("", text or "").strip()


def job_prefix() -> str:
    return os.environ.get("ALLOWED_JOB_PREFIX", "mody_")


def check_job_name(name: str) -> None:
    prefix = job_prefix()
    if prefix and not name.startswith(prefix):
        raise ValueError(f"Job '{name}' tidak diizinkan. Harus berawalan '{prefix}'")


def check_job_id(job_id: int | None) -> str:
    """Pastikan run berasal dari job yang diizinkan. Mengembalikan nama job."""
    if job_id is None:
        raise ValueError("Run ini bukan bagian dari job, tidak diizinkan")
    name = ws().jobs.get(job_id).settings.name
    check_job_name(name)
    return name


def run_state(state: Any) -> str | None:
    if not state:
        return None
    result = state.result_state.value if state.result_state else None
    return result or (state.life_cycle_state.value if state.life_cycle_state else None)


def job_params(run: Any) -> dict[str, str]:
    """Nilai efektif parameter job. value bernilai None kalau run memakai default."""
    return {p.name: p.value if p.value is not None else p.default for p in run.job_parameters or []}


@mcp.tool()
def list_job_runs(job_name: str, limit: int = 10) -> str:
    """Run terakhir satu job, misalnya mody_train_dev. Kolom overrides berisi parameter yang diubah dari default."""
    check_job_name(job_name)
    job = next(iter(ws().jobs.list(name=job_name)), None)
    if job is None:
        raise ValueError(f"Job '{job_name}' tidak ditemukan")
    runs = islice(ws().jobs.list_runs(job_id=job.job_id, limit=min(clamp(limit), 25)), clamp(limit))
    rows = [
        [r.run_id, run_state(r.state), ts(r.start_time), duration(r.start_time, r.end_time),
         r.trigger.value if r.trigger else None,
         ", ".join(f"{p.name}={p.value}" for p in r.job_parameters or [] if p.value not in (None, p.default))]
        for r in runs
    ]
    return f"{len(rows)} run {job_name}\n" + md_table(["run_id", "state", "start", "durasi", "trigger", "overrides"], rows)


@mcp.tool()
def get_job_run(run_id: int) -> str:
    """Status satu run job dan tiap task-nya. Pakai task_run_id dari sini di get_task_output untuk melihat error.

    Run yang di-repair menampilkan semua attempt. State run mengikuti repair terakhir.
    """
    run = ws().jobs.get_run(run_id, include_history=True)
    name = check_job_id(run.job_id)
    tasks = sorted(run.tasks or [], key=lambda t: (t.start_time or 0, t.attempt_number or 0))
    repairs = [h for h in run.repair_history or [] if h.type and h.type.value == "REPAIR"]
    repair_note = (
        f"{len(repairs)}x, attempt sebelumnya: "
        + ", ".join(cell(run_state(h.state)) for h in (run.repair_history or [])[:-1])
        if repairs else "-"
    )
    return (
        f"## {name} run {run.run_id}\n"
        f"- state: {cell(run_state(run.state))}, start: {cell(ts(run.start_time))}, "
        f"durasi: {cell(duration(run.start_time, run.end_time))}\n"
        f"- repair: {repair_note}\n"
        f"- message: {cell(run.state.state_message if run.state else None)}\n"
        f"- url: {cell(run.run_page_url)}\n\n"
        f"### params\n{md_table(['name', 'value'], sorted(job_params(run).items()))}\n\n"
        f"### tasks ({len(tasks)})\n"
        + md_table(
            ["task_key", "task_run_id", "attempt", "state", "start", "durasi"],
            [[t.task_key, t.run_id, t.attempt_number, run_state(t.state), ts(t.start_time),
              duration(t.start_time, t.end_time)] for t in tasks],
        )
    )


@mcp.tool()
def get_task_output(task_run_id: int) -> str:
    """Error dan stack trace satu task (task_run_id dari get_job_run). Trace dipotong ke bagian akhir."""
    out = ws().jobs.get_run_output(task_run_id)
    check_job_id(out.metadata.job_id if out.metadata else None)
    error = plain(out.error) or "(tidak ada error)"
    trace = plain(out.error_trace)
    if len(trace) > TRACE_TAIL:
        trace = "...\n" + trace[-TRACE_TAIL:]
    state = run_state(out.metadata.state if out.metadata else None)
    return (
        f"## task run {task_run_id}\n- state: {cell(state)}\n\n"
        f"### error\n{error}\n\n"
        f"### trace\n```\n{trace or '(kosong)'}\n```"
    )


# ---------- github ----------

GITHUB_API = "https://api.github.com"
DEFAULT_ALLOWED_PATHS = "usecases/*/*.yml,src/mody_ml/usecases/*/*.py,tests/unit/test_*.py,notebooks/training/*/*"
DENIED_PATHS = (".github/",)  # selalu ditolak, walau GITHUB_ALLOWED_PATHS diperluas
# File yang dieksekusi otomatis saat import/pytest/build. Selalu ditolak.
DENIED_NAMES = {"conftest.py", "__init__.py", "setup.py", "pyproject.toml"}
MAX_COMMIT_FILES = 20
MAX_COMMIT_CHARS = 500_000
SAFE_NAME = re.compile(r"[A-Za-z0-9._/-]+")
MAX_FILE_CHARS = 60_000
LOG_TIMESTAMP = re.compile(r"^\d{4}-\d\d-\d\dT[\d:.]+Z ", re.MULTILINE)


class GitHubError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


class FileChange(BaseModel):
    path: str
    content: str | None = None  # None = hapus file


def github_repo() -> str:
    repo = os.environ.get("GITHUB_REPO", "")
    if not SAFE_NAME.fullmatch(repo) or repo.count("/") != 1:
        raise ValueError("GITHUB_REPO belum diset di server (format owner/name)")
    return repo


def base_branch() -> str:
    return os.environ.get("GITHUB_BASE_BRANCH", "main")


def branch_prefix() -> str:
    return os.environ.get("GITHUB_BRANCH_PREFIX", "agent/")


def allowed_paths() -> list[str]:
    raw = os.environ.get("GITHUB_ALLOWED_PATHS", DEFAULT_ALLOWED_PATHS)
    return [p.strip() for p in raw.split(",") if p.strip()]


@cache
def gh() -> httpx.Client:
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise ValueError("GITHUB_TOKEN belum diset di server")
    # follow_redirects untuk log Actions; httpx membuang header Authorization saat redirect ke host lain.
    return httpx.Client(
        base_url=GITHUB_API,
        timeout=30,
        follow_redirects=True,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )


def gh_call(method: str, path: str, **kwargs: Any) -> httpx.Response:
    """Panggil REST API GitHub untuk GITHUB_REPO. path relatif terhadap /repos/<owner>/<name>."""
    resp = gh().request(method, f"/repos/{github_repo()}{path}", **kwargs)
    if resp.is_error:
        is_json = "json" in resp.headers.get("content-type", "")
        message = resp.json().get("message", "") if is_json else resp.text[:200]
        raise GitHubError(resp.status_code, f"GitHub {method} {path}: {resp.status_code} {message}")
    return resp


def check_relative(path: str) -> None:
    """Tolak path absolut, '..', dan karakter yang bisa mengubah URL API."""
    if not SAFE_NAME.fullmatch(path) or any(p in ("", ".", "..") for p in path.split("/")):
        raise ValueError(f"Path '{path}' tidak valid. Pakai path relatif dari root repo, misalnya src/mody_ml/usecases")


def is_writable(path: str) -> bool:
    # ponytail: '*' di fnmatch melewati '/', jadi kedalaman path wajib sama dengan pola. Butuh '**': PurePath.full_match (3.13).
    if path.startswith(DENIED_PATHS) or path.rsplit("/", 1)[-1] in DENIED_NAMES:
        return False
    return any(path.count("/") == p.count("/") and fnmatch.fnmatchcase(path, p) for p in allowed_paths())


def check_path(path: str) -> None:
    check_relative(path)
    if not is_writable(path):
        raise ValueError(f"Path '{path}' tidak boleh diubah agent. Yang diizinkan: {allowed_paths()}")


def check_branch(branch: str) -> None:
    prefix = branch_prefix()
    name = branch.removeprefix(prefix)
    if not branch.startswith(prefix) or not name or not SAFE_NAME.fullmatch(name) or ".." in branch:
        raise ValueError(f"Branch '{branch}' tidak diizinkan. Harus berawalan '{prefix}', misalnya {prefix}tune-lr")


def branch_sha(branch: str) -> str | None:
    try:
        return gh_call("GET", f"/git/ref/heads/{branch}").json()["object"]["sha"]
    except GitHubError as e:
        if e.status == 404:
            return None
        raise


def ref_params(ref: str) -> dict[str, str] | None:
    return {"ref": ref} if ref else None


@mcp.tool()
def github_list_files(path: str = "", ref: str = "") -> str:
    """Isi satu folder di repo. path kosong = root. ref = branch atau commit, kosong = branch utama.

    Kolom editable = yes kalau file boleh diubah lewat github_commit_files.
    """
    if path:
        check_relative(path)
    items = gh_call("GET", f"/contents/{path}", params=ref_params(ref)).json()
    if not isinstance(items, list):
        raise ValueError(f"'{path}' adalah file, pakai github_read_file")  # noqa: TRY004
    rows = [
        [i["type"], i["path"], i["size"] if i["type"] == "file" else None, "yes" if is_writable(i["path"]) else "no"]
        for i in items
    ]
    return f"{len(rows)} entri di /{path}\n" + md_table(["type", "path", "size", "editable"], rows)


@mcp.tool()
def github_read_file(path: str, ref: str = "") -> str:
    """Isi satu file teks di repo. ref = branch atau commit, kosong = branch utama. File panjang dipotong."""
    check_relative(path)
    item = gh_call("GET", f"/contents/{path}", params=ref_params(ref)).json()
    if isinstance(item, list):
        raise ValueError(f"'{path}' adalah folder, pakai github_list_files")  # noqa: TRY004
    if item.get("encoding") != "base64":
        raise ValueError(f"'{path}' terlalu besar untuk dibaca lewat API ({item.get('size')} byte)")
    try:
        text = base64.b64decode(item["content"]).decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError(f"'{path}' adalah file biner") from None
    note = f"\n... (dipotong, {len(text)} karakter)" if len(text) > MAX_FILE_CHARS else ""
    return (
        f"## {path} ({ref or base_branch()}, editable: {'yes' if is_writable(path) else 'no'})\n"
        f"```\n{text[:MAX_FILE_CHARS]}\n```{note}"
    )


@mcp.tool()
def github_create_branch(branch: str) -> str:
    """Buat branch agent dari ujung branch utama. Gagal kalau branch sudah ada (tidak pernah menimpa)."""
    check_branch(branch)
    if branch_sha(branch) is not None:
        raise ValueError(f"Branch '{branch}' sudah ada")
    sha = branch_sha(base_branch())
    if sha is None:
        raise ValueError(f"Branch utama '{base_branch()}' tidak ditemukan")
    gh_call("POST", "/git/refs", json={"ref": f"refs/heads/{branch}", "sha": sha})
    return f"Branch {branch} dibuat dari {base_branch()} ({sha[:12]})"

@mcp.tool()
def github_commit_files(branch: str, message: str, files: list[FileChange]) -> str:
    """Satu commit berisi satu atau beberapa file ke branch agent. Branch dibuat dari branch utama kalau belum ada.

    files: [{"path": "usecases/<uc>/usecase.yml", "content": "<isi lengkap file>"}]. content null = hapus file.
    Isi file ditulis utuh (bukan diff). Hanya path dengan editable = yes yang diterima.
    """
    check_branch(branch)
    paths = [f.path for f in files]
    if not files or len(set(paths)) != len(paths):
        raise ValueError("files harus berisi minimal satu file dan tiap path hanya sekali")
    if len(files) > MAX_COMMIT_FILES or sum(len(f.content or "") for f in files) > MAX_COMMIT_CHARS:
        raise ValueError(f"Maksimal {MAX_COMMIT_FILES} file dan {MAX_COMMIT_CHARS} karakter per commit")
    for path in paths:
        check_path(path)
    if not message.strip():
        raise ValueError("message commit wajib diisi")

    head = branch_sha(branch)
    is_new = head is None
    parent = head or branch_sha(base_branch())
    if parent is None:
        raise ValueError(f"Branch utama '{base_branch()}' tidak ditemukan")

    base_tree = gh_call("GET", f"/git/commits/{parent}").json()["tree"]["sha"]
    entries = [
        {"path": f.path, "mode": "100644", "type": "blob", **({"sha": None} if f.content is None else {"content": f.content})}
        for f in files
    ]
    tree = gh_call("POST", "/git/trees", json={"base_tree": base_tree, "tree": entries}).json()["sha"]
    commit = gh_call("POST", "/git/commits", json={"message": message, "tree": tree, "parents": [parent]}).json()
    if is_new:
        gh_call("POST", "/git/refs", json={"ref": f"refs/heads/{branch}", "sha": commit["sha"]})
    else:
        gh_call("PATCH", f"/git/refs/heads/{branch}", json={"sha": commit["sha"], "force": False})

    return (
        f"Commit {commit['sha'][:12]} ke {branch}{' (branch baru)' if is_new else ''}\n"
        f"- url: {commit.get('html_url')}\n\n"
        + md_table(["path", "aksi"], [[f.path, "hapus" if f.content is None else "tulis"] for f in files])
    )


def agent_pr(number: int) -> dict[str, Any]:
    """PR yang boleh diubah agent: dari branch agent di repo yang sama (bukan fork)."""
    pr = gh_call("GET", f"/pulls/{number}").json()
    check_branch(pr["head"]["ref"])
    if (pr["head"].get("repo") or {}).get("full_name") != github_repo():
        raise ValueError(f"PR #{number} berasal dari fork, tidak diizinkan")
    return pr


@mcp.tool()
def github_create_pr(branch: str, title: str, body: str) -> str:
    """Buka draft pull request dari branch agent ke branch utama. Ready for review, review, dan merge oleh manusia."""
    check_branch(branch)
    pr = gh_call(
        "POST", "/pulls", json={"head": branch, "base": base_branch(), "title": title, "body": body, "draft": True}
    ).json()
    return f"PR #{pr['number']} dibuka (draft): {pr['html_url']}"


@mcp.tool()
def github_update_pr(number: int, title: str | None = None, body: str | None = None) -> str:
    """Ubah judul atau deskripsi PR milik agent, misalnya menambah metrik dan run_id MLflow hasil training."""
    changes = {k: v for k, v in {"title": title, "body": body}.items() if v is not None}
    if not changes:
        raise ValueError("Isi title atau body")
    agent_pr(number)
    pr = gh_call("PATCH", f"/pulls/{number}", json=changes).json()
    return f"PR #{number} diperbarui: {pr['html_url']}"


@mcp.tool()
def github_pr_status(number: int) -> str:
    """Status PR dan check CI di commit terakhirnya. job_id check yang gagal dipakai di github_ci_log."""
    pr = agent_pr(number)
    checks = gh_call("GET", f"/commits/{pr['head']['sha']}/check-runs", params={"per_page": 50}).json()["check_runs"]
    return (
        f"## PR #{number} {cell(pr['title'])}\n"
        f"- state: {pr['state']}{' (draft)' if pr.get('draft') else ''}, merged: {cell(pr.get('merged'))}, "
        f"mergeable: {cell(pr.get('mergeable'))}\n"
        f"- branch: {pr['head']['ref']} -> {pr['base']['ref']}, head: {pr['head']['sha'][:12]}\n"
        f"- url: {pr['html_url']}\n\n"
        f"### checks ({len(checks)})\n"
        + md_table(["name", "status", "conclusion", "job_id"],
                   [[c["name"], c["status"], c.get("conclusion"), c["id"]] for c in checks])
    )


@mcp.tool()
def github_ci_log(job_id: int) -> str:
    """Bagian akhir log satu job GitHub Actions (job_id dari github_pr_status), tanpa timestamp dan kode warna."""
    check_branch(gh_call("GET", f"/actions/jobs/{job_id}").json().get("head_branch") or "")
    log = LOG_TIMESTAMP.sub("", plain(gh_call("GET", f"/actions/jobs/{job_id}/logs").text))
    if len(log) > TRACE_TAIL:
        log = "...\n" + log[-TRACE_TAIL:]
    return f"## log job {job_id}\n```\n{log or '(kosong)'}\n```"


# ---------- HTTP ----------


class BearerAuth:
    """Middleware ASGI: setiap request HTTP wajib membawa Authorization: Bearer <MCP_API_KEY>."""

    def __init__(self, app: Any, api_key: str):
        self.app = app
        self.expected = f"Bearer {api_key}".encode()

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "lifespan":
            got = dict(scope.get("headers") or []).get(b"authorization", b"")
            if not hmac.compare_digest(got, self.expected):
                await send({"type": "http.response.start", "status": 401, "headers": [(b"content-type", b"text/plain")]})
                await send({"type": "http.response.body", "body": b"unauthorized"})
                return
        await self.app(scope, receive, send)


def check_config(api_key: str) -> None:
    """Fail closed: pembatas yang kosong berarti semua diizinkan, jadi server menolak start."""
    if len(api_key) < MIN_API_KEY_LEN:
        raise SystemExit(f"MCP_API_KEY wajib diisi, minimal {MIN_API_KEY_LEN} karakter")
    if not allowed_catalogs():
        raise SystemExit("ALLOWED_CATALOGS wajib diisi")
    for name, value in (("ALLOWED_EXPERIMENT_PREFIX", experiment_prefix()), ("ALLOWED_JOB_PREFIX", job_prefix())):
        if not value.strip():
            raise SystemExit(f"{name} tidak boleh kosong")
    if not branch_prefix().strip() or base_branch().startswith(branch_prefix()):
        raise SystemExit("GITHUB_BRANCH_PREFIX tidak boleh kosong atau mencakup branch utama")


def create_app(api_key: str) -> BearerAuth:
    check_config(api_key)
    return BearerAuth(mcp.streamable_http_app(), api_key)


if __name__ == "__main__":
    import uvicorn

    # Handler sendiri: FastMCP memasang rich handler di root (multi-baris), SIEM butuh satu baris JSON.
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(message)s"))
    audit.addHandler(handler)
    audit.setLevel(logging.INFO)
    audit.propagate = False

    uvicorn.run(create_app(os.environ.get("MCP_API_KEY", "")), host=HOST, port=PORT)
