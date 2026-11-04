# mody-mcp

MCP server read-only untuk Unity Catalog, MLflow, dan Jobs Databricks, lewat Streamable HTTP di `/mcp`.
Spesifikasi lengkap ada di `docs/2026-10-30-mody-mcp-generik-design.md`.

| Tool | Isi | Warehouse |
|---|---|---|
| `list_catalogs()` | Daftar catalog | tidak |
| `list_schemas(catalog)` | Daftar schema | tidak |
| `list_tables(catalog, schema, include_columns=False)` | Daftar tabel, opsional dengan kolom | tidak |
| `describe_table(full_name)` | Kolom, tipe, komentar | tidak |
| `preview_table(full_name, limit=20)` | Contoh baris | ya |
| `query_table(full_name, columns, filters, order_by, desc, limit)` | Baris yang disaring | ya |
| `search_runs(experiment_path, filter, order_by, metrics, limit)` | Run MLflow, tag git, dan metrik | tidak |
| `get_run(run_id)` | Params, metrics, tags satu run | tidak |
| `list_model_versions(full_name, limit)` | Versi model UC dan alias (`champion`) | tidak |
| `list_job_runs(job_name, limit)` | Run terakhir satu job | tidak |
| `get_job_run(run_id)` | Status run dan tiap task | tidak |
| `get_task_output(task_run_id)` | Error dan stack trace task | tidak |

Tool MLflow dibatasi `ALLOWED_EXPERIMENT_PREFIX`, tool job dibatasi `ALLOWED_JOB_PREFIX`.

### GitHub (satu repo, `GITHUB_REPO`)

| Tool | Isi |
|---|---|
| `github_list_files(path, ref)` | Isi folder, dengan kolom `editable` |
| `github_read_file(path, ref)` | Isi file teks |
| `github_create_branch(branch)` | Branch `agent/*` baru dari `GITHUB_BASE_BRANCH`, tidak menimpa yang sudah ada |
| `github_commit_files(branch, message, files)` | Satu commit berisi beberapa file ke branch `agent/*` |
| `github_create_pr(branch, title, body)` | Draft PR dari branch agent ke `GITHUB_BASE_BRANCH` |
| `github_update_pr(number, title, body)` | Ubah PR milik agent |
| `github_pr_status(number)` | State PR dan check CI |
| `github_ci_log(job_id)` | Bagian akhir log job Actions |

Pembatas tulis: branch harus `GITHUB_BRANCH_PREFIX`, path harus cocok `GITHUB_ALLOWED_PATHS` dengan kedalaman folder
yang sama, `.github/` dan `conftest.py`/`__init__.py`/`setup.py`/`pyproject.toml` selalu ditolak, maksimal 20 file
atau 500.000 karakter per commit, update ref tanpa force, PR dari fork ditolak, PR selalu draft.
`github_pr_status` dan `github_ci_log` hanya untuk branch agent. Merge tetap oleh manusia (tidak ada tool merge).

Server menolak start kalau `MCP_API_KEY` kurang dari 32 karakter, atau `ALLOWED_CATALOGS`,
`ALLOWED_EXPERIMENT_PREFIX`, `ALLOWED_JOB_PREFIX`, `GITHUB_BRANCH_PREFIX` kosong.
`HOST` default `127.0.0.1`. Untuk akses remote, set `HOST` dan pasang reverse proxy TLS di depannya.

## Menjalankan

1. Isi env. Lihat `.env.example` untuk daftar lengkap.
2. Pasang dependency:
   ```bash
   uv sync
   ```
3. Jalankan server dengan `.env`:
   ```bash
   uv run --env-file .env python server.py
   ```

## Mencoba dengan MCP Inspector

```bash
bunx @modelcontextprotocol/inspector
```

Isi koneksi di Inspector:
- Transport: Streamable HTTP
- URL: `http://localhost:8000/mcp`
- Header: `Authorization: Bearer <MCP_API_KEY>`

## Tes

```bash
uv run pytest
```

Tes tidak butuh koneksi ke Databricks.
