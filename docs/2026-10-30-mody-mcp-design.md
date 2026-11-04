# Spesifikasi `mody-mcp`, MCP server monitoring Mody

Tanggal: 2026-10-30. Status: ditunda. Yang dikerjakan sekarang: `2026-10-30-mody-mcp-generik-design.md`.

## 1. Tujuan

Agent Mody (berjalan di platform sendiri, di luar Databricks) butuh konteks monitoring model:
status monitor, metrik terbaru, drift, alert aktif, dan struktur tabel. `mody-mcp` menyajikan itu
sebagai tool MCP read-only lewat Streamable HTTP.

`mody-mcp` bukan Unity Catalog. Ia klien yang membaca Unity Catalog dan SQL warehouse, lalu
menambahkan pengetahuan domain yang hanya ada di repo `ml-ops-databricks`: pemetaan use case ke
tabel, aturan alert di `usecase.yml`, dan arti kolom `window`.

Di luar cakupan: SQL bebas, lineage, tool tulis, cache, rate limit, banyak workspace.

## 2. Letak dan ketergantungan

```
test-MLOps_databricks/
├── ml-ops-databricks/     repo lifecycle, tidak diubah
└── mody-mcp/              semua pekerjaan MCP
    ├── docs/              spesifikasi ini
    ├── server.py          seluruh server
    ├── test_server.py     unit test tanpa Databricks
    ├── pyproject.toml
    └── .env.example
```

`mody-mcp` memakai ulang kode `mody_ml` (dipasang editable dari `../ml-ops-databricks`, tanpa extra
`databricks`, sehingga tidak menarik Spark atau MLflow):

| Dari `mody_ml` | Dipakai untuk |
|---|---|
| `config.load_config(usecase, env)`, `config.ENVS` | Nama catalog, `inference_table`, aturan alert |
| `lifecycle.monitor.profiling.InferenceMonitor.metric_tables` | Nama tabel `_profile_metrics` dan `_drift_metrics` |
| `lifecycle.monitor.alerts.load_rules`, `evaluate_rules` | Hitung alert, logika sama dengan job |

`load_config` membaca `conf/` dan `usecases/`. Lokasinya lewat env `MODY_ROOT`. Dengan begitu nama
tabel selalu sama dengan yang dipakai job.

Ketergantungan baru: `mcp[cli]>=1.28,<2` (FastMCP, jalan di Python 3.10, v2 butuh 3.11) dan
`databricks-sdk>=0.30`. Env dibaca dari platform, jadi `python-dotenv` tidak dipakai.

## 3. Konfigurasi

| Env | Wajib | Isi |
|---|---|---|
| `DATABRICKS_HOST` | ya | `https://<workspace>.cloud.databricks.com`, dibaca otomatis oleh SDK |
| `DATABRICKS_TOKEN` | ya | PAT, dibaca otomatis oleh SDK |
| `DATABRICKS_SQL_WAREHOUSE_ID` | ya | Warehouse untuk query metrik |
| `MCP_API_KEY` | ya | Kunci bearer untuk agent Mody. Server menolak start bila kosong |
| `MODY_ROOT` | ya | Path ke repo `ml-ops-databricks` |
| `HOST`, `PORT` | tidak | Default `0.0.0.0`, `8000` |

Pindah dari PAT ke service principal `sp_agent`: ganti `DATABRICKS_TOKEN` dengan
`DATABRICKS_CLIENT_ID` dan `DATABRICKS_CLIENT_SECRET`. Kode tidak berubah.

## 4. Tool

Semua tool menerima `usecase` dan `env`. `env` harus anggota `ENVS` (`dev`, `stg`, `prod`).
`usecase` harus nama folder di `MODY_ROOT/usecases/`. Agent tidak pernah mengirim nama tabel
atau SQL. Output Markdown ringkas.

| Tool | Sumber | Warehouse |
|---|---|---|
| `get_monitor_status(usecase, env)` | `w.quality_monitors.get(inference_table)`: status, refresh terakhir (`list_refreshes`), dashboard | tidak |
| `get_latest_metrics(usecase, env)` | `_profile_metrics`, window terbaru, `column_name = ':table'`, `slice_key IS NULL`, plus baris per `slice_key`/`slice_value` | ya |
| `get_drift(usecase, env)` | `_drift_metrics`, window terbaru, per kolom | ya |
| `get_active_alerts(usecase, env)` | `load_rules(cfg)` lalu query yang sama dengan `check_alerts`, dievaluasi `evaluate_rules` | ya |
| `describe_table(usecase, env, table)` | `w.tables.get`. `table` hanya `inference`, `profile_metrics`, atau `drift_metrics` | tidak |

Catatan per tool:
- Semua output metrik menyebut bahwa `window` adalah bulan job dijalankan (`predicted_at`), bukan
  bulan data. Bulan data ada di slice `period`.
- `get_active_alerts` saat ini selalu "tidak ada aturan", karena `alerts: []` di `usecase.yml`.
  Itu perilaku benar, bukan error.
- Kolom yang dipilih di `get_latest_metrics` dan `get_drift` ditentukan saat implementasi dengan
  `describe_table` ke tabel nyata, tidak ditebak. Query memakai `LIMIT` untuk menjaga ukuran output.

Query memakai `w.statement_execution.execute_statement(warehouse_id, statement, wait_timeout="30s")`.
Nama tabel disisipkan dari `Config`, bukan dari input agent, jadi aman dari injection.
Bila status bukan `SUCCEEDED`, tool mengembalikan pesan singkat.

## 5. Transport dan keamanan

- `FastMCP("mody-mcp", stateless_http=True, json_response=True)`, endpoint `/mcp`.
- App ASGI dari `mcp.streamable_http_app()` dibungkus middleware Starlette yang membandingkan
  `Authorization: Bearer <MCP_API_KEY>` dengan `hmac.compare_digest`. Gagal: HTTP 401.
- Server di-bind ke jaringan internal platform, tidak diekspos publik.
- PAT disimpan di secret manager platform, masa berlaku pendek (disarankan 30 hari).
- Tidak ada SQL bebas. Walau PAT punya hak tulis, server hanya menjalankan `SELECT` template.

## 6. Penanganan error

| Kondisi | Pesan ke agent |
|---|---|
| `usecase`/`env` tidak valid | Daftar nilai yang valid |
| Monitor belum dibuat (`NotFound`) | "Monitor belum dibuat untuk <tabel>. Jalankan stage monitor." |
| Izin ditolak (`PermissionDenied`) | "Token tidak punya akses ke <objek>" |
| Statement gagal atau timeout | State dan pesan error dari warehouse |
| Tabel metrik kosong | "Belum ada window metrik" |

Stack trace hanya di log server.

## 7. Pengujian

`test_server.py`, pytest, tanpa koneksi Databricks. `WorkspaceClient` diganti objek palsu.
- Validasi `env` dan `usecase` menolak nilai di luar daftar.
- `describe_table` menolak `table` di luar tiga pilihan.
- Format Markdown dari hasil statement palsu.
- Middleware: tanpa header 401, header salah 401, header benar diteruskan.

Uji manual ke workspace dev: jalankan server, sambungkan MCP Inspector
(`bunx @modelcontextprotocol/inspector`) ke `http://localhost:8000/mcp` dengan header bearer, panggil
kelima tool untuk `geo_commodity_classification` di `dev`.

## 8. Risiko terbuka

1. Belum terbukti SQL warehouse bisa membaca tabel metrik di `mody_dev` (Managed Iceberg tertaut
   Snowflake). Job membacanya lewat Spark. Cek pertama sebelum implementasi:
   `SELECT * FROM mody_dev.geo_commodity_classification.inference_log_profile_metrics LIMIT 1`
   di SQL editor dengan warehouse yang sama.
2. Nama kolom metrik (mis. `f1_score.macro`, `js_distance`) belum diverifikasi ke tabel nyata.
3. `mody_ml` jadi ketergantungan. Perubahan `Config` atau `InferenceMonitor` di repo lifecycle bisa
   memecah server. Unit test menangkap perubahan nama fungsi.
