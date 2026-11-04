# Spesifikasi `mody-mcp`, MCP server Unity Catalog read-only

Tanggal: 2026-10-30. Status: draf, menunggu review.
Menggantikan sementara `2026-10-30-mody-mcp-design.md` (versi monitoring, ditunda).

## 1. Tujuan

Agent Mody berjalan di platform sendiri, di luar Databricks. Agent ini butuh dua jenis konteks
dari Databricks:
- Struktur data: catalog, schema, tabel, kolom, tipe, dan komentar.
- Isi data: contoh baris, atau baris tertentu yang disaring.

`mody-mcp` menyajikan keduanya sebagai tool MCP read-only lewat Streamable HTTP.

Polanya mengikuti `rafaelcartenet/mcp-databricks-server` dengan dua perubahan. Pertama, tidak ada
`execute_sql` bebas. Kedua, pembacaan data hanya lewat query bertemplate yang semua nilainya
dikirim sebagai parameter.

Di luar cakupan:
- SQL bebas.
- Agregasi (`GROUP BY`, `COUNT`) dan join.
- Lineage.
- Tool tulis, cache, dan rate limit.

## 2. Letak

```
test-MLOps_databricks/
├── ml-ops-databricks/     tidak diubah, tidak dipakai sebagai dependency
└── mody-mcp/
    ├── docs/
    ├── server.py          seluruh server
    ├── test_server.py     unit test tanpa Databricks
    ├── pyproject.toml
    └── .env.example
```

Ketergantungan:
- `mcp[cli]>=1.28,<2`. Memakai `FastMCP` dari v1. Versi v2 butuh Python 3.11.
- `databricks-sdk>=0.30`.

## 3. Konfigurasi

| Env | Wajib | Isi |
|---|---|---|
| `DATABRICKS_HOST` | ya | `https://<workspace>.cloud.databricks.com`, dibaca otomatis oleh SDK |
| `DATABRICKS_TOKEN` | ya | PAT, dibaca otomatis oleh SDK |
| `DATABRICKS_SQL_WAREHOUSE_ID` | ya | Warehouse untuk `preview_table` dan `query_table` |
| `MCP_API_KEY` | ya | Kunci bearer untuk agent Mody. Server menolak start kalau kosong |
| `ALLOWED_CATALOGS` | tidak | Daftar dipisah koma, misalnya `mody_dev`. Kosong berarti semua catalog yang bisa diakses token |
| `MAX_ROWS` | tidak | Batas baris per query. Default `100` |
| `HOST`, `PORT` | tidak | Default `0.0.0.0` dan `8000` |

Tiga env pertama sama dengan repo referensi.

Untuk pindah ke service principal `sp_agent`, ganti `DATABRICKS_TOKEN` dengan
`DATABRICKS_CLIENT_ID` dan `DATABRICKS_CLIENT_SECRET`. Kode tidak perlu diubah.

## 4. Tool

Output berupa Markdown ringkas.

### 4.1 Metadata (Unity Catalog API, tanpa warehouse)

| Tool | API SDK | Output |
|---|---|---|
| `list_catalogs()` | `w.catalogs.list()` | Nama dan komentar, sudah difilter `ALLOWED_CATALOGS` |
| `list_schemas(catalog)` | `w.schemas.list(catalog_name=)` | Nama dan komentar |
| `list_tables(catalog, schema, include_columns=False)` | `w.tables.list(catalog_name=, schema_name=)` | Nama, tipe tabel, komentar. Kalau `include_columns`, ditambah kolom (nama, tipe, komentar) |
| `describe_table(full_name)` | `w.tables.get(full_name=)` | Tipe tabel, format, owner, komentar, dan tabel kolom (nama, `type_text`, nullable, komentar) |

### 4.2 Data (Statement Execution API, butuh warehouse)

| Tool | Isi |
|---|---|
| `preview_table(full_name, limit=20)` | Contoh baris pertama dari tabel |
| `query_table(full_name, columns=None, filters=None, order_by=None, desc=False, limit=20)` | Baris yang disaring dan diurutkan |

Bentuk parameter `query_table`:

```json
{
  "full_name": "mody_dev.geo_commodity_classification.inference_log",
  "columns": ["period", "pred_class", "truth_class"],
  "filters": [
    {"column": "scope", "op": "=", "value": "production"},
    {"column": "period", "op": ">=", "value": "2026-09-01"}
  ],
  "order_by": "period",
  "desc": true,
  "limit": 50
}
```

- `columns`: kalau kosong, semua kolom diambil.
- `op`: salah satu dari `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `IS NULL`, `IS NOT NULL`. Untuk `IN`,
  `value` berupa list. Untuk `IS NULL` dan `IS NOT NULL`, `value` diabaikan.
- Antar filter digabung dengan `AND`. `OR` sengaja tidak didukung.
- `limit` dibatasi di rentang 1 sampai `MAX_ROWS`.

Cara query disusun:
1. `full_name` dicek dengan `w.tables.get`. Langkah ini sekaligus mengambil daftar kolom yang sah.
2. Setiap nama di `columns`, `filters[].column`, dan `order_by` wajib ada di daftar kolom itu.
   Kalau tidak ada, tool menolak sebelum query dikirim.
3. SQL disusun dari potongan tetap. Semua nama dan nilai dikirim sebagai parameter marker. Tidak
   ada input agent yang ditempel langsung ke teks SQL:
   ```sql
   SELECT IDENTIFIER(:c0), IDENTIFIER(:c1) FROM IDENTIFIER(:tbl)
   WHERE IDENTIFIER(:f0) = :v0 AND IDENTIFIER(:f1) >= :v1
   ORDER BY IDENTIFIER(:o) DESC
   LIMIT 50
   ```
   `IDENTIFIER` disebut aman dari SQL injection di dokumentasi Databricks SQL. Operator dipilih
   dari daftar di atas, bukan disalin dari input. Angka `LIMIT` dimasukkan setelah divalidasi
   sebagai integer.
4. Query dijalankan dengan `w.statement_execution.execute_statement(warehouse_id=...,
   statement=..., parameters=[StatementParameterListItem(...)], wait_timeout="30s")`.
5. Hasil dikembalikan sebagai tabel Markdown. Nilai yang panjangnya lebih dari 200 karakter
   dipotong, misalnya `proba_json` dan geometri.

`preview_table` sama dengan `query_table` tanpa `columns`, `filters`, dan `order_by`.

### 4.3 Validasi umum

- `catalog` dan catalog di `full_name` harus ada di `ALLOWED_CATALOGS` kalau env itu diisi.
- `full_name` harus tepat tiga bagian: `catalog.schema.table`.

## 5. Transport dan keamanan

- Server dibuat dengan `FastMCP("mody-mcp", stateless_http=True, json_response=True)`, endpoint
  `/mcp`.
- `mcp.streamable_http_app()` dibungkus middleware Starlette. Middleware membandingkan header
  `Authorization: Bearer <MCP_API_KEY>` memakai `hmac.compare_digest`. Kalau tidak cocok, server
  membalas 401.
- Server di-bind ke jaringan internal, tidak diekspos ke internet publik.
- PAT disimpan di secret manager platform, dengan masa berlaku pendek.
- Server hanya mengirim `SELECT` hasil template. Walaupun PAT punya hak tulis, server tidak
  menyediakan jalur untuk menulis data.
- Data yang dibaca agent masuk ke konteks LLM. Kalau ada tabel berisi data sensitif,
  `ALLOWED_CATALOGS` dan grant `SELECT` adalah batasnya.

Grant minimum untuk produksi (`sp_agent`):

```sql
GRANT USE CATALOG ON CATALOG mody_dev TO `<application-id sp_agent>`;
GRANT USE SCHEMA, SELECT ON SCHEMA mody_dev.geo_commodity_classification TO `<application-id sp_agent>`;
```

Ditambah izin `CAN USE` pada SQL warehouse.

## 6. Penanganan error

| Kondisi | Pesan ke agent |
|---|---|
| Catalog di luar `ALLOWED_CATALOGS` | Daftar catalog yang diizinkan |
| `full_name` tidak berformat tiga bagian | Contoh format yang benar |
| Kolom tidak dikenal atau `op` tidak valid | Daftar kolom atau operator yang sah |
| `NotFound` | "<objek> tidak ditemukan" |
| `PermissionDenied` | "Token tidak punya akses ke <objek>" |
| Statement `FAILED` atau timeout | State dan pesan error dari warehouse |
| Hasil kosong | "0 baris" |

Stack trace hanya ditulis ke log server.

## 7. Pengujian

`test_server.py` memakai pytest, tanpa koneksi ke Databricks. `WorkspaceClient` diganti objek palsu.
- Filter `ALLOWED_CATALOGS` dan penolakan catalog di luar daftar.
- `full_name` yang bukan tiga bagian ditolak.
- Format Markdown dari `TableInfo` palsu.
- Penyusun query `query_table`:
  - SQL hanya berisi `IDENTIFIER(:x)` dan `:v`, tanpa nilai dari input.
  - Kolom tidak dikenal ditolak.
  - `op` di luar daftar ditolak.
  - `limit` dibatasi `MAX_ROWS`.
  - `IN` dan `IS NULL` tersusun benar.
- Middleware: tanpa header 401, header salah 401, header benar diteruskan.

Uji manual ke workspace dev:
1. Jalankan `python server.py`.
2. Jalankan `bunx @modelcontextprotocol/inspector`.
3. Sambungkan ke `http://localhost:8000/mcp` dengan header bearer.
4. Panggil keenam tool untuk `mody_dev.geo_commodity_classification`.

## 8. Risiko terbuka

1. Pemilik PAT perlu akses ke `mody_dev` (`USE CATALOG`, `USE SCHEMA`, `SELECT`) dan `CAN USE` pada
   warehouse. Unit test tidak terpengaruh.
2. Belum terbukti SQL warehouse bisa membaca tabel Managed Iceberg di `mody_dev` yang tertaut ke
   Snowflake. Job repo membacanya lewat Spark. Uji pertama adalah `preview_table` ke
   `inference_log`. Kalau gagal, tool data tidak bisa dipakai, sedangkan tool metadata tetap jalan.
3. Belum dicek apakah `tables.get` mengembalikan kolom untuk tabel Iceberg tertaut. Kalau kolom
   kosong, validasi kolom di `query_table` akan menolak semua query. Ini dicek lewat
   `describe_table` saat uji manual.
4. `IDENTIFIER` dengan parameter marker butuh Databricks SQL (warehouse) yang mendukungnya. Ini
   akan terkonfirmasi dari uji manual.
5. Output `include_columns=True` pada schema besar bisa panjang. Untuk sementara dibiarkan.
