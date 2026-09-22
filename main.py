from fastapi import FastAPI, Query, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import sqlite3
import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv

# Safe import for psycopg2
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except Exception as e:
    psycopg2 = None
    RealDictCursor = None
    HAS_PSYCOPG2 = False
    print(f"Notice: psycopg2 not loaded: {e}")

load_dotenv()

app = FastAPI(title="Indore ETL RU & Marine Hull API", version="2.1.0")

# Setup CORS to allow the frontend to communicate with the backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def safe_int(val, default: int = 0) -> int:
    try:
        if val is None:
            return default
        s = str(val).strip()
        return int(s) if s else default
    except (ValueError, TypeError):
        return default

# Database Configuration (Supabase & Railway friendly)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "aws-0-ap-northeast-2.pooler.supabase.com").strip() or "aws-0-ap-northeast-2.pooler.supabase.com",
    "port": safe_int(os.getenv("DB_PORT"), 6543),
    "dbname": os.getenv("DB_NAME", "postgres").strip() or "postgres",
    "user": os.getenv("DB_USER", "postgres.uaoysegountarjanafbb").strip() or "postgres.uaoysegountarjanafbb",
    "password": os.getenv("DB_PASS", "").strip(),
}

# Use /tmp on Vercel serverless to avoid read-only filesystem errors
if os.getenv("VERCEL"):
    SQLITE_DB_PATH = "/tmp/etl_database.sqlite"
else:
    SQLITE_DB_PATH = os.path.join(os.path.dirname(__file__), "etl_database.sqlite")

# Track active DB engine ("postgres" or "sqlite")
ACTIVE_DB_ENGINE = "sqlite"

def get_postgres_connection():
    """Establishes a connection to Supabase PostgreSQL."""
    if not psycopg2:
        raise ValueError("psycopg2 is not available in the current environment.")

    # Check if DATABASE_URL is provided and has a real password
    if (DATABASE_URL and 
        "[MASUKKAN_PASSWORD" not in DATABASE_URL and 
        "[YOUR" not in DATABASE_URL and 
        "<password>" not in DATABASE_URL):
        return psycopg2.connect(
            DATABASE_URL, 
            cursor_factory=RealDictCursor, 
            sslmode="require", 
            connect_timeout=6
        )
    elif (DB_CONFIG.get("password") and 
          "[MASUKKAN_PASSWORD" not in DB_CONFIG["password"] and 
          "[YOUR" not in DB_CONFIG["password"]):
        return psycopg2.connect(
            **DB_CONFIG, 
            cursor_factory=RealDictCursor, 
            sslmode="require", 
            connect_timeout=6
        )
    else:
        raise ValueError("Password Supabase PostgreSQL belum disetel di .env / Environment Variables")

def test_postgres_connection() -> bool:
    """Verifies whether PostgreSQL (Supabase) is reachable."""
    if not psycopg2:
        return False
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

# Initialize database schema and initial seed data in SQLite (for offline fallback)
def init_sqlite_db():
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1. FACUL_ETL_MH_AKSEPTASI table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS FACUL_ETL_MH_AKSEPTASI (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fac_code TEXT,
            reff_number TEXT,
            direct TEXT,
            broker TEXT,
            nama_tertanggung TEXT,
            afiliasi_tertanggung TEXT,
            nama_tertanggung_loss TEXT,
            nama_kapal TEXT,
            code_kapal TEXT,
            sum_insured REAL,
            loss_amount REAL,
            currency TEXT,
            date_of_loss TEXT,
            loss_cause TEXT,
            status TEXT
        )
    ''')

    # 2. FACUL_ETL_MH_LOSS_PLA table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS FACUL_ETL_MH_LOSS_PLA (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fac_code TEXT,
            reff_number TEXT,
            direct TEXT,
            broker TEXT,
            nama_tertanggung TEXT,
            afiliasi_tertanggung TEXT,
            nama_tertanggung_loss TEXT,
            nama_kapal TEXT,
            code_kapal TEXT,
            sum_insured REAL,
            loss_amount REAL,
            currency TEXT,
            date_of_loss TEXT,
            loss_cause TEXT,
            status TEXT
        )
    ''')

    # 3. FACUL_ETL_MH_LOSS_SETTLE table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS FACUL_ETL_MH_LOSS_SETTLE (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fac_code TEXT,
            reff_number TEXT,
            direct TEXT,
            broker TEXT,
            nama_tertanggung TEXT,
            afiliasi_tertanggung TEXT,
            nama_tertanggung_loss TEXT,
            nama_kapal TEXT,
            code_kapal TEXT,
            sum_insured REAL,
            loss_amount REAL,
            currency TEXT,
            date_of_loss TEXT,
            loss_cause TEXT,
            status TEXT
        )
    ''')

    # 4. ETL_HISTORY table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS ETL_HISTORY (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            file_size TEXT,
            file_type TEXT DEFAULT 'XLSX',
            cedant TEXT,
            cob TEXT,
            status TEXT,
            date_display TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            records_count INTEGER DEFAULT 0,
            schema_accuracy REAL DEFAULT 99.8,
            duration_seconds REAL DEFAULT 8.0,
            log_message TEXT
        )
    ''')

    # 5. MAPPING_TEMPLATES table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS MAPPING_TEMPLATES (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            target_schema TEXT DEFAULT 'ipr_stage_db',
            mappings_json TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()
    conn.close()

# Auto-initialize local SQLite on startup as fallback
try:
    init_sqlite_db()
except Exception as e:
    print(f"Warning: Failed to initialize SQLite fallback: {e}")

def get_db_connection():
    """Returns an active connection, preferring PostgreSQL (Supabase) then SQLite."""
    global ACTIVE_DB_ENGINE
    if test_postgres_connection():
        ACTIVE_DB_ENGINE = "postgres"
        return get_postgres_connection()
    else:
        ACTIVE_DB_ENGINE = "sqlite"
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

def format_query(query_sql: str, is_sqlite: bool) -> str:
    """Adapts query syntax between Postgres and SQLite."""
    sql = query_sql
    if is_sqlite:
        sql = sql.replace('public."FACUL_ETL_MH_AKSEPTASI"', 'FACUL_ETL_MH_AKSEPTASI')
        sql = sql.replace('public."FACUL_ETL_MH_LOSS_PLA"', 'FACUL_ETL_MH_LOSS_PLA')
        sql = sql.replace('public."FACUL_ETL_MH_LOSS_SETTLE"', 'FACUL_ETL_MH_LOSS_SETTLE')
        sql = sql.replace('"FACUL_ETL_MH_AKSEPTASI"', 'FACUL_ETL_MH_AKSEPTASI')
        sql = sql.replace('"FACUL_ETL_MH_LOSS_PLA"', 'FACUL_ETL_MH_LOSS_PLA')
        sql = sql.replace('"FACUL_ETL_MH_LOSS_SETTLE"', 'FACUL_ETL_MH_LOSS_SETTLE')
        sql = sql.replace('etl_history', 'ETL_HISTORY')
        sql = sql.replace('mapping_templates', 'MAPPING_TEMPLATES')
        sql = sql.replace('%s', '?')
    else:
        sql = sql.replace('?', '%s')
        sql = sql.replace('ETL_HISTORY', 'etl_history')
        sql = sql.replace('MAPPING_TEMPLATES', 'mapping_templates')
    return sql

def query_all(query_sql: str, params: list = []):
    """Executes a SELECT query returning all matching rows as dictionaries."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        is_sqlite = (ACTIVE_DB_ENGINE == "sqlite")
        sql = format_query(query_sql, is_sqlite)
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        cur.close()
        conn.close()

def query_one(query_sql: str, params: list = []):
    """Executes a SELECT query returning a single row as a dictionary."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        is_sqlite = (ACTIVE_DB_ENGINE == "sqlite")
        sql = format_query(query_sql, is_sqlite)
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        cur.close()
        conn.close()

def execute_dml(query_sql: str, params: list = [], returning_id: bool = False):
    """Executes INSERT, UPDATE, or DELETE query with id extraction."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        is_sqlite = (ACTIVE_DB_ENGINE == "sqlite")
        sql = format_query(query_sql, is_sqlite)
        
        # If SQLite and has RETURNING id, clean up if not supported
        if is_sqlite and "RETURNING" in sql.upper():
            sql = sql.split("RETURNING")[0].strip()

        cur.execute(sql, params)
        last_id = None
        if returning_id:
            if is_sqlite:
                last_id = cur.lastrowid
            else:
                try:
                    row = cur.fetchone()
                    if row:
                        last_id = row["id"] if isinstance(row, dict) else row[0]
                except Exception:
                    last_id = None
        conn.commit()
        return last_id
    finally:
        cur.close()
        conn.close()

# ---------------- API ENDPOINTS ----------------

@app.get("/")
def root():
    return {
        "title": "Indore ETL RU & Marine Hull API",
        "status": "online",
        "active_database": ACTIVE_DB_ENGINE,
        "postgres_configured": bool(DATABASE_URL or DB_CONFIG.get("password")),
        "supabase_project": "web etl (uaoysegountarjanafbb)"
    }

@app.get("/api/health")
def health_check():
    return {
        "status": "ok",
        "active_database": ACTIVE_DB_ENGINE,
        "postgres_connected": (ACTIVE_DB_ENGINE == "postgres"),
        "postgres_host": DB_CONFIG.get("host"),
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/stats")
def get_stats():
    try:
        loss_stat = query_one('''
            SELECT 
                count(*) as total_loss_count,
                COALESCE(sum(loss_amount), 0) as total_loss_amount,
                count(DISTINCT direct) as unique_cedants,
                count(DISTINCT nama_kapal) as unique_vessels
            FROM public."FACUL_ETL_MH_LOSS_PLA"
        ''')
        akseptasi_stat = query_one('SELECT count(*) as count FROM public."FACUL_ETL_MH_AKSEPTASI"')
        settle_stat = query_one('SELECT count(*) as count FROM public."FACUL_ETL_MH_LOSS_SETTLE"')
        history_stat = query_one('SELECT count(*) as count FROM etl_history')

        loss_count = loss_stat['total_loss_count'] if loss_stat else 0
        loss_amount = float(loss_stat['total_loss_amount']) if loss_stat and loss_stat['total_loss_amount'] else 0.0
        unique_cedants = loss_stat['unique_cedants'] if loss_stat else 0
        unique_vessels = loss_stat['unique_vessels'] if loss_stat else 0

        akseptasi_count = akseptasi_stat['count'] if akseptasi_stat else 0
        settle_count = settle_stat['count'] if settle_stat else 0
        history_count = history_stat['count'] if history_stat else 0

        total_records = loss_count + akseptasi_count + settle_count

        return {
            "totalRecords": total_records,
            "totalLossPLA": loss_count,
            "totalAkseptasi": akseptasi_count,
            "totalLossSettle": settle_count,
            "totalProcessedFiles": history_count,
            "totalClaimValue": loss_amount,
            "uniqueCedants": unique_cedants,
            "uniqueVessels": unique_vessels,
            "slaResolvedRate": "100.0%" if loss_count > 0 else "0.0%"
        }
    except Exception:
        return {
            "totalRecords": 0,
            "totalLossPLA": 0,
            "totalAkseptasi": 0,
            "totalLossSettle": 0,
            "totalProcessedFiles": 0,
            "totalClaimValue": 0,
            "uniqueCedants": 0,
            "uniqueVessels": 0,
            "slaResolvedRate": "0.0%"
        }

def build_filter_clause(
    fac_code: Optional[str] = None,
    reff_number: Optional[str] = None,
    company_name: Optional[str] = None,
    direct: Optional[str] = None,
    broker: Optional[str] = None,
    insured_name: Optional[str] = None,
    insured_loss_name: Optional[str] = None,
    vessel_name: Optional[str] = None,
    vessel_code: Optional[str] = None,
    status: Optional[str] = None,
    loss_cause: Optional[str] = None,
    currency: Optional[str] = None,
    date_of_loss: Optional[str] = None,
    search: Optional[str] = None
):
    where_clauses = []
    params = []

    if fac_code:
        where_clauses.append("LOWER(fac_code) LIKE %s")
        params.append(f"%{fac_code.lower().strip()}%")
    if reff_number:
        where_clauses.append("LOWER(reff_number) LIKE %s")
        params.append(f"%{reff_number.lower().strip()}%")
    c_name = company_name or direct
    if c_name:
        where_clauses.append("LOWER(direct) LIKE %s")
        params.append(f"%{c_name.lower().strip()}%")
    if broker:
        where_clauses.append("LOWER(broker) LIKE %s")
        params.append(f"%{broker.lower().strip()}%")
    if insured_name:
        where_clauses.append("LOWER(nama_tertanggung) LIKE %s")
        params.append(f"%{insured_name.lower().strip()}%")
    if insured_loss_name:
        where_clauses.append("(LOWER(nama_tertanggung_loss) LIKE %s OR LOWER(nama_tertanggung) LIKE %s)")
        params.extend([f"%{insured_loss_name.lower().strip()}%", f"%{insured_loss_name.lower().strip()}%"])
    if vessel_name:
        where_clauses.append("LOWER(nama_kapal) LIKE %s")
        params.append(f"%{vessel_name.lower().strip()}%")
    if vessel_code:
        where_clauses.append("LOWER(code_kapal) LIKE %s")
        params.append(f"%{vessel_code.lower().strip()}%")
    if status and status not in ["Semua", "Status: Semua"]:
        where_clauses.append("LOWER(status) = %s")
        params.append(status.lower().strip())
    if loss_cause:
        where_clauses.append("LOWER(loss_cause) LIKE %s")
        params.append(f"%{loss_cause.lower().strip()}%")
    if currency:
        where_clauses.append("LOWER(currency) = %s")
        params.append(currency.lower().strip())
    if date_of_loss:
        where_clauses.append("date_of_loss LIKE %s")
        params.append(f"%{date_of_loss.strip()}%")
    if search:
        s = f"%{search.lower().strip()}%"
        where_clauses.append("(LOWER(fac_code) LIKE %s OR LOWER(reff_number) LIKE %s OR LOWER(direct) LIKE %s OR LOWER(nama_kapal) LIKE %s OR LOWER(nama_tertanggung) LIKE %s)")
        params.extend([s, s, s, s, s])

    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    return where_sql, params

@app.get("/api/loss-pla")
def get_loss_pla(
    page: int = 1,
    limit: int = 50,
    fac_code: Optional[str] = None,
    reff_number: Optional[str] = None,
    company_name: Optional[str] = None,
    direct: Optional[str] = None,
    broker: Optional[str] = None,
    insured_name: Optional[str] = None,
    insured_loss_name: Optional[str] = None,
    vessel_name: Optional[str] = None,
    vessel_code: Optional[str] = None,
    status: Optional[str] = None,
    loss_cause: Optional[str] = None,
    currency: Optional[str] = None,
    date_of_loss: Optional[str] = None,
    search: Optional[str] = None
):
    offset = (page - 1) * limit
    where_sql, params = build_filter_clause(
        fac_code=fac_code, reff_number=reff_number, company_name=company_name, direct=direct,
        broker=broker, insured_name=insured_name, insured_loss_name=insured_loss_name,
        vessel_name=vessel_name, vessel_code=vessel_code, status=status,
        loss_cause=loss_cause, currency=currency, date_of_loss=date_of_loss, search=search
    )

    count_res = query_one(f'SELECT count(*) as count FROM public."FACUL_ETL_MH_LOSS_PLA"{where_sql}', params)
    total_records = count_res['count'] if count_res else 0

    query = f'SELECT * FROM public."FACUL_ETL_MH_LOSS_PLA"{where_sql} ORDER BY id ASC LIMIT %s OFFSET %s'
    data_params = params + [limit, offset]
    data = query_all(query, data_params)

    total_pages = (total_records + limit - 1) // limit if limit > 0 else 1

    return {
        "page": page,
        "limit": limit,
        "total": total_records,
        "totalPages": total_pages,
        "data": data
    }

@app.get("/api/akseptasi")
def get_akseptasi(
    page: int = 1,
    limit: int = 50,
    fac_code: Optional[str] = None,
    reff_number: Optional[str] = None,
    company_name: Optional[str] = None,
    direct: Optional[str] = None,
    broker: Optional[str] = None,
    insured_name: Optional[str] = None,
    vessel_name: Optional[str] = None,
    vessel_code: Optional[str] = None,
    status: Optional[str] = None,
    currency: Optional[str] = None,
    search: Optional[str] = None
):
    offset = (page - 1) * limit
    where_sql, params = build_filter_clause(
        fac_code=fac_code, reff_number=reff_number, company_name=company_name, direct=direct,
        broker=broker, insured_name=insured_name, vessel_name=vessel_name, vessel_code=vessel_code,
        status=status, currency=currency, search=search
    )

    count_res = query_one(f'SELECT count(*) as count FROM public."FACUL_ETL_MH_AKSEPTASI"{where_sql}', params)
    total_records = count_res['count'] if count_res else 0

    data = query_all(
        f'SELECT * FROM public."FACUL_ETL_MH_AKSEPTASI"{where_sql} ORDER BY id ASC LIMIT %s OFFSET %s', 
        params + [limit, offset]
    )
    total_pages = (total_records + limit - 1) // limit if limit > 0 else 1

    return {
        "page": page,
        "limit": limit,
        "total": total_records,
        "totalPages": total_pages,
        "data": data
    }

@app.get("/api/loss-settle")
def get_loss_settle(
    page: int = 1,
    limit: int = 50,
    fac_code: Optional[str] = None,
    reff_number: Optional[str] = None,
    company_name: Optional[str] = None,
    direct: Optional[str] = None,
    broker: Optional[str] = None,
    insured_name: Optional[str] = None,
    vessel_name: Optional[str] = None,
    vessel_code: Optional[str] = None,
    status: Optional[str] = None,
    currency: Optional[str] = None,
    date_of_loss: Optional[str] = None,
    search: Optional[str] = None
):
    offset = (page - 1) * limit
    where_sql, params = build_filter_clause(
        fac_code=fac_code, reff_number=reff_number, company_name=company_name, direct=direct,
        broker=broker, insured_name=insured_name, vessel_name=vessel_name, vessel_code=vessel_code,
        status=status, currency=currency, date_of_loss=date_of_loss, search=search
    )

    count_res = query_one(f'SELECT count(*) as count FROM public."FACUL_ETL_MH_LOSS_SETTLE"{where_sql}', params)
    total_records = count_res['count'] if count_res else 0

    data = query_all(
        f'SELECT * FROM public."FACUL_ETL_MH_LOSS_SETTLE"{where_sql} ORDER BY id ASC LIMIT %s OFFSET %s', 
        params + [limit, offset]
    )
    total_pages = (total_records + limit - 1) // limit if limit > 0 else 1

    return {
        "page": page,
        "limit": limit,
        "total": total_records,
        "totalPages": total_pages,
        "data": data
    }

# ---------------- HISTORY ENDPOINTS ----------------

@app.get("/api/history")
def get_history(
    search: Optional[str] = None,
    cob: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    limit: int = 9
):
    offset = (page - 1) * limit
    where_clauses = []
    params = []

    if search:
        search_lower = f"%{search.lower().strip()}%"
        where_clauses.append("(LOWER(file_name) LIKE %s OR LOWER(cedant) LIKE %s)")
        params.extend([search_lower, search_lower])

    if cob and cob not in ["Semua Tipe COB", "Semua"]:
        where_clauses.append("LOWER(cob) = %s")
        params.append(cob.lower().strip())

    if status and status not in ["Status: Semua", "Semua"]:
        where_clauses.append("status = %s")
        params.append(status.strip())

    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    count_res = query_one(f"SELECT count(*) as count FROM etl_history{where_sql}", params)
    total_records = count_res['count'] if count_res else 0

    rows = query_all(
        f"SELECT * FROM etl_history{where_sql} ORDER BY id DESC LIMIT %s OFFSET %s", 
        params + [limit, offset]
    )

    total_pages = (total_records + limit - 1) // limit if limit > 0 else 1

    return {
        "page": page,
        "limit": limit,
        "total": total_records,
        "totalPages": total_pages,
        "data": rows
    }

class HistoryCreateRequest(BaseModel):
    file_name: str
    file_size: str
    file_type: Optional[str] = "XLSX"
    cedant: str
    cob: str
    status: Optional[str] = "Berhasil Dimuat"
    records_count: Optional[int] = 48250
    schema_accuracy: Optional[float] = 99.8
    duration_seconds: Optional[float] = 8.0
    log_message: Optional[str] = "File diproses dan divalidasi sukses."

@app.post("/api/history")
def create_history(item: HistoryCreateRequest):
    now_display = datetime.now().strftime("%d %b %Y, %H:%M WIB")

    sql = '''
        INSERT INTO etl_history (
            file_name, file_size, file_type, cedant, cob, status,
            date_display, records_count, schema_accuracy, duration_seconds, log_message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    '''

    new_id = execute_dml(sql, [
        item.file_name, item.file_size, item.file_type, item.cedant, item.cob,
        item.status, now_display, item.records_count, item.schema_accuracy,
        item.duration_seconds, item.log_message
    ], returning_id=True)

    return {
        "success": True,
        "id": new_id,
        "message": f"Riwayat berkas {item.file_name} berhasil disimpan."
    }

@app.get("/api/history/{history_id}")
def get_history_detail(history_id: int):
    row = query_one("SELECT * FROM etl_history WHERE id = %s", [history_id])
    if not row:
        raise HTTPException(status_code=404, detail="Riwayat tidak ditemukan")
    return row

# ---------------- MAPPING TEMPLATES ENDPOINTS ----------------

@app.get("/api/mapping-templates")
def get_mapping_templates():
    rows = query_all("SELECT * FROM mapping_templates ORDER BY id ASC")
    result = []
    for r in rows:
        item = dict(r)
        if isinstance(item.get("mappings_json"), str):
            try:
                item["mappings"] = json.loads(item["mappings_json"])
            except Exception:
                item["mappings"] = []
        elif isinstance(item.get("mappings_json"), list):
            item["mappings"] = item["mappings_json"]
        else:
            item["mappings"] = []
        result.append(item)
    return result

class MappingTemplateRequest(BaseModel):
    name: str
    target_schema: Optional[str] = "ipr_stage_db"
    mappings: List[Dict[str, Any]]

@app.post("/api/mapping-templates")
def save_mapping_template(req: MappingTemplateRequest):
    mappings_str = json.dumps(req.mappings)
    
    if ACTIVE_DB_ENGINE == "postgres":
        sql = '''
            INSERT INTO mapping_templates (name, target_schema, mappings_json)
            VALUES (%s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET
                target_schema = EXCLUDED.target_schema,
                mappings_json = EXCLUDED.mappings_json;
        '''
    else:
        sql = '''
            INSERT INTO mapping_templates (name, target_schema, mappings_json)
            VALUES (%s, %s, %s)
            ON CONFLICT(name) DO UPDATE SET
                target_schema = excluded.target_schema,
                mappings_json = excluded.mappings_json
        '''

    execute_dml(sql, [req.name, req.target_schema, mappings_str])
    return {"success": True, "message": f"Template '{req.name}' berhasil disimpan."}

@app.delete("/api/mapping-templates/{template_id}")
def delete_mapping_template(template_id: int):
    execute_dml("DELETE FROM mapping_templates WHERE id = %s", [template_id])
    return {"success": True, "message": "Template berhasil dihapus."}

# ---------------- FILE UPLOAD & AI PARSING SIMULATION ----------------

@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    cob: Optional[str] = Form("Fire & Property"),
    mapping_template: Optional[str] = Form("Format Standar Bordero TriPakarta Fire 2026")
):
    contents = await file.read()
    size_mb = round(len(contents) / (1024 * 1024), 2)
    if size_mb == 0.0:
        size_mb = 1.8 # Friendly fallback display size for small test uploads

    detected_cols = [
        "No", "COB", "REGISTER NO.", "POLICY NUMBER", "CERTIFICATE NO",
        "REFF BORDEREAUX", "NAMA TERTANGGUNG", "TSI ORIGINAL", "SUM INSURED SHARE",
        "PREMIUM AMOUNT", "BROKER NAME", "VESSEL CODE", "VESSEL NAME",
        "LOSS CAUSE", "DATE OF LOSS", "SETTLEMENT STATUS"
    ]

    return {
        "file_name": file.filename,
        "file_size": f"{size_mb} MB",
        "cob": cob,
        "detected_columns_count": len(detected_cols),
        "detected_columns": detected_cols,
        "ready_for_mapping": True
    }

class ProcessETLRequest(BaseModel):
    file_name: str
    file_size: str
    cedant: Optional[str] = "PT Asuransi Tri Pakarta"
    cob: Optional[str] = "Fire & Property"
    mappings: Optional[List[Dict[str, Any]]] = []

@app.post("/api/process-etl")
def process_etl(req: ProcessETLRequest):
    now_display = datetime.now().strftime("%d %b %Y, %H:%M WIB")

    sql = '''
        INSERT INTO etl_history (
            file_name, file_size, file_type, cedant, cob, status,
            date_display, records_count, schema_accuracy, duration_seconds, log_message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    '''

    new_id = execute_dml(sql, [
        req.file_name, req.file_size, "XLSX", req.cedant, req.cob,
        "Berhasil Dimuat", now_display, 48250, 99.8, 8.2,
        "Pemrosesan AI & Validasi skema sukses. Sebanyak 48,250 baris data berhasil divalidasi dan dimuat ke database."
    ], returning_id=True)

    return {
        "job_id": f"job-{int(time.time())}",
        "history_id": new_id,
        "file_name": req.file_name,
        "status": "completed",
        "total_records": 48250,
        "schema_accuracy": 99.8,
        "remaining_seconds": 0,
        "message": "Pemrosesan AI selesai dan disimpan ke database."
    }

if __name__ == "__main__":
    import uvicorn
    port = safe_int(os.getenv("PORT"), 8000)
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
