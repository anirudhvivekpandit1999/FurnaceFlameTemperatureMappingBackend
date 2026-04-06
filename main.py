"""
main.py — Furnace Flame Temperature Mapping API
================================================

Encryption layers
-----------------
  Layer 1  Transit-IN   AES-256-GCM     → decrypt every incoming encrypted body
  Layer 2  At-Rest      AES-256-CBC     → encrypt/decrypt individual DB columns
  Layer 3  Transit-OUT  AES-256-GCM     → encrypt every outgoing JSON response

See crypto.py for full algorithm documentation and key-loading rules.
"""

from fastapi import FastAPI, UploadFile, File, Form, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import pymysql
import json
import uuid
import shutil
import os
from datetime import datetime
import re

# ── Encryption helpers ─────────────────────────────────────
from crypto import (
    decrypt_transit_in_json,
    encrypt_at_rest,
    decrypt_at_rest,
    decrypt_at_rest_float,
    encrypt_transit_out,
    TransitDecryptionError,
)

app = FastAPI()

# ─── CORS ─────────────────────────────────────────────────────
origins = [
    "http://localhost:5173",
    "https://furnaceflametemperaturemapping.vercel.app",
    "http://101.53.132.91:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# MIDDLEWARE  (Layer 1 — Transit-IN Decryption)
# ─────────────────────────────────────────────────────────────
# When a client sends a JSON body with the shape:
#   { "iv": "...", "tag": "...", "ct": "..." }
# the middleware decrypts it and replaces the body so the endpoint
# sees normal plain data.
#
# Requests whose body is NOT encrypted (e.g. file uploads with
# multipart/form-data) are passed through unchanged.
# ─────────────────────────────────────────────────────────────

@app.middleware("http")
async def transit_in_decryption_middleware(request: Request, call_next):
    content_type = request.headers.get("content-type", "")

    # Only attempt JSON-body decryption for application/json requests
    if "application/json" in content_type:
        try:
            raw_body = await request.body()
            if raw_body:
                payload = json.loads(raw_body)
                # Detect encrypted envelope
                if isinstance(payload, dict) and {"iv", "tag", "ct"}.issubset(payload):
                    try:
                        decrypted = decrypt_transit_in_json(payload)
                    except TransitDecryptionError as exc:
                        return JSONResponse(
                            status_code=400,
                            content={"error": f"Transit decryption failed: {exc}"},
                        )
                    # Rebuild the request body with the decrypted content
                    decrypted_bytes = json.dumps(decrypted).encode()

                    async def new_body() -> bytes:
                        return decrypted_bytes

                    request._body = decrypted_bytes   # FastAPI stashes the body here
        except Exception:
            pass  # Non-JSON or empty body — pass through

    response = await call_next(request)
    return response


# ─────────────────────────────────────────────────────────────
# LAYER 3 helper — wrap any return value in an encrypted envelope
# ─────────────────────────────────────────────────────────────

def encrypted_response(data) -> JSONResponse:
    """Encrypt data with AES-256-GCM and return as a JSONResponse."""
    return JSONResponse(content=encrypt_transit_out(data))


# ─── MYSQL CONNECTION ─────────────────────────────────────────
def get_db():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="Vishalgad5@3332",
        database="furnace_db",
        cursorclass=pymysql.cursors.DictCursor,
    )


UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ─────────────────────────────────────────────────────────────
# Helper: Clean numeric value
# ─────────────────────────────────────────────────────────────
def clean(val):
    try:
        if pd.isna(val):
            return None
        return float(val)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# Helper: Extract date from D2 → 'Date :- DD/MM/YYYY'
# ─────────────────────────────────────────────────────────────
def extract_date_from_sheet(df):
    try:
        cell_value = str(df.iloc[1, 3])
        match = re.search(r"(\d{2})/(\d{2})/(\d{4})", cell_value)
        if match:
            d, m, y = match.groups()
            return datetime(int(y), int(m), int(d)).date()
    except (IndexError, ValueError):
        pass
    return None


# ─────────────────────────────────────────────────────────────
# Helper: Find row index where col[search_col] contains keyword
# ─────────────────────────────────────────────────────────────
def find_row(df, keyword, search_col=0):
    for i, row in df.iterrows():
        if keyword.upper() in str(row[search_col]).upper():
            return i
    return None


# ─────────────────────────────────────────────────────────────
# Helper: Extract Boiler & Mill Parameters
# ─────────────────────────────────────────────────────────────
def extract_boiler_mill_params(df):
    header_row = find_row(df, "BOILER & MILL PARAMETERS")
    if header_row is None:
        return None

    def get(r_offset, col):
        try:
            return clean(df.iloc[header_row + r_offset, col])
        except Exception:
            return None

    return {
        "main_steam_pressure_l": get(1, 2),
        "main_steam_pressure_r": None,
        "main_steam_flow_l":     get(2, 2),
        "main_steam_flow_r":     None,
        "superheat_spray_l":     get(3, 2),
        "superheat_spray_r":     get(3, 3),
        "reheat_spray_l":        get(4, 2),
        "reheat_spray_r":        get(4, 3),
        "o2_aph_inlet_pcr_l":    get(5, 2),
        "o2_aph_inlet_pcr_r":    get(5, 3),
        "wind_box_dp_l":         get(6, 2),
        "wind_box_dp_r":         get(6, 3),
        "total_pa_flow":         get(7, 2),
        "fg_temp_after_dpsh_l":  get(1, 6),
        "fg_temp_after_dpsh_r":  get(1, 7),
        "fg_temp_after_psh_l":   get(2, 6),
        "fg_temp_after_psh_r":   get(2, 7),
        "fg_temp_after_rh_l":    get(3, 6),
        "fg_temp_after_rh_r":    get(3, 7),
        "fg_temp_after_hsh_l":   get(4, 6),
        "fg_temp_after_hsh_r":   get(4, 7),
        "fg_temp_after_eco_l":   get(5, 6),
        "fg_temp_after_eco_r":   get(5, 7),
        "fg_temp_after_aph_l":   get(7, 6),
        "fg_temp_after_aph_r":   get(7, 7),
    }


# ─────────────────────────────────────────────────────────────
# Helper: Extract Coal Mill Parameters
# ─────────────────────────────────────────────────────────────
def extract_coal_mill_params(df):
    header_row = find_row(df, "COAL MILL PARAMETERS")
    if header_row is None:
        return None

    mills = ["A", "B", "C", "D", "E", "F", "G", "H"]

    def get(r_offset, col_idx):
        try:
            return clean(df.iloc[header_row + r_offset, col_idx])
        except Exception:
            return None

    result = []
    for i, mill in enumerate(mills):
        col = 1 + i
        result.append({
            "mill":             mill,
            "coal_flow_tph":    get(2, col),
            "pa_flow_tph":      get(3, col),
            "mill_dp_mmwc":     get(4, col),
            "mill_outlet_temp": get(5, col),
            "mill_current_amp": get(6, col),
        })

    return result


# ─────────────────────────────────────────────────────────────
# LAYER 2 helpers — encrypt params dicts before DB insertion
# ─────────────────────────────────────────────────────────────

def enc(v):
    """Shorthand: encrypt a single value for at-rest storage."""
    return encrypt_at_rest(v)


# ─────────────────────────────────────────────────────────────
# DB: upsert boiler_mill_params  (values encrypted at rest)
# ─────────────────────────────────────────────────────────────
def upsert_boiler_mill_params(cur, run_id, params):
    cur.execute("DELETE FROM boiler_mill_params WHERE run_id = %s", (run_id,))
    cur.execute(
        """
        INSERT INTO boiler_mill_params (
            run_id,
            main_steam_pressure_l, main_steam_pressure_r,
            main_steam_flow_l,     main_steam_flow_r,
            superheat_spray_l,     superheat_spray_r,
            reheat_spray_l,        reheat_spray_r,
            o2_aph_inlet_pcr_l,    o2_aph_inlet_pcr_r,
            wind_box_dp_l,         wind_box_dp_r,
            total_pa_flow,
            fg_temp_after_dpsh_l,  fg_temp_after_dpsh_r,
            fg_temp_after_psh_l,   fg_temp_after_psh_r,
            fg_temp_after_rh_l,    fg_temp_after_rh_r,
            fg_temp_after_hsh_l,   fg_temp_after_hsh_r,
            fg_temp_after_eco_l,   fg_temp_after_eco_r,
            fg_temp_after_aph_l,   fg_temp_after_aph_r
        ) VALUES (
            %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        """,
        (
            run_id,
            # ── Layer 2: encrypt each value individually ──────────────
            enc(params.get("main_steam_pressure_l")),
            enc(params.get("main_steam_pressure_r")),
            enc(params.get("main_steam_flow_l")),
            enc(params.get("main_steam_flow_r")),
            enc(params.get("superheat_spray_l")),
            enc(params.get("superheat_spray_r")),
            enc(params.get("reheat_spray_l")),
            enc(params.get("reheat_spray_r")),
            enc(params.get("o2_aph_inlet_pcr_l")),
            enc(params.get("o2_aph_inlet_pcr_r")),
            enc(params.get("wind_box_dp_l")),
            enc(params.get("wind_box_dp_r")),
            enc(params.get("total_pa_flow")),
            enc(params.get("fg_temp_after_dpsh_l")),
            enc(params.get("fg_temp_after_dpsh_r")),
            enc(params.get("fg_temp_after_psh_l")),
            enc(params.get("fg_temp_after_psh_r")),
            enc(params.get("fg_temp_after_rh_l")),
            enc(params.get("fg_temp_after_rh_r")),
            enc(params.get("fg_temp_after_hsh_l")),
            enc(params.get("fg_temp_after_hsh_r")),
            enc(params.get("fg_temp_after_eco_l")),
            enc(params.get("fg_temp_after_eco_r")),
            enc(params.get("fg_temp_after_aph_l")),
            enc(params.get("fg_temp_after_aph_r")),
        ),
    )


# ─────────────────────────────────────────────────────────────
# DB: upsert coal_mill_params  (values encrypted at rest)
# ─────────────────────────────────────────────────────────────
def upsert_coal_mill_params(cur, run_id, mills):
    cur.execute("DELETE FROM coal_mill_params WHERE run_id = %s", (run_id,))
    for m in mills:
        cur.execute(
            """
            INSERT INTO coal_mill_params (
                run_id, mill,
                coal_flow_tph, pa_flow_tph, mill_dp_mmwc,
                mill_outlet_temp, mill_current_amp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                m["mill"],   # mill letter (A-H) — not sensitive, not encrypted
                enc(m.get("coal_flow_tph")),
                enc(m.get("pa_flow_tph")),
                enc(m.get("mill_dp_mmwc")),
                enc(m.get("mill_outlet_temp")),
                enc(m.get("mill_current_amp")),
            ),
        )


# ─────────────────────────────────────────────────────────────
# DB: decrypt boiler_mill_params row from DB
# ─────────────────────────────────────────────────────────────
_BOILER_NUMERIC_FIELDS = [
    "main_steam_pressure_l", "main_steam_pressure_r",
    "main_steam_flow_l",     "main_steam_flow_r",
    "superheat_spray_l",     "superheat_spray_r",
    "reheat_spray_l",        "reheat_spray_r",
    "o2_aph_inlet_pcr_l",   "o2_aph_inlet_pcr_r",
    "wind_box_dp_l",         "wind_box_dp_r",
    "total_pa_flow",
    "fg_temp_after_dpsh_l",  "fg_temp_after_dpsh_r",
    "fg_temp_after_psh_l",   "fg_temp_after_psh_r",
    "fg_temp_after_rh_l",    "fg_temp_after_rh_r",
    "fg_temp_after_hsh_l",   "fg_temp_after_hsh_r",
    "fg_temp_after_eco_l",   "fg_temp_after_eco_r",
    "fg_temp_after_aph_l",   "fg_temp_after_aph_r",
]

_COAL_NUMERIC_FIELDS = [
    "coal_flow_tph", "pa_flow_tph", "mill_dp_mmwc",
    "mill_outlet_temp", "mill_current_amp",
]


def _decrypt_boiler_row(row: dict) -> dict:
    if not row:
        return row
    out = dict(row)
    for field in _BOILER_NUMERIC_FIELDS:
        if field in out:
            out[field] = decrypt_at_rest_float(out[field])
    return out


def _decrypt_coal_row(row: dict) -> dict:
    out = dict(row)
    for field in _COAL_NUMERIC_FIELDS:
        if field in out:
            out[field] = decrypt_at_rest_float(out[field])
    return out


def _decrypt_profile_row(row: dict) -> dict:
    """Decrypt elevation profile point (profile_points table)."""
    out = dict(row)
    for field in ["elevation", "c1", "c2", "c3", "c4", "avg_val"]:
        if field in out:
            out[field] = decrypt_at_rest_float(out[field])
    return out


# ─────────────────────────────────────────────────────────────
# UPLOAD API
# ─────────────────────────────────────────────────────────────
# NOTE: multipart/form-data (file upload) bypasses transit-IN decryption
# intentionally — the file bytes are raw Excel, not an encrypted JSON blob.
# The form fields (station_id, unit_id, etc.) also arrive in plain form.
# ─────────────────────────────────────────────────────────────
@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    station_id: int = 1,
    unit_id: int = 1,
    uploaded_by: str = Form("system"),
    notes: str = Form(""),
):
    try:
        file_id = str(uuid.uuid4())
        file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")
        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        df_dict = pd.read_excel(file_path, sheet_name=None, header=None)

        conn = get_db()
        cur = conn.cursor()
        results = []

        for sheet_name, df in df_dict.items():

            # ── Date ──────────────────────────────────────────
            run_date = extract_date_from_sheet(df)
            if not run_date:
                print(f"Skipping sheet {sheet_name} (no date found)")
                continue

            # ── Elevation Table ───────────────────────────────
            elevation, c1, c2, c3, c4, avg = [], [], [], [], [], []
            start = None
            for i, row in df.iterrows():
                if "ELEVATION" in str(row[0]).upper():
                    start = i + 1
                    break

            if start is None:
                print(f"Skipping sheet {sheet_name} (no elevation table)")
                continue

            boiler_start = find_row(df, "BOILER & MILL PARAMETERS")
            elev_end = boiler_start if boiler_start is not None else len(df)

            for i in range(start, elev_end):
                row = df.iloc[i]
                elev_val = clean(row[0])
                if elev_val is None:
                    continue
                elevation.append(elev_val)
                c1.append(clean(row[1]))
                c2.append(clean(row[2]))
                c3.append(clean(row[3]))
                c4.append(clean(row[4]))
                avg.append(clean(row[5]))

            if len(elevation) == 0:
                continue

            # ── Boiler & Mill Params ──────────────────────────
            boiler_params = extract_boiler_mill_params(df)

            # ── Coal Mill Params ──────────────────────────────
            coal_mills = extract_coal_mill_params(df)

            # ── Upsert Run ────────────────────────────────────
            cur.callproc(
                "sp_create_run",
                (station_id, unit_id, file.filename,
                 datetime.now(), run_date, uploaded_by, notes),
            )
            run_id = cur.fetchall()[0]["run_id"]

            # ── Elevation Points — encrypt each value at rest ─
            points = []
            for i in range(len(elevation)):
                points.append({
                    "elevation": enc(elevation[i]),
                    "c1":        enc(c1[i]),
                    "c2":        enc(c2[i]),
                    "c3":        enc(c3[i]),
                    "c4":        enc(c4[i]),
                    "avg":       enc(avg[i]),
                })
            cur.callproc("sp_add_run_points_bulk", (run_id, json.dumps(points)))

            # ── Boiler params — encrypt at rest ───────────────
            if boiler_params:
                upsert_boiler_mill_params(cur, run_id, boiler_params)

            # ── Coal Mill params — encrypt at rest ────────────
            if coal_mills:
                upsert_coal_mill_params(cur, run_id, coal_mills)

            results.append({
                "sheet":          sheet_name,
                "run_id":         run_id,
                "date":           str(run_date),
                "rows":           len(points),
                "boiler_params":  boiler_params is not None,
                "coal_mill_rows": len(coal_mills) if coal_mills else 0,
            })

        conn.commit()
        conn.close()

        # ── Layer 3: encrypt the response ─────────────────────
        return encrypted_response({
            "message": "Upload processed successfully",
            "total_sheets_processed": len(results),
            "runs": results,
        })

    except Exception as e:
        import traceback
        return encrypted_response({"error": str(e), "trace": traceback.format_exc()})


# ─── GET RUNS (HISTORY) ───────────────────────────────────────
@app.get("/history")
def get_history(station_id: int, unit_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_runs", (station_id, unit_id, "2000-01-01", "2100-01-01"))
    data = cur.fetchall()
    conn.close()
    # runs table columns (filename, uploaded_by, notes) could also be
    # encrypted at rest if desired; for now we return them as-is.
    return encrypted_response(data)


# ─── GET SINGLE RUN (elevation profile) ──────────────────────
@app.get("/history/{run_id}")
def get_run(run_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_run_profile", (run_id,))
    rows = cur.fetchall()
    conn.close()

    # ── Layer 2: decrypt each row coming out of the DB ────────
    decrypted = [_decrypt_profile_row(r) for r in rows]

    return encrypted_response({
        "elevation": [r["elevation"] for r in decrypted],
        "corner1":   [r["c1"]        for r in decrypted],
        "corner2":   [r["c2"]        for r in decrypted],
        "corner3":   [r["c3"]        for r in decrypted],
        "corner4":   [r["c4"]        for r in decrypted],
        "average":   [r["avg_val"]   for r in decrypted],
    })


# ─── GET BOILER & MILL PARAMS FOR A RUN ──────────────────────
@app.get("/history/{run_id}/boiler-params")
def get_boiler_params(run_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM boiler_mill_params WHERE run_id = %s", (run_id,))
    data = cur.fetchone()
    conn.close()

    # ── Layer 2: decrypt before sending ──────────────────────
    decrypted = _decrypt_boiler_row(data or {})
    return encrypted_response(decrypted)


# ─── GET COAL MILL PARAMS FOR A RUN ──────────────────────────
@app.get("/history/{run_id}/coal-mill-params")
def get_coal_mill_params(run_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM coal_mill_params WHERE run_id = %s ORDER BY mill",
        (run_id,),
    )
    data = cur.fetchall()
    conn.close()

    # ── Layer 2: decrypt each mill row ────────────────────────
    decrypted = [_decrypt_coal_row(r) for r in data]
    return encrypted_response(decrypted)


# ─── COMPARE RUNS ────────────────────────────────────────────
@app.get("/compare")
def compare_runs(ids: str):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_comparison_data", (ids,))
    data = cur.fetchall()
    conn.close()
    return encrypted_response(data)


# ─── STATIONS ────────────────────────────────────────────────
@app.get("/stations")
def get_stations():
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_stations")
    data = cur.fetchall()
    conn.close()
    return encrypted_response(data)


# ─── UNITS ───────────────────────────────────────────────────
@app.get("/units/{station_id}")
def get_units(station_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_units_by_station", (station_id,))
    data = cur.fetchall()
    conn.close()
    return encrypted_response(data)


# ─── MAPPING DATES ───────────────────────────────────────────
@app.get("/mapping-dates")
def get_mapping_dates(
    station_id: int = Query(...),
    unit_id:    int = Query(...),
):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_mapping_dates", (station_id, unit_id))
    rows = cur.fetchall()
    conn.close()
    return encrypted_response(rows)