"""
main.py — Furnace Flame Temperature Mapping API
================================================

Encryption
----------
  Layer 2 ONLY — At-Rest AES-256-CBC + HMAC-SHA256
  Data is encrypted when written to the DB and decrypted when read.
  Transit is plain HTTP — no WebCrypto required on the client.
"""



from fastapi import FastAPI, UploadFile, File, Form, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import pymysql
import json
import uuid
import shutil
import os
from datetime import datetime
import re

from crypto import (
    encrypt_at_rest,
    decrypt_at_rest_float,
)

app = FastAPI()

# ─── CORS ─────────────────────────────────────────────────────
origins = [
    "http://localhost:5173",
    "https://furnaceflametemperaturemapping.vercel.app",
    "http://101.53.132.91:5173",
    "http://91.203.132.34:5173",

]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── MYSQL CONNECTION ──────────────────────────────────────────
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


def clean(val):
    try:
        if pd.isna(val):
            return None
        return float(val)
    except Exception:
        return None


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


def find_row(df, keyword, search_col=0):
    for i, row in df.iterrows():
        if keyword.upper() in str(row[search_col]).upper():
            return i
    return None


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


def enc(v):
    return v


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
            params.get("main_steam_pressure_l"),
            params.get("main_steam_pressure_r"),
            params.get("main_steam_flow_l"),
            params.get("main_steam_flow_r"),
            params.get("superheat_spray_l"),
            params.get("superheat_spray_r"),
            params.get("reheat_spray_l"),
            params.get("reheat_spray_r"),
            params.get("o2_aph_inlet_pcr_l"),
            params.get("o2_aph_inlet_pcr_r"),
            params.get("wind_box_dp_l"),
            params.get("wind_box_dp_r"),
            params.get("total_pa_flow"),
            params.get("fg_temp_after_dpsh_l"),
            params.get("fg_temp_after_dpsh_r"),
            params.get("fg_temp_after_psh_l"),
            params.get("fg_temp_after_psh_r"),
            params.get("fg_temp_after_rh_l"),
            params.get("fg_temp_after_rh_r"),
            params.get("fg_temp_after_hsh_l"),
            params.get("fg_temp_after_hsh_r"),
            params.get("fg_temp_after_eco_l"),
            params.get("fg_temp_after_eco_r"),
            params.get("fg_temp_after_aph_l"),
            params.get("fg_temp_after_aph_r"),
        ),
    )


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
                m.get("mill"),
                m.get("coal_flow_tph"),
                m.get("pa_flow_tph"),
                m.get("mill_dp_mmwc"),
                m.get("mill_outlet_temp"),
                m.get("mill_current_amp"),
            ),
        )
# ── Decrypt helpers ────────────────────────────────────────────
_BOILER_FIELDS = [
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
_COAL_FIELDS = ["coal_flow_tph", "pa_flow_tph", "mill_dp_mmwc", "mill_outlet_temp", "mill_current_amp"]


def _dec_boiler(row):
    if not row:
        return row
    out = dict(row)
    for f in _BOILER_FIELDS:
        if f in out:
            out[f] = decrypt_at_rest_float(out[f])
    return out


def _dec_coal(row):
    out = dict(row)
    for f in _COAL_FIELDS:
        if f in out:
            out[f] = decrypt_at_rest_float(out[f])
    return out


def _dec_profile(row):
    out = dict(row)
    for f in ["elevation", "c1", "c2", "c3", "c4", "avg_val"]:
        if f in out:
            out[f] = decrypt_at_rest_float(out[f])
    return out


# ─── UPLOAD ───────────────────────────────────────────────────
@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    station_id: int = 1,
    unit_id: int = Form(1),
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
            run_date = extract_date_from_sheet(df)
            
            if not run_date:
                continue

            elevation, c1, c2, c3, c4, avg = [], [], [], [], [], []
            
            start = None
            for i, row in df.iterrows():
                if "ELEVATION" in str(row[0]).upper():
                    start = i + 1
                    break
            if start is None:
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

            if not elevation:
                continue

            boiler_params = extract_boiler_mill_params(df)
            coal_mills    = extract_coal_mill_params(df)
            location = df.iloc[1, 0]
            unit = df.iloc[1,1]


            cur.callproc("sp_create_run", (
                station_id, unit_id, file.filename,
                datetime.now(), run_date, uploaded_by, notes,location, unit
            ))
            run_id = cur.fetchall()[0]["run_id"]

            points = [
    {
        "elevation": elevation[i],
        "c1": c1[i],
        "c2": c2[i],
        "c3": c3[i],
        "c4": c4[i],
        "avg": avg[i],
    }
    for i in range(len(elevation))
]
            cur.callproc("sp_add_run_points_bulk", (run_id, location, unit, json.dumps(points)))

            if boiler_params:
                upsert_boiler_mill_params(cur, run_id, boiler_params)
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
        return {"message": "Upload processed successfully", "total_sheets_processed": len(results), "runs": results}

    except Exception as e:
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()}


# ─── HISTORY ──────────────────────────────────────────────────
@app.get("/history")
def get_history(station_id: str, unit_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_runs", (station_id, unit_id, "2000-01-01", "2100-01-01"))
    data = cur.fetchall()
    conn.close()
    return data


@app.get("/history/{run_id}")
def get_run(run_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_run_profile", (run_id,))
    rows = cur.fetchall()
    conn.close()

    return {
        "elevation": [r["elevation"] for r in rows],
        "corner1":   [r["c1"]        for r in rows],
        "corner2":   [r["c2"]        for r in rows],
        "corner3":   [r["c3"]        for r in rows],
        "corner4":   [r["c4"]        for r in rows],
        "average":   [r["avg_val"]   for r in rows],
    }


@app.get("/history/{run_id}/boiler-params")
def get_boiler_params(run_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM boiler_mill_params WHERE run_id = %s", (run_id,))
    data = cur.fetchone()
    conn.close()
    return data or {}


@app.get("/history/{run_id}/coal-mill-params")
def get_coal_mill_params(run_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM coal_mill_params WHERE run_id = %s ORDER BY mill", (run_id,))
    data = cur.fetchall()
    conn.close()
    return data


# ─── MISC ─────────────────────────────────────────────────────
@app.get("/compare")
def compare_runs(ids: str):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_comparison_data", (ids,))
    data = cur.fetchall()
    conn.close()
    return data


@app.get("/stations")
def get_stations():
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_stations")
    data = cur.fetchall()
    conn.close()
    return data


@app.get("/units/{station_id}")
def get_units(station_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_units_by_station", (station_id,))
    data = cur.fetchall()
    conn.close()
    return data


@app.get("/mapping-dates")
def get_mapping_dates(station_id: int = Query(...), unit_id: int = Query(...)):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_mapping_dates", (station_id, unit_id))
    rows = cur.fetchall()
    conn.close()
    return rows

@app.delete("/runs/{run_id}")
def delete_run(run_id: int):
    try:
        conn = get_db()
        cur = conn.cursor()

        # Delete dependent data first (IMPORTANT for FK safety)
        cur.execute("DELETE FROM profile_points WHERE run_id = %s", (run_id,))
        cur.execute("DELETE FROM boiler_mill_params WHERE run_id = %s", (run_id,))
        cur.execute("DELETE FROM coal_mill_params WHERE run_id = %s", (run_id,))
        
        # Finally delete run
        cur.execute("DELETE FROM runs WHERE run_id = %s", (run_id,))

        conn.commit()
        conn.close()

        return {"message": f"Run {run_id} deleted successfully"}

    except Exception as e:
        return {"error": str(e)}
    
@app.get("/get-upload-log")
def get_upload_log():
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_event_log")
    rows = cur.fetchall()
    conn.close()
    return rows


class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/login")
def login(data: LoginRequest):
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.callproc("sp_login", (data.username, data.password))

        rows = []
        for result in cur.stored_results():
            rows = result.fetchall()

        conn.close()

        if len(rows) > 0:
            return {
                "message": "Login successful",
                "username": data.username
            }
        else:
            return {
                "message": "Login failed",
                "username": data.username
            }

    except Exception as e:
        return {"error": str(e)}