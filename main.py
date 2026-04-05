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

app = FastAPI()

# ─── CORS ─────────────────────────────────────────────────────
origins = [
    "http://localhost:5173",
    "https://furnaceflametemperaturemapping.vercel.app",
    "http://101.53.132.91:5173"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── MYSQL CONNECTION ─────────────────────────────────────────
def get_db():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="Vishalgad5@3332",
        database="furnace_db",
        cursorclass=pymysql.cursors.DictCursor
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
    except:
        return None


# ─────────────────────────────────────────────────────────────
# Helper: Extract date from D2 → 'Date :- DD/MM/YYYY'
# ─────────────────────────────────────────────────────────────
def extract_date_from_sheet(df):
    try:
        cell_value = str(df.iloc[1, 3])
        match = re.search(r'(\d{2})/(\d{2})/(\d{4})', cell_value)
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
#
# EXACT LAYOUT (verified from screenshot, 0-based col indices):
#
#  Row+0  │ "Boiler & Mill Parameters"  header
#  Row+1  │ A=label   C(2)=L   D(3)=R  │ E=label  G(6)=L   H(7)=R   ← Main Steam Pressure / FG DPSH
#  Row+2  │ A=label   C(2)=single val  │ E=label  G(6)=L   H(7)=R   ← Main Steam Flow     / FG PSH
#  Row+3  │ A=label   C(2)=L   D(3)=R  │ E=label  G(6)=L   H(7)=R   ← Superheat Spray     / FG RH
#  Row+4  │ A=label   C(2)=L   D(3)=R  │ E=label  G(6)=L   H(7)=R   ← Re-heat Spray       / FG HSH
#  Row+5  │ A=label   C(2)=L   D(3)=R  │ E=label  G(6)=L   H(7)=R   ← O2 at APH PCR       / FG Eco
#  Row+6  │ A=label   C(2)=L   D(3)=R  │ (no right-side label)       ← Wind Box DP
#  Row+7  │ A=label   C(2)=single val  │ E=label  G(6)=L   H(7)=R   ← Total PA Flow        / FG APH
# ─────────────────────────────────────────────────────────────
def extract_boiler_mill_params(df):
    header_row = find_row(df, "BOILER & MILL PARAMETERS")
    if header_row is None:
        return None

    def get(r_offset, col):
        try:
            return clean(df.iloc[header_row + r_offset, col])
        except:
            return None

    return {
        # ── Left side: L col=2, R col=3 ───────────────────────────────────
        "main_steam_pressure_l": get(1, 2),   # single val, no R
        "main_steam_pressure_r": None,
        "main_steam_flow_l":     get(2, 2),   # single merged value
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

        # ── Right side: FG Temps  L col=6, R col=7 ────────────────────────
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
        "fg_temp_after_aph_l":   get(7, 6),   # row+7 same as Total PA flow row
        "fg_temp_after_aph_r":   get(7, 7),
    }


# ─────────────────────────────────────────────────────────────
# Helper: Extract Coal Mill Parameters
#
# EXACT LAYOUT (verified from screenshot, 0-based col indices):
#
#  Row+0  │ "Coal Mill Parameters" header
#  Row+1  │ "COAL MILLS" | Mill-A(1) | Mill-B(2) | Mill-C(3) | Mill-D(4)
#          │              | Mill-E(5) | Mill-F(6) | Mill-G(7) | Mill-H(8)
#  Row+2  │ Coal Flow (TPH)    → cols 1–8
#  Row+3  │ PA Flow (TPH)      → cols 1–8
#  Row+4  │ Mill DP (mmwc)     → cols 1–8
#  Row+5  │ Mill Outlet Temp   → cols 1–8
#  Row+6  │ Mill Current (Amp) → cols 1–8
# ─────────────────────────────────────────────────────────────
def extract_coal_mill_params(df):
    # "Coal Mill Parameters" is the section header (row+0)
    # "COAL MILLS" label with mill names is row+1
    header_row = find_row(df, "COAL MILL PARAMETERS")
    if header_row is None:
        return None

    mills = ["A", "B", "C", "D", "E", "F", "G", "H"]

    def get(r_offset, col_idx):
        try:
            return clean(df.iloc[header_row + r_offset, col_idx])
        except:
            return None

    result = []
    for i, mill in enumerate(mills):
        col = 1 + i   # Mill A = col 1, Mill B = col 2 … Mill H = col 8
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
# DB: upsert boiler_mill_params
# ─────────────────────────────────────────────────────────────
def upsert_boiler_mill_params(cur, run_id, params):
    cur.execute("DELETE FROM boiler_mill_params WHERE run_id = %s", (run_id,))
    cur.execute("""
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
    """, (
        run_id,
        params.get("main_steam_pressure_l"), params.get("main_steam_pressure_r"),
        params.get("main_steam_flow_l"),     params.get("main_steam_flow_r"),
        params.get("superheat_spray_l"),     params.get("superheat_spray_r"),
        params.get("reheat_spray_l"),        params.get("reheat_spray_r"),
        params.get("o2_aph_inlet_pcr_l"),    params.get("o2_aph_inlet_pcr_r"),
        params.get("wind_box_dp_l"),         params.get("wind_box_dp_r"),
        params.get("total_pa_flow"),
        params.get("fg_temp_after_dpsh_l"),  params.get("fg_temp_after_dpsh_r"),
        params.get("fg_temp_after_psh_l"),   params.get("fg_temp_after_psh_r"),
        params.get("fg_temp_after_rh_l"),    params.get("fg_temp_after_rh_r"),
        params.get("fg_temp_after_hsh_l"),   params.get("fg_temp_after_hsh_r"),
        params.get("fg_temp_after_eco_l"),   params.get("fg_temp_after_eco_r"),
        params.get("fg_temp_after_aph_l"),   params.get("fg_temp_after_aph_r"),
    ))


# ─────────────────────────────────────────────────────────────
# DB: upsert coal_mill_params
# ─────────────────────────────────────────────────────────────
def upsert_coal_mill_params(cur, run_id, mills):
    cur.execute("DELETE FROM coal_mill_params WHERE run_id = %s", (run_id,))
    for m in mills:
        cur.execute("""
            INSERT INTO coal_mill_params (
                run_id, mill,
                coal_flow_tph, pa_flow_tph, mill_dp_mmwc,
                mill_outlet_temp, mill_current_amp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            run_id,
            m["mill"],
            m.get("coal_flow_tph"),
            m.get("pa_flow_tph"),
            m.get("mill_dp_mmwc"),
            m.get("mill_outlet_temp"),
            m.get("mill_current_amp"),
        ))


# ─────────────────────────────────────────────────────────────
# UPLOAD API
# ─────────────────────────────────────────────────────────────
@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    station_id: int = 1,
    unit_id: int = 1,
    uploaded_by: str = Form("system"),
    notes: str = Form("")
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

            # Find the row index where the boiler section starts so we
            # know exactly where the elevation table ends.
            boiler_start = find_row(df, "BOILER & MILL PARAMETERS")
            elev_end = boiler_start if boiler_start is not None else len(df)

            for i in range(start, elev_end):
                row = df.iloc[i]
                elev_val = clean(row[0])
                # Skip rows with no elevation value (merged/blank cells,
                # note rows, etc.)
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
            cur.callproc("sp_create_run", (
                station_id, unit_id, file.filename,
                datetime.now(), run_date, uploaded_by, notes
            ))
            run_id = cur.fetchall()[0]["run_id"]

            # ── Elevation Points ──────────────────────────────
            points = [
                {"elevation": elevation[i], "c1": c1[i], "c2": c2[i],
                 "c3": c3[i], "c4": c4[i], "avg": avg[i]}
                for i in range(len(elevation))
            ]
            cur.callproc("sp_add_run_points_bulk", (run_id, json.dumps(points)))

            # ── Boiler params ─────────────────────────────────
            if boiler_params:
                upsert_boiler_mill_params(cur, run_id, boiler_params)

            # ── Coal Mill params ──────────────────────────────
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

        return {
            "message": "Upload processed successfully",
            "total_sheets_processed": len(results),
            "runs": results
        }

    except Exception as e:
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()}


# ─── GET RUNS (HISTORY) ───────────────────────────────────────
@app.get("/history")
def get_history(station_id: int, unit_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_runs", (station_id, unit_id, "2000-01-01", "2100-01-01"))
    data = cur.fetchall()
    conn.close()
    return data


# ─── GET SINGLE RUN (elevation profile) ──────────────────────
@app.get("/history/{run_id}")
def get_run(run_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_run_profile", (run_id,))
    rows = cur.fetchall()
    conn.close()
    return {
        "elevation": [r["elevation"] for r in rows],
        "corner1":   [r["c1"] for r in rows],
        "corner2":   [r["c2"] for r in rows],
        "corner3":   [r["c3"] for r in rows],
        "corner4":   [r["c4"] for r in rows],
        "average":   [r["avg_val"] for r in rows],
    }


# ─── GET BOILER & MILL PARAMS FOR A RUN ──────────────────────
@app.get("/history/{run_id}/boiler-params")
def get_boiler_params(run_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM boiler_mill_params WHERE run_id = %s", (run_id,))
    data = cur.fetchone()
    conn.close()
    return data or {}


# ─── GET COAL MILL PARAMS FOR A RUN ──────────────────────────
@app.get("/history/{run_id}/coal-mill-params")
def get_coal_mill_params(run_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM coal_mill_params WHERE run_id = %s ORDER BY mill",
        (run_id,)
    )
    data = cur.fetchall()
    conn.close()
    return data


# ─── COMPARE RUNS ────────────────────────────────────────────
@app.get("/compare")
def compare_runs(ids: str):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_comparison_data", (ids,))
    data = cur.fetchall()
    conn.close()
    return data


# ─── STATIONS ────────────────────────────────────────────────
@app.get("/stations")
def get_stations():
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_stations")
    data = cur.fetchall()
    conn.close()
    return data


# ─── UNITS ───────────────────────────────────────────────────
@app.get("/units/{station_id}")
def get_units(station_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_units_by_station", (station_id,))
    data = cur.fetchall()
    conn.close()
    return data


# ─── MAPPING DATES ───────────────────────────────────────────
@app.get("/mapping-dates")
def get_mapping_dates(
    station_id: int = Query(...),
    unit_id:    int = Query(...)
):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_mapping_dates", (station_id, unit_id))
    rows = cur.fetchall()
    conn.close()
    return rows