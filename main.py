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
# Helper: Clean string value
# ─────────────────────────────────────────────────────────────
def clean_str(val):
    try:
        if pd.isna(val):
            return None
        s = str(val).strip()
        return s if s else None
    except:
        return None


# ─────────────────────────────────────────────────────────────
# Helper: Extract date from cell D2 (row index 1, col index 3)
# Expected format: 'Date :- DD/MM/YYYY'
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
# Helper: Find a row index that contains a keyword in col 0
# ─────────────────────────────────────────────────────────────
def find_row(df, keyword, col=0):
    for i, row in df.iterrows():
        if keyword.upper() in str(row[col]).upper():
            return i
    return None


# ─────────────────────────────────────────────────────────────
# Helper: Extract Boiler & Mill Parameters
#
# Layout (0-based col indices):
#   Col A (0): label
#   Col B (1): L value
#   Col C (2): R value  (some rows)
#   Col E (4): label (right-side params)
#   Col F (5): value
#
# Rows (relative to "Boiler & Mill Parameters" header row):
#   +1: Main Steam Pressure  (col B=L, col C=R)  | FG Temp after DPSH (col F)
#   +2: Main Steam Flow      (col B=L, col C=R)  | FG Temp after PSH  (col F)
#   +3: Superheat Spray      (col B=L, col C=R)  | FG Temp after RH   (col F)
#   +4: Re-heat Spray        (col B=L, col C=R)  | FG Temp after HSH  (col F)
#   +5: O2 at APH inlet PCR  (col B=L, col C=R)  | FG Temp after Eco  (col F)
#   +6: Wind Box DP          (col B=L, col C=R)  | FG Temp after APH  (col F)
#   +7: Total PA flow        (col B=single)
#   ...then merged single-value rows for FG Temp at inlet (col B or F, verify)
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

    params = {
        # Left column params (L / R split)
        "main_steam_pressure_l":   get(1, 1),
        "main_steam_pressure_r":   get(1, 2),
        "main_steam_flow_l":       get(2, 1),
        "main_steam_flow_r":       get(2, 2),
        "superheat_spray_l":       get(3, 1),
        "superheat_spray_r":       get(3, 2),
        "reheat_spray_l":          get(4, 1),
        "reheat_spray_r":          get(4, 2),
        "o2_aph_inlet_pcr_l":      get(5, 1),
        "o2_aph_inlet_pcr_r":      get(5, 2),
        "wind_box_dp_l":           get(6, 1),
        "wind_box_dp_r":           get(6, 2),
        "total_pa_flow":           get(7, 1),

        # Right column: FG Temps (col F = index 5)
        "fg_temp_after_dpsh_l":    get(1, 5),
        "fg_temp_after_dpsh_r":    get(1, 6),
        "fg_temp_after_psh_l":     get(2, 5),
        "fg_temp_after_psh_r":     get(2, 6),
        "fg_temp_after_rh_l":      get(3, 5),
        "fg_temp_after_rh_r":      get(3, 6),
        "fg_temp_after_hsh_l":     get(4, 5),
        "fg_temp_after_hsh_r":     get(4, 6),
        "fg_temp_after_eco_l":     get(5, 5),
        "fg_temp_after_eco_r":     get(5, 6),
        "fg_temp_after_aph_l":     get(6, 5),
        "fg_temp_after_aph_r":     get(6, 6),
    }

    # FG Temp at APH inlet — single merged cell, try col 5 at row +0 of the
    # blank separator or a dedicated row; adjust offset if layout differs
    # (row header_row+8 in your screenshot shows "Total PA flow (TPH)")
    # We'll also try reading the single-value FG temp rows that appear
    # below the L/R section in col F
    try:
        params["fg_temp_dpsh_inlet"] = clean(df.iloc[header_row + 1, 5]) \
            if params["fg_temp_after_dpsh_l"] is None else params["fg_temp_after_dpsh_l"]
    except:
        pass

    return params


# ─────────────────────────────────────────────────────────────
# Helper: Extract Coal Mill Parameters
#
# Layout:
#   Row "COAL MILLS": labels row → Mills A B C D E F in cols B-G (1-6)
#   +1: Coal Flow (TPH)
#   +2: PA Flow (TPH)
#   +3: Mill DP (mmwc)
#   +4: O2 at APH (%) — actually "O₂ at APH inlet (PCR) %"
#   +5: Mill Outlet Temp (°C)
#   +6: Mill Current (Amp)
# ─────────────────────────────────────────────────────────────
def extract_coal_mill_params(df):
    header_row = find_row(df, "COAL MILLS")
    if header_row is None:
        return None

    mills = ["A", "B", "C", "D", "E", "F"]
    col_offset = 1  # Mill A starts at col index 1

    def get(r_offset, col_idx):
        try:
            return clean(df.iloc[header_row + r_offset, col_idx])
        except:
            return None

    result = []
    for i, mill in enumerate(mills):
        col = col_offset + i
        result.append({
            "mill":             mill,
            "coal_flow_tph":    get(1, col),
            "pa_flow_tph":      get(2, col),
            "mill_dp_mmwc":     get(3, col),
            "o2_at_aph_pcr":    get(4, col),   # may be blank for some mills
            "mill_outlet_temp": get(5, col),
            "mill_current_amp": get(6, col),
        })

    return result


# ─────────────────────────────────────────────────────────────
# DB HELPERS — insert boiler params & coal mill params
# ─────────────────────────────────────────────────────────────
def upsert_boiler_mill_params(cur, run_id, params):
    """
    DELETE existing row for this run_id then INSERT fresh.
    Table: boiler_mill_params
    """
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
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s
        )
    """, (
        run_id,
        params.get("main_steam_pressure_l"),   params.get("main_steam_pressure_r"),
        params.get("main_steam_flow_l"),        params.get("main_steam_flow_r"),
        params.get("superheat_spray_l"),        params.get("superheat_spray_r"),
        params.get("reheat_spray_l"),           params.get("reheat_spray_r"),
        params.get("o2_aph_inlet_pcr_l"),       params.get("o2_aph_inlet_pcr_r"),
        params.get("wind_box_dp_l"),            params.get("wind_box_dp_r"),
        params.get("total_pa_flow"),
        params.get("fg_temp_after_dpsh_l"),     params.get("fg_temp_after_dpsh_r"),
        params.get("fg_temp_after_psh_l"),      params.get("fg_temp_after_psh_r"),
        params.get("fg_temp_after_rh_l"),       params.get("fg_temp_after_rh_r"),
        params.get("fg_temp_after_hsh_l"),      params.get("fg_temp_after_hsh_r"),
        params.get("fg_temp_after_eco_l"),      params.get("fg_temp_after_eco_r"),
        params.get("fg_temp_after_aph_l"),      params.get("fg_temp_after_aph_r"),
    ))


def upsert_coal_mill_params(cur, run_id, mills):
    """
    DELETE existing rows for this run_id then INSERT fresh.
    Table: coal_mill_params
    """
    cur.execute("DELETE FROM coal_mill_params WHERE run_id = %s", (run_id,))
    for m in mills:
        cur.execute("""
            INSERT INTO coal_mill_params (
                run_id, mill,
                coal_flow_tph, pa_flow_tph, mill_dp_mmwc,
                o2_at_aph_pcr, mill_outlet_temp, mill_current_amp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            run_id,
            m["mill"],
            m.get("coal_flow_tph"),
            m.get("pa_flow_tph"),
            m.get("mill_dp_mmwc"),
            m.get("o2_at_aph_pcr"),
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
        # 1. Save file
        file_id = str(uuid.uuid4())
        file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")

        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        # 2. Read ALL sheets
        df_dict = pd.read_excel(file_path, sheet_name=None, header=None)

        conn = get_db()
        cur = conn.cursor()

        results = []

        for sheet_name, df in df_dict.items():

            # 3. Extract date
            run_date = extract_date_from_sheet(df)
            if not run_date:
                print(f"Skipping sheet {sheet_name} (no date found)")
                continue

            # ── 4a. Extract Elevation Table ───────────────────
            elevation, c1, c2, c3, c4, avg = [], [], [], [], [], []

            start = None
            for i, row in df.iterrows():
                if "ELEVATION" in str(row[0]).upper():
                    start = i + 1
                    break

            if start is None:
                print(f"Skipping sheet {sheet_name} (no elevation table)")
                continue

            for i in range(start, len(df)):
                row = df.iloc[i]
                if pd.isna(row[0]):
                    break
                elevation.append(clean(row[0]))
                c1.append(clean(row[1]))
                c2.append(clean(row[2]))
                c3.append(clean(row[3]))
                c4.append(clean(row[4]))
                avg.append(clean(row[5]))

            if len(elevation) == 0:
                continue

            # ── 4b. Extract Boiler & Mill Parameters ──────────
            boiler_params = extract_boiler_mill_params(df)

            # ── 4c. Extract Coal Mill Parameters ──────────────
            coal_mills = extract_coal_mill_params(df)

            # ── 5. UPSERT RUN ─────────────────────────────────
            cur.callproc("sp_create_run", (
                station_id,
                unit_id,
                file.filename,
                datetime.now(),
                run_date,
                uploaded_by,
                notes
            ))
            run_id = cur.fetchall()[0]["run_id"]

            # ── 6. Elevation points JSON ───────────────────────
            points = []
            for i in range(len(elevation)):
                points.append({
                    "elevation": elevation[i],
                    "c1": c1[i],
                    "c2": c2[i],
                    "c3": c3[i],
                    "c4": c4[i],
                    "avg": avg[i]
                })

            cur.callproc("sp_add_run_points_bulk", (
                run_id,
                json.dumps(points)
            ))

            # ── 7. Boiler & Mill params ────────────────────────
            if boiler_params:
                upsert_boiler_mill_params(cur, run_id, boiler_params)

            # ── 8. Coal Mill params ────────────────────────────
            if coal_mills:
                upsert_coal_mill_params(cur, run_id, coal_mills)

            results.append({
                "sheet":              sheet_name,
                "run_id":             run_id,
                "date":               str(run_date),
                "rows":               len(points),
                "boiler_params":      boiler_params is not None,
                "coal_mill_rows":     len(coal_mills) if coal_mills else 0,
            })

        conn.commit()
        conn.close()

        return {
            "message":                "Upload processed successfully",
            "total_sheets_processed": len(results),
            "runs":                   results
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


# ─── GET SINGLE RUN ───────────────────────────────────────────
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


# ─── GET BOILER & MILL PARAMS FOR A RUN ───────────────────────
@app.get("/history/{run_id}/boiler-params")
def get_boiler_params(run_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM boiler_mill_params WHERE run_id = %s",
        (run_id,)
    )
    data = cur.fetchone()
    conn.close()
    return data or {}


# ─── GET COAL MILL PARAMS FOR A RUN ───────────────────────────
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


# ─── COMPARE RUNS ─────────────────────────────────────────────
@app.get("/compare")
def compare_runs(ids: str):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_comparison_data", (ids,))
    data = cur.fetchall()
    conn.close()
    return data


# ─── STATIONS ─────────────────────────────────────────────────
@app.get("/stations")
def get_stations():
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_stations")
    data = cur.fetchall()
    conn.close()
    return data


# ─── UNITS ────────────────────────────────────────────────────
@app.get("/units/{station_id}")
def get_units(station_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_units_by_station", (station_id,))
    data = cur.fetchall()
    conn.close()
    return data


# ─── MAPPING DATES ────────────────────────────────────────────
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