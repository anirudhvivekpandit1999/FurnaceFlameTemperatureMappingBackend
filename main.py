from fastapi import FastAPI, UploadFile, File, Form
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
        password="Vishalgad5@3332",  # 👈 no password
        database="furnace_db",
        cursorclass=pymysql.cursors.DictCursor
    )

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ─── CLEAN HELPERS ────────────────────────────────────────────
from fastapi import UploadFile, File, Form
from datetime import datetime
import pandas as pd
import uuid, os, shutil, json, re

# ─────────────────────────────────────────────
# Helper: Clean values
# ─────────────────────────────────────────────
def clean(val):
    try:
        if pd.isna(val):
            return None
        return float(val)
    except:
        return None


# ─────────────────────────────────────────────
# Helper: Extract date (ddmmyyyy)
# ─────────────────────────────────────────────
def extract_date_from_sheet(df):
    pattern = r'\b(\d{2})(\d{2})(\d{4})\b'

    for i in range(min(20, len(df))):  # scan top 20 rows
        for val in df.iloc[i]:
            if isinstance(val, str):
                match = re.search(pattern, val)
                if match:
                    d, m, y = match.groups()
                    try:
                        return datetime(int(y), int(m), int(d)).date()
                    except:
                        continue
    return None


# ─────────────────────────────────────────────
# MAIN API
# ─────────────────────────────────────────────
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

        # ─────────────────────────────────────────────
        # LOOP THROUGH ALL SHEETS
        # ─────────────────────────────────────────────
        for sheet_name, df in df_dict.items():

            # 3. Extract date from sheet
            run_date = extract_date_from_sheet(df)
            if not run_date:
                print(f"Skipping sheet {sheet_name} (no date found)")
                continue

            # 4. Extract table
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

            # 5. UPSERT RUN (IMPORTANT)
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

            # 6. Prepare JSON points
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

            # 7. Replace points (delete + insert)
            cur.callproc("sp_add_run_points_bulk", (
                run_id,
                json.dumps(points)
            ))

            results.append({
                "sheet": sheet_name,
                "run_id": run_id,
                "date": str(run_date),
                "rows": len(points)
            })

        conn.commit()
        conn.close()

        return {
            "message": "Upload processed successfully",
            "total_sheets_processed": len(results),
            "runs": results
        }

    except Exception as e:
        return {"error": str(e)}

# ─── GET RUNS (HISTORY) ───────────────────────────────────────
@app.get("/history")
def get_history(station_id: int, unit_id: int):
    conn = get_db()
    cur = conn.cursor()

    cur.callproc("sp_get_runs", (
        station_id,
        unit_id,
        "2000-01-01",
        "2100-01-01"
    ))

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

    # transform for frontend
    return {
        "elevation": [r["elevation"] for r in rows],
        "corner1": [r["c1"] for r in rows],
        "corner2": [r["c2"] for r in rows],
        "corner3": [r["c3"] for r in rows],
        "corner4": [r["c4"] for r in rows],
        "average": [r["avg_val"] for r in rows],
    }

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