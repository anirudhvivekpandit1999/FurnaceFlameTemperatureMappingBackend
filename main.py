from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
import shutil
import os
import pandas as pd
import json
import uuid
from datetime import datetime
import math
import traceback

app = FastAPI()

origins = [
    "http://localhost:5173",
    "https://furnaceflametemperaturemapping.vercel.app"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"
DATA_DIR   = "saved_data"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(DATA_DIR,   exist_ok=True)


# ─── helpers ──────────────────────────────────────────────────────────────────

def clean_value(val):
    """Return a JSON-safe Python scalar, or None."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    if hasattr(val, "item"):          # numpy scalar → native Python
        return val.item()
    return val


def safe_str(val):
    """Return a non-empty stripped string, or None."""
    v = clean_value(val)
    if v is None:
        return None
    s = str(v).strip()
    return s if s and s.lower() not in ("nan", "none", "") else None


def cell(row, col_idx):
    """Safely get a cleaned cell value from a pandas Series by integer position."""
    if col_idx >= len(row):
        return None
    return clean_value(row.iloc[col_idx])


def find_row(df, keyword):
    """First row index where any cell contains keyword (case-insensitive)."""
    kw = keyword.upper()
    for i, row in df.iterrows():
        if any(kw in str(c).upper() for c in row.values):
            return i
    return None


# ─── upload route ─────────────────────────────────────────────────────────────

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), date: str = Form(None)):
    try:
        file_id   = str(uuid.uuid4())
        timestamp = date if date else datetime.utcnow().date().isoformat()

        file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")
        with open(file_path, "wb") as buf:
            shutil.copyfileobj(file.file, buf)

        xl           = pd.ExcelFile(file_path)
        latest_sheet = xl.sheet_names[-1]

        # Read with no header so every cell is addressable by integer index
        df = pd.read_excel(file_path, sheet_name=latest_sheet, header=None)

        # =====================================================================
        # SECTION 1 — HEADER METADATA  (rows 1–5, 0-indexed)
        #
        # Row 0  : Title row — "FURNACE FLAME TEMPERATURE MAPPING"
        # Row 1  : col0="UNIT : 5" area  | col3="Date :- 18/03/2026" | col6="Time :- 9:00"
        # Row 2  : col0="Load (MW) :-"   col1=495 | col3="Total Coal flow (TPH) :-" col6=355
        # Row 3  : col0="Coal Mills in Service :" col1=6 col2="ABDEFG"
        #          col3="Total Air flow (TPH) :-" col6=1655
        # Row 4  : col0="Oil Guns in Service :"  col1=value
        #          col3="Burner Tilt Position (Deg) :-" col4=tilt_value
        # =====================================================================
        meta = {}
        try:
            r1 = df.iloc[1]
            meta["unit"]     = safe_str(cell(r1, 1))
            meta["date"]     = safe_str(cell(r1, 4))
            meta["time"]     = safe_str(cell(r1, 6))

            r2 = df.iloc[2]
            meta["load_mw"]             = cell(r2, 1)
            meta["total_coal_flow_tph"] = cell(r2, 6)

            r3 = df.iloc[3]
            meta["coal_mills_in_service"] = cell(r3, 1)
            meta["coal_mills_list"]       = safe_str(cell(r3, 2))
            meta["total_air_flow_tph"]    = cell(r3, 6)

            r4 = df.iloc[4]
            meta["oil_guns_in_service"]  = cell(r4, 1)
            meta["burner_tilt_position"] = safe_str(cell(r4, 4))
        except Exception:
            pass  # metadata is nice-to-have; never fail the upload over it

        # =====================================================================
        # SECTION 2 — ELEVATION TABLE
        #
        # Header row: any row whose col0 cell contains "ELEVATION"
        #   col0 = Elevation (m)
        #   col1 = Corner 1
        #   col2 = Corner 2
        #   col3 = Corner 3
        #   col4 = Corner 4
        #   col5 = Average
        #   col7 = Elevation label (e.g. "58.4 M", "GH / 35.0 M") — display only
        #
        # Data rows immediately follow; stop when col0 is empty.
        # =====================================================================
        elev_header_row = find_row(df, "ELEVATION")

        elevation, corner1, corner2, corner3, corner4, average, elev_labels = \
            [], [], [], [], [], [], []

        if elev_header_row is not None:
            for i in range(elev_header_row + 1, len(df)):
                row = df.iloc[i]
                elev_val = cell(row, 0)
                if elev_val is None:
                    break
                elevation.append(elev_val)
                corner1.append(cell(row, 1))
                corner2.append(cell(row, 2))
                corner3.append(cell(row, 3))
                corner4.append(cell(row, 4))
                average.append(cell(row, 5))
                elev_labels.append(safe_str(cell(row, 7)))  # "58.4 M", "GH / 35.0 M" etc.

        # =====================================================================
        # SECTION 3 — BOILER & MILL PARAMETERS
        #
        # Header row  : "Boiler & Mill Parameters :"   (col0)
        # Sub-header  : blank | L | R | blank | blank | L | R    ← skip this row
        # Data rows:
        #   col0 = left-side parameter name
        #   col1 = left-side L value
        #   col2 = left-side R value
        #   col3 = blank
        #   col4 = right-side parameter name  (FG Temp after …)
        #   col5 = right-side L value
        #   col6 = right-side R value
        #
        # Examples from the sheet:
        #   "Main Steam Pressure (Kg/cm²) :"  | blank | 164  | blank | "FG Temp after DPSH…" | 853 | 817
        #   "Main Steam Flow (TPH) :"          | blank | 1518 | blank | "FG Temp after PSH…"  | 815 | 712
        #   "Superheat Spray (TPH) :"          | 35    | 17   | blank | "FG Temp after RH…"   | 675 | 607
        #   "Re-heat Spray (TPH) :"            | 4     | 5    | blank | "FG Temp after HSH…"  | 487 | 472
        #   "O2 at APH Inlet (PCR) %"          | 3.6   | 2.2  | blank | "FG Temp after Econ…" | 350 | 358
        #   "Wind Box DP - A / B (mmwcl) :"    | 84    | 92   | blank | "FG Temp after APH…"  | 137 | 140
        #   "Total PA flow (TPH)"              | blank | 768  | blank | blank                 |     |
        #
        # Stop condition: both col0 AND col4 are empty, OR we hit "COAL".
        # =====================================================================
        boiler_header_row = find_row(df, "BOILER")
        boiler_data = {"left": {}, "right": {}}

        if boiler_header_row is not None:
            data_start = boiler_header_row + 2   # skip title row + "L / R" sub-header

            left_params,  left_l,  left_r  = [], [], []
            right_params, right_l, right_r = [], [], []

            for i in range(data_start, len(df)):
                row = df.iloc[i]

                lp = safe_str(cell(row, 0))
                ll = cell(row, 1)
                lr = cell(row, 2)
                rp = safe_str(cell(row, 4))
                rl = cell(row, 5)
                rr = cell(row, 6)

                # Hard stop: hit coal section
                if "COAL" in (str(lp or "") + str(rp or "")).upper():
                    break

                # Soft stop: both label columns empty
                if lp is None and rp is None:
                    break

                if lp is not None:
                    left_params.append(lp)
                    left_l.append(ll)
                    left_r.append(lr)

                if rp is not None:
                    right_params.append(rp)
                    right_l.append(rl)
                    right_r.append(rr)

            boiler_data = {
                "left": {
                    "parameters": left_params,
                    "l_values":   left_l,
                    "r_values":   left_r,
                },
                "right": {
                    "parameters": right_params,
                    "l_values":   right_l,
                    "r_values":   right_r,
                },
            }

        # =====================================================================
        # SECTION 4 — COAL MILL PARAMETERS
        #
        # Row N   : "Coal Mill Parameters :-"    ← section title, col0 only
        # Row N+1 : "COAL MILLS" | "Coal Mill - A" | "Coal Mill - B" | … | "Coal Mill - H"
        #           indices:   0  |       1         |       2         | … |       8
        # Row N+2+: param | val_A | val_B | val_C | val_D | val_E | val_F | val_G | val_H
        #
        # find_row("COAL MILL") will match the title row first ("Coal Mill Parameters :-").
        # Detect this by checking whether "PARAMETER" appears on that row.
        # If yes → col-header row = title + 1, data = title + 2.
        # =====================================================================
        coal_title_row = find_row(df, "COAL MILL")
        coal_data = {}

        if coal_title_row is not None:
            title_text = " ".join(
                str(c).upper() for c in df.iloc[coal_title_row].values
            )
            col_header_row = (coal_title_row + 1) if "PARAMETER" in title_text \
                             else coal_title_row
            data_start = col_header_row + 1

            params    = []
            mill_keys = [
                "coal_mill_a", "coal_mill_b", "coal_mill_c", "coal_mill_d",
                "coal_mill_e", "coal_mill_f", "coal_mill_g", "coal_mill_h",
            ]
            mills = {k: [] for k in mill_keys}

            for i in range(data_start, len(df)):
                row   = df.iloc[i]
                param = safe_str(cell(row, 0))
                if param is None:
                    break
                params.append(param)
                for j, key in enumerate(mill_keys):
                    mills[key].append(cell(row, j + 1))

            coal_data = {"parameters": params, **mills}

        # =====================================================================
        # ASSEMBLE & SAVE
        # =====================================================================
        result = {
            "id":                     file_id,
            "filename":               file.filename,
            "timestamp":              timestamp,
            "metadata":               meta,
            "elevation":              elevation,
            "elevation_labels":       elev_labels,   # "58.4 M", "GH / 35.0 M", etc.
            "corner1":                corner1,
            "corner2":                corner2,
            "corner3":                corner3,
            "corner4":                corner4,
            "average":                average,
            "boiler_mill_parameters": boiler_data,
            "coal_mill_parameters":   coal_data,
        }

        save_path = os.path.join(DATA_DIR, f"{file_id}.json")
        with open(save_path, "w") as f:
            json.dump(result, f, indent=2)

        return result

    except Exception as e:
        return {"error": str(e), "trace": traceback.format_exc()}


# ─── history routes ───────────────────────────────────────────────────────────

@app.get("/history")
def get_history():
    history = []
    for fname in os.listdir(DATA_DIR):
        path = os.path.join(DATA_DIR, fname)
        with open(path) as f:
            data = json.load(f)
        history.append({
            "id":        data["id"],
            "filename":  data["filename"],
            "timestamp": data["timestamp"],
        })
    history.sort(key=lambda x: x["timestamp"], reverse=True)
    return history


@app.get("/history/{file_id}")
def get_single(file_id: str):
    path = os.path.join(DATA_DIR, f"{file_id}.json")
    if not os.path.exists(path):
        return {"error": "File not found"}
    with open(path) as f:
        return json.load(f)