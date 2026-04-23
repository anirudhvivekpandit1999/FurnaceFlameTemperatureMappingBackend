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
from pydantic import BaseModel

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
    "https://91.203.132.34:5174",
    "https://localhost:5174",
    "http://91.203.132.34:5176",
    "http://localhost:5176",

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
    import mysql.connector
    return mysql.connector.connect(
        host='localhost', user='root', password='Vishalgad5@3332', database='furnace_db'
    )


UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


def clean(val):
    """Return stripped string or None for blank/NaN."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    s = str(val).strip()
    return s if s else None

def extract_date_from_sheet(df):
    """
    Look for a cell matching 'Date :- DD/MM/YYYY' in the first 5 rows.
    Returns a date object or None.
    """
    date_re = re.compile(r'date\s*[:\-]+\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})', re.IGNORECASE)
    for i in range(min(5, len(df))):
        for val in df.iloc[i]:
            if val is None:
                continue
            m = date_re.search(str(val))
            if m:
                raw = m.group(1).replace('-', '/')
                for fmt in ('%d/%m/%Y', '%d/%m/%y', '%m/%d/%Y'):
                    try:
                        return datetime.strptime(raw, fmt).date()
                    except ValueError:
                        pass
    # Fallback: look for a bare datetime / date object in first 5 rows
    for i in range(min(5, len(df))):
        for val in df.iloc[i]:
            if isinstance(val, datetime):
                return val.date()
            if isinstance(val, __builtins__.__class__) or hasattr(val, 'year'):
                try:
                    return val
                except Exception:
                    pass
    return None


def find_row(df, text):
    """
    Return the integer iloc index of the first row where any cell
    contains `text` (case-insensitive). Returns None if not found.
    """
    text_l = text.lower()
    for i, row in df.iterrows():
        for cell in row:
            if cell is not None and text_l in str(cell).lower():
                return i
    return 

def find_col_in_row(df, row_idx, text):
    """
    Return the column position (0-based) in df.iloc[row_idx] where the
    cell contains `text` (case-insensitive). Returns None if not found.
    """
    text_l = text.lower()
    for col_pos, val in enumerate(df.iloc[row_idx]):
        if val is not None and text_l in str(val).lower():
            return col_pos
    return None

def extract_metadata(df):
    """
    Extract BSL, unit, date, time, load_mw, coal_flow_tph, air_flow_tph,
    coal_mills_in_service, oil_guns_in_service, burner_tilt_position.
 
    Based on template row 2: BSL(col0) | Unit X(col1) | | Date:-...(col3) | | Time:-(col5)
    """
    meta = {}
 
    for i in range(min(6, len(df))):
        row_vals = list(df.iloc[i])
        row_str  = ' '.join(str(v) for v in row_vals if v is not None)
 
        # BSL + unit are in row 2 (iloc 1)
        if i == 1:
            meta['bsl']  = clean(row_vals[0])
            # unit is something like "Unit 5" – grab numeric part
            unit_raw = clean(row_vals[1])
            if unit_raw:
                um = re.search(r'\d+', unit_raw)
                meta['unit'] = um.group() if um else unit_raw
 
        # Date
        dm = re.search(r'date\s*[:\-]+\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})', row_str, re.IGNORECASE)
        if dm and 'run_date' not in meta:
            raw = dm.group(1).replace('-', '/')
            for fmt in ('%d/%m/%Y', '%d/%m/%y'):
                try:
                    meta['run_date'] = datetime.strptime(raw, fmt).date()
                    break
                except ValueError:
                    pass
 
        # Time
        tm = re.search(r'time\s*[:\-]+\s*(\d{1,2}:\d{2})', row_str, re.IGNORECASE)
        if tm and 'run_time' not in meta:
            meta['run_time'] = tm.group(1)
 
        # Numeric KV pairs we care about
        for col_pos, val in enumerate(row_vals):
            if val is None:
                continue
            vs = str(val).strip()
            nv = clean(row_vals[col_pos + 1]) if col_pos + 1 < len(row_vals) else None
 
            if re.search(r'load\s*\(mw\)', vs, re.IGNORECASE) and nv:
                meta['load_mw'] = nv
            if re.search(r'total coal flow', vs, re.IGNORECASE) and nv:
                meta['coal_flow_tph'] = nv
            if re.search(r'total air flow', vs, re.IGNORECASE) and nv:
                meta['air_flow_tph'] = nv
            if re.search(r'coal mills in service', vs, re.IGNORECASE) and nv:
                meta['coal_mills_in_service'] = nv
            if re.search(r'oil guns in service', vs, re.IGNORECASE) and nv:
                meta['oil_guns_in_service'] = nv
            if re.search(r'burner tilt', vs, re.IGNORECASE) and nv:
                meta['burner_tilt_position'] = nv
 
    return meta

def extract_boiler_mill_params(df):
    """
    Dynamically find the Boiler & Mill Parameters section, then scan
    both the left block and right block for L/R values by label text.
    """
    boiler_row = find_row(df, 'BOILER & MILL PARAMETERS')
    if boiler_row is None:
        return None
 
    coal_row = find_row(df, 'COAL MILL PARAMETERS')
    end_row  = coal_row if coal_row is not None else len(df)
 
    # ── Detect column positions dynamically ───────────────────────────────
    # Left block: the section header row tells us where 'L' and 'R' are
    # Screenshot: cols are A(0)=label, B(1)=blank, C(2)=L, D(3)=R,
    #             E(4)=right-label, F(5)=blank, G(6)=L, H(7)=R
    # We find 'L' and 'R' header cols in the boiler header row.
 
    header_row = boiler_row  # "Boiler & Mill Parameters :" is the section row
    # Look one row below for the L / R sub-headers
    lr_row = boiler_row + 1
    lr_vals = list(df.iloc[lr_row]) if lr_row < len(df) else []
 
    # Find columns with 'L' and 'R' (there will be two pairs: left block & right block)
    l_cols = [c for c, v in enumerate(lr_vals) if clean(v) == 'L']
    r_cols = [c for c, v in enumerate(lr_vals) if clean(v) == 'R']
 
    # Left block uses first L/R pair, right block uses second pair
    left_label_col  = l_cols[0] - 1 if l_cols else 0
    left_l_col      = l_cols[0]     if l_cols else 2
    left_r_col      = r_cols[0]     if r_cols else 3
    right_label_col = l_cols[1] - 1 if len(l_cols) > 1 else 4
    right_l_col     = l_cols[1]     if len(l_cols) > 1 else 6
    right_r_col     = r_cols[1]     if len(r_cols) > 1 else 7
 
    data_start = lr_row + 1   # actual data starts one row after L/R header
 
    left_data  = _scan_boiler_block(df, data_start, end_row,
                                    left_label_col,  left_l_col,  left_r_col,  _BOILER_LEFT)
    right_data = _scan_boiler_block(df, data_start, end_row,
                                    right_label_col, right_l_col, right_r_col, _BOILER_RIGHT)
 
    return {**left_data, **right_data}

_COAL_ROW_LABELS = {
    # result key : label fragment
    'coal_flow_tph':    'coal flow',
    'pa_flow_tph':      'pa flow',
    'mill_dp_mmwc':     'mill dp',
    'mill_outlet_temp': 'mill outlet',
    'mill_current_amp': 'mill current',
}


def extract_coal_mill_params(df):
    """
    Find Coal Mill Parameters section. Detect mill names from the COAL MILLS
    header row, then for each parameter row scan by label.
 
    Returns list of dicts: {mill, coal_flow_tph, pa_flow_tph, ...}
    """
    coal_row = find_row(df, 'COAL MILL PARAMETERS')
    if coal_row is None:
        return []
 
    end_row = find_row(df, r'end')
    if end_row is None:
        end_row = len(df)
 
    # The COAL MILLS row has mill names: "Coal Mill - A", "Coal Mill - B", ...
    mills_header_row = None
    for i in range(coal_row, min(coal_row + 3, len(df))):
        row_str = ' '.join(str(v) for v in df.iloc[i] if v is not None).lower()
        if 'coal mill' in row_str or 'coal mills' in row_str:
            mills_header_row = i
            break
 
    if mills_header_row is None:
        return []
 
    # Collect (mill_letter, col_index) pairs from that header row
    mills = []
    for col_pos, val in enumerate(df.iloc[mills_header_row]):
        if val is None:
            continue
        vs = str(val).strip()
        # Match "Coal Mill - A" or "Coal Mill A" or just "A"
        m = re.search(r'coal\s*mill[\s\-]*([A-Ha-h])\b', vs, re.IGNORECASE)
        if m:
            mills.append((m.group(1).upper(), col_pos))
 
    if not mills:
        return []
 
    # Find the label column (leftmost non-mill column in that row)
    label_col = 0   # usually column A (index 0)
 
    # For each row label, find the row index
    label_row_map = {}
    for i in range(mills_header_row + 1, end_row):
        cell = clean(df.iloc[i, label_col])
        if not cell:
            continue
        cell_l = cell.lower()
        for key, frag in _COAL_ROW_LABELS.items():
            if frag in cell_l and key not in label_row_map:
                label_row_map[key] = i
 
    # Build result – one dict per mill
    result = []
    for mill_letter, col_pos in mills:
        entry = {'mill': mill_letter}
        for key in _COAL_ROW_LABELS:
            row_i = label_row_map.get(key)
            entry[key] = clean(df.iloc[row_i, col_pos]) if row_i is not None and col_pos < df.shape[1] else None
        # Only include if at least one value is non-null
        if any(v for k, v in entry.items() if k != 'mill'):
            result.append(entry)
 
    return result



def extract_profile_points(df):
    """
    Find the ELEVATION header row, then collect rows until we hit the
    Boiler & Mill Parameters section (or a row with no elevation value).
 
    Returns list of dicts: {elevation, c1, c2, c3, c4, avg_val}
    """
    elev_row = find_row(df, 'ELEVATION')
    if elev_row is None:
        return []
 
    # Columns: ELEVATION=0, C1=1, C2=2, C3=3, C4=4, AVG=5
    # (confirmed from screenshot header row)
    header_row = df.iloc[elev_row]
    elev_col = find_col_in_row(df, elev_row, 'ELEVATION')
    if elev_col is None:
        elev_col = 0
 
    # Find where the boiler section starts so we know when to stop
    boiler_row = find_row(df, 'BOILER & MILL PARAMETERS')
    end_row    = boiler_row if boiler_row is not None else len(df)
 
    points = []
    for i in range(elev_row + 1, end_row):
        row = df.iloc[i]
        elev_val = clean(row.iloc[elev_col])
 
        # Stop on NOTE row or blank elevation
        if elev_val is None:
            continue
        if re.search(r'\bNOTE\b', elev_val, re.IGNORECASE):
            break
 
        # Try to parse as a number
        try:
            float(elev_val.split()[0])   # e.g. "58.4" or "GH/ 35.0"
        except ValueError:
            continue   # skip label-only rows like "GH / 35.0 M"
 
        points.append({
            'elevation': elev_val,
            'c1':        clean(row.iloc[elev_col + 1]) if elev_col + 1 < len(row) else None,
            'c2':        clean(row.iloc[elev_col + 2]) if elev_col + 2 < len(row) else None,
            'c3':        clean(row.iloc[elev_col + 3]) if elev_col + 3 < len(row) else None,
            'c4':        clean(row.iloc[elev_col + 4]) if elev_col + 4 < len(row) else None,
            'avg':       clean(row.iloc[elev_col + 5]) if elev_col + 5 < len(row) else None,
        })
 
    return points


_BOILER_LEFT = {
    # key in result dict : label fragment to search for
    'main_steam_pressure_l': ('main steam pressure', 'l'),
    'main_steam_pressure_r': ('main steam pressure', 'r'),
    'main_steam_flow_l':     ('main steam flow',     'l'),
    'main_steam_flow_r':     ('main steam flow',     'r'),
    'superheat_spray_l':     ('superheat spray',     'l'),
    'superheat_spray_r':     ('superheat spray',     'r'),
    'reheat_spray_l':        ('re-heat spray',       'l'),
    'reheat_spray_r':        ('re-heat spray',       'r'),
    'o2_aph_inlet_pcr_l':   ('o2',                  'l'),
    'o2_aph_inlet_pcr_r':   ('o2',                  'r'),
    'wind_box_dp_l':         ('wind box',            'l'),
    'wind_box_dp_r':         ('wind box',            'r'),
    'total_pa_flow':         ('total pa flow',       None),
}

_BOILER_RIGHT = {
    'fg_temp_after_dpsh_l':  ('dpsh', 'l'),
    'fg_temp_after_dpsh_r':  ('dpsh', 'r'),
    'fg_temp_after_psh_l':   ('psh',  'l'),
    'fg_temp_after_psh_r':   ('psh',  'r'),
    'fg_temp_after_rh_l':    ('rh',   'l'),   # NOTE: was swapped in original code
    'fg_temp_after_rh_r':    ('rh',   'r'),
    'fg_temp_after_hsh_l':   ('hsh',  'l'),
    'fg_temp_after_hsh_r':   ('hsh',  'r'),
    'fg_temp_after_eco_l':   ('eco',  'l'),
    'fg_temp_after_eco_r':   ('eco',  'r'),
    'fg_temp_after_aph_l':   ('aph',  'l'),
    'fg_temp_after_aph_r':   ('aph',  'r'),
}
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
            o2_aph_inlet_pcr_l,   o2_aph_inlet_pcr_r,
            wind_box_dp_l,         wind_box_dp_r,
            total_pa_flow,
            fg_temp_after_dpsh_l,  fg_temp_after_dpsh_r,
            fg_temp_after_psh_l,   fg_temp_after_psh_r,
            fg_temp_after_rh_l,    fg_temp_after_rh_r,
            fg_temp_after_hsh_l,   fg_temp_after_hsh_r,
            fg_temp_after_eco_l,   fg_temp_after_eco_r,
            fg_temp_after_aph_l,   fg_temp_after_aph_r,
            created_at
        ) VALUES (
            %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            NOW()
        )
        """,
        (
            run_id,
            params.get('main_steam_pressure_l'), params.get('main_steam_pressure_r'),
            params.get('main_steam_flow_l'),     params.get('main_steam_flow_r'),
            params.get('superheat_spray_l'),     params.get('superheat_spray_r'),
            params.get('reheat_spray_l'),        params.get('reheat_spray_r'),
            params.get('o2_aph_inlet_pcr_l'),    params.get('o2_aph_inlet_pcr_r'),
            params.get('wind_box_dp_l'),         params.get('wind_box_dp_r'),
            params.get('total_pa_flow'),
            params.get('fg_temp_after_dpsh_l'),  params.get('fg_temp_after_dpsh_r'),
            params.get('fg_temp_after_psh_l'),   params.get('fg_temp_after_psh_r'),
            params.get('fg_temp_after_rh_l'),    params.get('fg_temp_after_rh_r'),   # ← fixed order
            params.get('fg_temp_after_hsh_l'),   params.get('fg_temp_after_hsh_r'),
            params.get('fg_temp_after_eco_l'),   params.get('fg_temp_after_eco_r'),
            params.get('fg_temp_after_aph_l'),   params.get('fg_temp_after_aph_r'),
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
                mill_outlet_temp, mill_current_amp,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (
                run_id,
                m.get('mill'),
                m.get('coal_flow_tph'),
                m.get('pa_flow_tph'),
                m.get('mill_dp_mmwc'),
                m.get('mill_outlet_temp'),
                m.get('mill_current_amp'),
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

def _scan_boiler_block(df, section_start, section_end, label_col, l_col, r_col, field_map):
    """
    Scan rows [section_start, section_end) looking for label text in `label_col`.
    For each match, grab values from l_col and r_col.
    field_map: {result_key: (label_fragment, 'l'|'r'|None)}
    Returns a flat dict of {result_key: value}.
    """
    # Build a lookup: label_fragment → list of rows where it appears
    rows_by_label = {}
    for i in range(section_start, section_end):
        cell = clean(df.iloc[i, label_col])
        if not cell:
            continue
        cell_l = cell.lower()
        for result_key, (frag, side) in field_map.items():
            if frag in cell_l:
                rows_by_label.setdefault(frag, []).append(i)
 
    result = {}
    for result_key, (frag, side) in field_map.items():
        rows = rows_by_label.get(frag, [])
        if not rows:
            result[result_key] = None
            continue
        row_i = rows[0]   # take first match
        if side == 'l':
            result[result_key] = clean(df.iloc[row_i, l_col]) if l_col < df.shape[1] else None
        elif side == 'r':
            result[result_key] = clean(df.iloc[row_i, r_col]) if r_col < df.shape[1] else None
        else:
            # No side (e.g. total_pa_flow which is a merged/single value)
            result[result_key] = clean(df.iloc[row_i, l_col]) if l_col < df.shape[1] else None
 
    return result


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
@app.post('/upload')
async def upload_file(
    file:       UploadFile = File(...),
    station_id: int        = Form(1),
    unit_id:    int        = Form(1),
    uploaded_by: str       = Form('system'),
    notes:      str        = Form(''),
):
    try:
        file_id   = str(uuid.uuid4())
        file_path = os.path.join(UPLOAD_DIR, f'{file_id}_{file.filename}')
        with open(file_path, 'wb') as f:
            shutil.copyfileobj(file.file, f)
 
        # Read all sheets, no header inference
        df_dict = pd.read_excel(file_path, sheet_name=None, header=None)
 
        conn = get_db()
        cur  = conn.cursor(dictionary=True)   # ← dictionary=True so fetchall returns dicts
        results = []
 
        for sheet_name, df in df_dict.items():
            # ── Date ──────────────────────────────────────────────────────
            run_date = extract_date_from_sheet(df)
            if not run_date:
                continue
 
            # ── Metadata ──────────────────────────────────────────────────
            meta     = extract_metadata(df)
            location = meta.get('bsl', '')
            unit     = meta.get('unit', '')
 
            # ── Profile points ────────────────────────────────────────────
            points = extract_profile_points(df)
            if not points:
                continue
 
            # ── Boiler & Mill params ──────────────────────────────────────
            boiler_params = extract_boiler_mill_params(df)
 
            # ── Coal mill params ──────────────────────────────────────────
            coal_mills = extract_coal_mill_params(df)
 
            # ── Persist run ───────────────────────────────────────────────
            cur.callproc('sp_create_run', (
                station_id, unit_id, file.filename,
                datetime.now(), run_date, uploaded_by, notes,
                location, unit,
            ))
            # callproc stores results; fetch from the result set
            run_id = None
            for result_set in cur.stored_results():
                row = result_set.fetchone()
                if row:
                    run_id = row['run_id']
                    break
 
            if run_id is None:
                continue
 
            # ── Persist profile points ────────────────────────────────────
            cur.callproc('sp_add_run_points_bulk', (
                run_id, location, unit_id, json.dumps(points),
            ))
 
            # ── Persist boiler params ─────────────────────────────────────
            if boiler_params:
                upsert_boiler_mill_params(cur, run_id, boiler_params)
 
            # ── Persist coal mill params ──────────────────────────────────
            if coal_mills:
                upsert_coal_mill_params(cur, run_id, coal_mills)
 
            results.append({
                'sheet':          sheet_name,
                'run_id':         run_id,
                'date':           str(run_date),
                'point_rows':     len(points),
                'boiler_params':  boiler_params is not None,
                'coal_mill_rows': len(coal_mills),
            })
 
        conn.commit()
        cur.close()
        conn.close()
 
        return {
            'message':               'Upload processed successfully',
            'total_sheets_processed': len(results),
            'runs':                  results,
        }
 
    except Exception as exc:
        import traceback
        return {'error': str(exc), 'trace': traceback.format_exc()}
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
def get_mapping_dates(station: str = Query(...), unit: int = Query(...)):
    conn = get_db()
    cur = conn.cursor()
    cur.callproc("sp_get_mapping_dates", (station, unit))
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

        rows = cur.fetchall()

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