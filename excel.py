"""
debug_excel.py  —  Run this locally to see exact cell layout:
    python debug_excel.py "your_file.xlsx"
"""
import sys
import pandas as pd

path = sys.argv[1] if len(sys.argv) > 1 else "test.xlsx"

df_dict = pd.read_excel(path, sheet_name=None, header=None)

for sheet_name, df in df_dict.items():
    print(f"\n{'='*70}")
    print(f"SHEET: {sheet_name}")
    print(f"{'='*70}")

    # ── Find and print Boiler & Mill Parameters section ──────────────────
    boiler_row = None
    coal_row = None

    for i, row in df.iterrows():
        if "BOILER" in str(row[0]).upper() and "MILL" in str(row[0]).upper():
            boiler_row = i
        if "COAL MILLS" in str(row[0]).upper():
            coal_row = i

    if boiler_row is not None:
        print(f"\n--- BOILER & MILL PARAMETERS (header at row index {boiler_row}) ---")
        print(f"{'Row':>4}  {'Col':>4}  {'Value'}")
        for r in range(boiler_row, boiler_row + 12):
            for c in range(8):
                val = df.iloc[r, c] if r < len(df) and c < len(df.columns) else "OUT_OF_RANGE"
                if not (isinstance(val, float) and pd.isna(val)):
                    print(f"  r{r:>2}  c{c:>2}  {repr(val)}")

    if coal_row is not None:
        print(f"\n--- COAL MILL PARAMETERS (header at row index {coal_row}) ---")
        print(f"{'Row':>4}  {'Col':>4}  {'Value'}")
        for r in range(coal_row, coal_row + 10):
            for c in range(8):
                val = df.iloc[r, c] if r < len(df) and c < len(df.columns) else "OUT_OF_RANGE"
                if not (isinstance(val, float) and pd.isna(val)):
                    print(f"  r{r:>2}  c{c:>2}  {repr(val)}")

    # ── Also print the full raw grid for these sections (easier to read) ──
    if boiler_row is not None:
        print(f"\n--- RAW GRID: Boiler section (rows {boiler_row} to {boiler_row+11}) ---")
        section = df.iloc[boiler_row: boiler_row + 12, :9]
        section.index = [f"r{i}" for i in section.index]
        section.columns = [f"c{i}" for i in range(len(section.columns))]
        print(section.to_string())

    if coal_row is not None:
        print(f"\n--- RAW GRID: Coal Mill section (rows {coal_row} to {coal_row+9}) ---")
        section = df.iloc[coal_row: coal_row + 10, :9]
        section.index = [f"r{i}" for i in section.index]
        section.columns = [f"c{i}" for i in range(len(section.columns))]
        print(section.to_string())