from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import shutil
import os
import pandas as pd
import json
import uuid
from datetime import datetime
import math

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
DATA_DIR = "saved_data"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)


def clean_list(lst):
    cleaned = []
    for x in lst:
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            cleaned.append(None)
        else:
            cleaned.append(x)
    return cleaned


def get_col(df, col_name):
    if col_name not in df.columns:
        return []
    col = df[col_name]
    if isinstance(col, pd.DataFrame):
        col = col.iloc[:, 0]
    return col.tolist()


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        file_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()

        file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")

        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        excel_file = pd.ExcelFile(file_path)
        sheet_names = excel_file.sheet_names
        latest_sheet = sheet_names[-1] 

        df = pd.read_excel(file_path, sheet_name=latest_sheet, header=None)

        start_row = None
        for i, row in df.iterrows():
            if any("ELEVATION" in str(cell).upper() for cell in row.values):
                start_row = i
                break

        if start_row is None:
            return {"error": "ELEVATION table not found"}

        headers = df.iloc[start_row]
        data = df.iloc[start_row+1:start_row+10]

        data.columns = [str(col).strip().upper() for col in headers]
        data = data.loc[:, ~data.columns.duplicated()]

        if "ELEVATION" not in data.columns:
            return {"error": f"ELEVATION column missing. Found: {data.columns.tolist()}"}

        data = data.dropna(subset=["ELEVATION"])

        result = {
            "id": file_id,
            "filename": file.filename,
            "timestamp": timestamp,
            "elevation": clean_list(get_col(data, "ELEVATION")),
            "corner1": clean_list(get_col(data, "CORNER 1")),
            "corner2": clean_list(get_col(data, "CORNER 2")),
            "corner3": clean_list(get_col(data, "CORNER 3")),
            "corner4": clean_list(get_col(data, "CORNER 4")),
            "average": clean_list(get_col(data, "AVERAGE"))
        }

        save_path = os.path.join(DATA_DIR, f"{file_id}.json")
        with open(save_path, "w") as f:
            json.dump(result, f)

        return result

    except Exception as e:
        return {"error": str(e)}


@app.get("/history")
def get_history():
    files = os.listdir(DATA_DIR)

    history = []
    for file in files:
        path = os.path.join(DATA_DIR, file)
        with open(path, "r") as f:
            data = json.load(f)

            history.append({
                "id": data["id"],
                "filename": data["filename"],
                "timestamp": data["timestamp"]
            })

    history.sort(key=lambda x: x["timestamp"], reverse=True)

    return history


@app.get("/history/{file_id}")
def get_single(file_id: str):
    path = os.path.join(DATA_DIR, f"{file_id}.json")

    if not os.path.exists(path):
        return {"error": "File not found"}

    with open(path, "r") as f:
        data = json.load(f)

    return data