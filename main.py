
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import shutil
import os
import pandas as pd

app = FastAPI()

origins = [
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        file_path = os.path.join(UPLOAD_DIR, file.filename)

        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        df = pd.read_excel(file_path, header=None)

        print(df.head(20))

        start_row = None
        for i, row in df.iterrows():
            if any("ELEVATION" in str(cell).upper() for cell in row.values):
                start_row = i
                break

        if start_row is None:
            return {"error": "ELEVATION table not found"}

        headers = df.iloc[start_row]
        data = df.iloc[start_row+1:start_row+15]

        data.columns = [str(col).strip().upper() for col in headers]

        data = data.loc[:, ~data.columns.duplicated()]

        print("COLUMNS:", data.columns.tolist())

        if "ELEVATION" not in data.columns:
            return {"error": f"ELEVATION column missing. Found: {data.columns.tolist()}"}

        data = data.dropna(subset=["ELEVATION"])

        def get_col(df, col_name):
            if col_name not in df.columns:
                return []
            col = df[col_name]
            if isinstance(col, pd.DataFrame):
                col = col.iloc[:, 0]
            return col.tolist()

        import math

        def clean_list(lst):
            cleaned = []
            for x in lst:
                if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
                    cleaned.append(None)
                else:
                    cleaned.append(x)
            return cleaned

        result = {
            "elevation": clean_list(get_col(data, "ELEVATION")),
            "corner1": clean_list(get_col(data, "CORNER 1")),
            "corner2": clean_list(get_col(data, "CORNER 2")),
            "corner3": clean_list(get_col(data, "CORNER 3")),
            "corner4": clean_list(get_col(data, "CORNER 4")),
            "average": clean_list(get_col(data, "AVERAGE"))
        }

        print("RESULT:", result)

        return result

    except Exception as e:
        print("ERROR:", str(e))
        return {"error": str(e)}



