#!/usr/bin/env python3

from fastapi import FastAPI, UploadFile, File, Form, Query, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import pandas as pd
import pymysql
import json
import uuid
import shutil
import os
from datetime import datetime, timedelta
import re
from pydantic import BaseModel
import bcrypt
import jwt

from crypto import encrypt_at_rest, decrypt_at_rest_float

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
JWT_SECRET = os.getenv("JWT_SECRET", "supersecret")
JWT_ALGO = "HS256"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS","Vishalgad5@3332"),
    "database": os.getenv("DB_NAME", "furnace_db"),
}

MAX_FILE_SIZE_MB = 10

app = FastAPI()
security = HTTPBearer()

# ─────────────────────────────────────────────────────────────
# CORS
# ─────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # restrict in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────────────────────
def get_db():
    return pymysql.connect(
        **DB_CONFIG,
        cursorclass=pymysql.cursors.DictCursor,
    )

# ─────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────
def create_token(username):
    payload = {
        "sub": username,
        "exp": datetime.utcnow() + timedelta(hours=8)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
        return payload["sub"]
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def clean(val):
    try:
        if pd.isna(val):
            return None
        return float(val)
    except:
        return None

def enc(v):
    return encrypt_at_rest(v) if v is not None else None

def dec(v):
    return decrypt_at_rest_float(v) if v is not None else None

# ─────────────────────────────────────────────────────────────
# LOGIN
# ─────────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/login")
def login(data: LoginRequest):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT password FROM user_master WHERE username=%s", (data.username,))
    row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(401, "Invalid credentials")

    if not bcrypt.checkpw(data.password.encode(), row["password"].encode()):
        raise HTTPException(401, "Invalid credentials")

    token = create_token(data.username)

    return {"token": token}

# ─────────────────────────────────────────────────────────────
# FILE UPLOAD
# ─────────────────────────────────────────────────────────────
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    user: str = Depends(verify_token)
):
    # Validate file
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Invalid file type")

    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(400, "File too large")

    file_id = str(uuid.uuid4())
    file_path = os.path.join(UPLOAD_DIR, file_id + ".xlsx")

    with open(file_path, "wb") as f:
        f.write(contents)

    df_dict = pd.read_excel(file_path, sheet_name=None, header=None)

    conn = get_db()
    cur = conn.cursor()

    results = []

    for sheet_name, df in df_dict.items():
        run_date = datetime.now().date()

        elevation, c1, c2, c3, c4, avg = [], [], [], [], [], []

        for i, row in df.iterrows():
            val = clean(row[0])
            if val is None:
                continue

            elevation.append(enc(val))
            c1.append(enc(clean(row[1])))
            c2.append(enc(clean(row[2])))
            c3.append(enc(clean(row[3])))
            c4.append(enc(clean(row[4])))
            avg.append(enc(clean(row[5])))

        if not elevation:
            continue

        cur.execute(
            "INSERT INTO runs (created_at, run_date, uploaded_by) VALUES (%s,%s,%s)",
            (datetime.now(), run_date, user)
        )
        run_id = cur.lastrowid

        for i in range(len(elevation)):
            cur.execute(
                """
                INSERT INTO profile_points
                (run_id,elevation,c1,c2,c3,c4,avg_val)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (run_id, elevation[i], c1[i], c2[i], c3[i], c4[i], avg[i])
            )

        results.append({"sheet": sheet_name, "run_id": run_id})

    conn.commit()
    conn.close()

    return {"runs": results}

# ─────────────────────────────────────────────────────────────
# HISTORY (DECRYPTED)
# ─────────────────────────────────────────────────────────────
@app.get("/history/{run_id}")
def get_run(run_id: int, user: str = Depends(verify_token)):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM profile_points WHERE run_id=%s", (run_id,))
    rows = cur.fetchall()

    conn.close()

    return {
        "elevation": [dec(r["elevation"]) for r in rows],
        "c1": [dec(r["c1"]) for r in rows],
        "c2": [dec(r["c2"]) for r in rows],
        "c3": [dec(r["c3"]) for r in rows],
        "c4": [dec(r["c4"]) for r in rows],
        "avg": [dec(r["avg_val"]) for r in rows],
    }

# ─────────────────────────────────────────────────────────────
# DELETE
# ─────────────────────────────────────────────────────────────
@app.delete("/runs/{run_id}")
def delete_run(run_id: int, user: str = Depends(verify_token)):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("DELETE FROM profile_points WHERE run_id=%s", (run_id,))
    cur.execute("DELETE FROM runs WHERE run_id=%s", (run_id,))

    conn.commit()
    conn.close()

    return {"message": "Deleted"}