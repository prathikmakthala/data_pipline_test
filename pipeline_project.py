# pipeline.py
# Nature Counter: Journals → Excel → Google Drive
# - Core logic only. No hard-coded credentials.
# - Accepts a config dict; falls back to env vars if cfg is None.
# - Timestamps preserved as strings (isoformat if datetime objs).
# - Country rule:
#     (A) if loc.country present → normalize US variants to "USA"; others unchanged
#     (B) elif state is a US code OR address contains a US state → "USA"
#     (C) else blank
# - Modes: cfg["RUN_MODE"] == "full" (backfill) or "inc" (incremental)

import io
import os
import json
import logging
import re
from typing import Optional, Tuple, Dict

import pandas as pd
from pymongo import MongoClient
from bson import ObjectId
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("nc-pipeline")

DB_NAME = "NC_dev_db"
JOURNALS_COL, USERS_COL, LOCATIONS_COL = "journals", "userdetails", "locations"

US_STATES = set("""
AL AK AZ AR CA CO CT DC DE FL GA HI ID IL IN IA KS KY LA MA MD ME MI MN MO MS MT
NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY PR GU VI
""".split())

# Updated Columns per user request
FINAL_COLS = [
    "Status", "User Name", "User email", "Timestamp", "n_Duration", "End Date Time",
    "n_Name", "City", "State", "Zip", "Country", "n_Place", "n_Lati", "n_Long",
    "n_park_nbr", "n_activity", "n_notes"
]

WATERMARK_FILE = "watermark.json"

def _require(cfg: Dict, key: str) -> str:
    v = cfg.get(key) or os.getenv(key)
    if not v:
        raise SystemExit(f"Missing required setting: {key}")
    return v

def _ensure_sa_file(cfg: Dict) -> str:
    """
    Returns path to service account JSON.
    If cfg has DRIVE_SA_JSON (string with full JSON), writes it to SA_JSON_PATH.
    Otherwise uses SA_JSON_PATH that must point to an existing file.
    """
    sa_inline = cfg.get("DRIVE_SA_JSON") or os.getenv("DRIVE_SA_JSON")
    sa_path   = cfg.get("SA_JSON_PATH")  or os.getenv("SA_JSON_PATH", "drive-sa.json")
    if sa_inline:
        with open(sa_path, "w") as f:
            f.write(sa_inline)
    if not os.path.exists(sa_path):
        raise SystemExit(f"Service account JSON not found at SA_JSON_PATH: {sa_path}")
    return sa_path

def _drive_client(sa_path: str):
    creds = Credentials.from_service_account_file(sa_path, scopes=["https://www.googleapis.com/auth/drive"])
    drive = build("drive", "v3", credentials=creds)
    sa_email = json.load(open(sa_path))["client_email"]
    return drive, sa_email

def _escape_q(s: str) -> str:
    return s.replace("'", "\\'")

def find_file_id(drive, name: str, folder: str) -> Optional[str]:
    q = f"name='{_escape_q(name)}' and '{folder}' in parents and trashed=false"
    r = drive.files().list(q=q, fields="files(id)", pageSize=1).execute()
    return r["files"][0]["id"] if r.get("files") else None

def upload_excel(drive, local_path: str, dest_name: str, folder: str) -> None:
    fid = find_file_id(drive, dest_name, folder)
    media = MediaFileUpload(local_path, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", resumable=True)
    req = drive.files().update(fileId=fid, media_body=media) if fid else \
          drive.files().create(body={"name": dest_name, "parents": [folder]}, media_body=media, fields="id")
    while True:
        try:
            _, resp = req.next_chunk()
            if resp:
                break
        except HttpError as e:
            log.warning("Drive upload retry: %s", e)

def download_excel(drive, name: str, folder: str) -> pd.DataFrame:
    fid = find_file_id(drive, name, folder)
    if not fid:
        return pd.DataFrame()
    buf = io.BytesIO()
    req = drive.files().get_media(fileId=fid)
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while True:
        try:
            _, done = downloader.next_chunk()
            if done: break
        except HttpError:
            break
    buf.seek(0)
    try:
        return pd.read_excel(buf, dtype=str)
    except Exception:
        return pd.DataFrame()

def decide_country(address: str, state: str, loc_country: str) -> str:
    c = (loc_country or "").strip()
    if c:
        if c.upper() in {"US", "USA", "U.S.", "UNITED STATES", "UNITED STATES OF AMERICA"}:
            return "USA"
        return c
    if (state or "").strip().upper() in US_STATES:
        return "USA"
    tokens = re.split(r"[^A-Za-z]+", (address or "").upper())
    tokens = [t for t in tokens if t]
    if any(t in US_STATES for t in tokens):
        return "USA"
    return ""

def agg_pipeline(match: dict):
    return [
        {"$match": match},
        {"$addFields": {
            "uid_obj": {"$convert": {"input": "$uid", "to": "objectId", "onError": None, "onNull": None}},
            "loc_obj": {"$convert": {"input": "$locationId", "to": "objectId", "onError": None, "onNull": None}},
        }},
        {"$lookup": {"from": USERS_COL, "let": {"u": "$uid_obj"},
                     "pipeline": [{"$match": {"$expr": {"$eq": ["$_id", "$$u"]}}}],
                     "as": "u"}},
        {"$unwind": {"path": "$u", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {"from": LOCATIONS_COL, "let": {"l": "$loc_obj"},
                     "pipeline": [{"$match": {"$expr": {"$eq": ["$_id", "$$l"]}}}],
                     "as": "loc"}},
        {"$unwind": {"path": "$loc", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {
            "lng_from_geojson": {"$cond": [
                {"$eq": [{"$type": "$loc.coordinates.coordinates"}, "array"]},
                {"$arrayElemAt": ["$loc.coordinates.coordinates", 0]},
                None
            ]},
            "lat_from_geojson": {"$cond": [
                {"$eq": [{"$type": "$loc.coordinates.coordinates"}, "array"]},
                {"$arrayElemAt": ["$loc.coordinates.coordinates", 1]},
                None
            ]},
        }},
        {"$project": {
            "_id": 0,
            "journal_id": {"$toString": "$_id"},

            "Timestamp": "$start_time",
            "End Date Time": "$end_time",
            "n_Duration": {
                "$round": [
                    {"$divide": [{"$subtract": ["$end_time", "$start_time"]}, 60000]},
                    2
                ]
            },

            "User Name": {"$ifNull": ["$u.name", ""]},
            "User email": {"$ifNull": ["$u.email", ""]},

            "n_Name": {"$ifNull": ["$loc.name", ""]},
            "City": {"$ifNull": ["$loc.city", ""]},
            "State": {"$ifNull": ["$loc.stateInitials", {"$ifNull": ["$loc.state", ""]}]} ,
            "Zip": {"$ifNull": ["$loc.zip", ""]},

            "LocCountry": {"$ifNull": ["$loc.country", ""]},
            "Address": {"$ifNull": ["$loc.address", ""]},

            "n_Place": {"$concat": [
                {"$ifNull": ["$loc.name", ""]}, ", ",
                {"$ifNull": ["$loc.city", ""]}, " ",
                {"$ifNull": ["$loc.stateInitials", {"$ifNull": ["$loc.state", ""]}]}
            ]},

            "n_Lati": {"$ifNull": ["$loc.coordinates.lat",
                       {"$ifNull": ["$loc.coordinates.latitude", "$lat_from_geojson"]}]},
            "n_Long": {"$ifNull": ["$loc.coordinates.lng",
                       {"$ifNull": ["$loc.coordinates.longitude", "$lng_from_geojson"]}]},

            "n_park_nbr": {"$ifNull": ["$loc.parkNumber", {"$arrayElemAt": ["$loc.category", 0]}]},
            
            "n_activity": {"$ifNull": ["$activity", ""]},
            "n_notes": {"$ifNull": ["$notes", ""]}
        }},
        {"$sort": {"journal_id": 1}}
    ]

def _to_str_timestamp(x):
    if x is None:
        return ""
    try:
        return x.isoformat()
    except Exception:
        return str(x)

def clean(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=FINAL_COLS)
    df = df.copy()

    addr_src  = df.get("Address", pd.Series([""]*len(df), index=df.index)).astype(str)
    place_src = df.get("n_Place", pd.Series([""]*len(df), index=df.index)).astype(str)
    address_for_check = addr_src.where(addr_src.str.len() > 0, place_src)

    state_series       = df.get("State", pd.Series([""]*len(df), index=df.index)).astype(str)
    loc_country_series = df.get("LocCountry", pd.Series([""]*len(df), index=df.index)).astype(str)
    df["Country"] = [
        decide_country(addr, st, lc)
        for addr, st, lc in zip(address_for_check, state_series, loc_country_series)
    ]

    df["n_Lati"]  = pd.to_numeric(df.get("n_Lati"), errors="coerce").round(6)
    df["n_Long"]  = pd.to_numeric(df.get("n_Long"), errors="coerce").round(6)
    df["n_Place"] = place_src.str.replace(r"\s{2,}", " ", regex=True).str.strip(" ,")

    df["Timestamp"]     = df["Timestamp"].apply(_to_str_timestamp)
    df["End Date Time"] = df["End Date Time"].apply(_to_str_timestamp)
    
    # Ensure Status column exists and is empty if not present
    if "Status" not in df.columns:
        df["Status"] = ""

    for c in FINAL_COLS:
        if c not in df.columns:
            df[c] = ""
            
    # Keep journal_id for deduplication but do NOT return it in final columns if not requested
    # The user requested journal_id be removed from output.
    # We will return FINAL_COLS.
    # Note: We need to handle deduplication BEFORE dropping journal_id if we have multiple chunks.
    # But here 'df' is the cleaned chunk.
    return df

# --- Watermark Logic ---

def get_watermark(drive, folder_id: str, excel_name: str) -> Optional[str]:
    """
    Tries to read watermark.json. If missing, tries to migrate from existing Excel.
    """
    # 1. Try JSON
    fid = find_file_id(drive, WATERMARK_FILE, folder_id)
    if fid:
        try:
            buf = io.BytesIO()
            drive.files().get_media(fileId=fid).download(buf)
            buf.seek(0)
            data = json.load(buf)
            return data.get("last_oid")
        except Exception as e:
            log.warning("Failed to read watermark.json: %s", e)
    
    # 2. Migration: Try Excel
    log.info("ℹ️ watermark.json not found. Attempting migration from %s...", excel_name)
    try:
        existing = download_excel(drive, excel_name, folder_id)
        if not existing.empty and "journal_id" in existing.columns:
            oids = []
            for s in existing["journal_id"].astype(str):
                try:
                    oids.append(ObjectId(s))
                except Exception:
                    continue
            if oids:
                last_oid = str(max(oids))
                log.info("✅ Migrated watermark from Excel: %s", last_oid)
                # Save it immediately so we don't scan Excel next time
                save_watermark(drive, folder_id, last_oid)
                return last_oid
    except Exception as e:
        log.warning("Migration failed: %s", e)
    
    return None

def save_watermark(drive, folder_id: str, last_oid: str):
    if not last_oid:
        return
    
    content = json.dumps({"last_oid": last_oid})
    fid = find_file_id(drive, WATERMARK_FILE, folder_id)
    
    media = MediaFileUpload(io.BytesIO(content.encode("utf-8")), mimetype="application/json", resumable=True)
    
    if fid:
        drive.files().update(fileId=fid, media_body=media).execute()
    else:
        drive.files().create(body={"name": WATERMARK_FILE, "parents": [folder_id]}, media_body=media).execute()
    log.info("💾 Saved watermark: %s", last_oid)

def fetch(db, last_oid: Optional[str]) -> Tuple[pd.DataFrame, Optional[str]]:
    match = {"end_time": {"$ne": None}}
    if last_oid:
        try:
            match["_id"] = {"$gt": ObjectId(last_oid)}
        except Exception:
            log.warning("Invalid last_oid; running full fetch.")
    docs = list(db[JOURNALS_COL].aggregate(agg_pipeline(match)))
    
    if not docs:
        return pd.DataFrame(), last_oid

    # Find the max ID in this batch to update watermark
    # (Assuming sorted by journal_id in pipeline)
    new_last_oid = docs[-1]["journal_id"]
    return pd.DataFrame(docs), new_last_oid

def run_once(cfg: Dict = None):
    """
    Runs one end-to-end pass using cfg (dict) or env vars.
    Required keys/envs: MONGO_URI, DRIVE_FOLDER_ID, SA_JSON_PATH or DRIVE_SA_JSON
    Optional: OUTPUT_NAME (default NC-DA-Journal-Data.xlsx), RUN_MODE (full|inc)
    """
    cfg = cfg or {}
    mongo_uri       = _require(cfg, "MONGO_URI")
    drive_folder_id = _require(cfg, "DRIVE_FOLDER_ID")
    output_name     = cfg.get("OUTPUT_NAME") or os.getenv("OUTPUT_NAME", "NC-DA-Journal-Data.xlsx")
    run_mode        = (cfg.get("RUN_MODE") or os.getenv("RUN_MODE", "inc")).lower()

    sa_path = _ensure_sa_file(cfg)
    drive, sa_email = _drive_client(sa_path)

    # Connectivity checks
    try:
        client = MongoClient(mongo_uri, tz_aware=True)
        client.admin.command("ping")
    except Exception as e:
        raise SystemExit(f"Mongo connection failed. Check MONGO_URI. Details: {e}")

    try:
        drive.files().get(fileId=drive_folder_id, fields="id").execute()
    except HttpError as e:
        raise SystemExit(f"Drive folder not accessible. Share {drive_folder_id} with {sa_email} (Editor). Details: {e}")

    db = client[DB_NAME]
    
    # Determine start point
    last_oid = None
    if run_mode == "inc":
        last_oid = get_watermark(drive, drive_folder_id, output_name)

    raw, new_watermark = fetch(db, last_oid)
    
    if raw is None or raw.empty:
        log.info("ℹ️ No new data; nothing to upload.")
        return

    cleaned = clean(raw)
    
    # Download existing to append
    existing = download_excel(drive, output_name, drive_folder_id)
    
    # If existing has journal_id (old format), we might want to drop it or keep it?
    # The user wants the NEW format. So we should probably just align columns.
    # If existing is empty or columns mismatch, pandas concat handles it (filling NaN).
    # We only keep FINAL_COLS.
    
    if not existing.empty:
        # Ensure existing has all final cols
        for c in FINAL_COLS:
            if c not in existing.columns:
                existing[c] = ""
        existing = existing[FINAL_COLS]
    
    out = pd.concat([existing, cleaned[FINAL_COLS]], ignore_index=True) if not existing.empty else cleaned[FINAL_COLS]
    
    # Save locally then upload
    tmp_path = "NC-out.xlsx"
    out.to_excel(tmp_path, index=False)
    upload_excel(drive, tmp_path, output_name, drive_folder_id)
    log.info("✅ Uploaded %s (%d rows)", output_name, len(out))
    
    # Update watermark ONLY after successful upload
    if run_mode == "inc" and new_watermark:
        save_watermark(drive, drive_folder_id, new_watermark)

if __name__ == "__main__":
    # Fallback to env-only run
    cfg_env = {
        "MONGO_URI":       os.getenv("MONGO_URI"),
        "DRIVE_FOLDER_ID": os.getenv("DRIVE_FOLDER_ID"),
        "OUTPUT_NAME":     os.getenv("OUTPUT_NAME", "NC-DA-Journal-Data.xlsx"),
        "RUN_MODE":        os.getenv("RUN_MODE", "inc"),
        "SA_JSON_PATH":    os.getenv("SA_JSON_PATH", "drive-sa.json"),
        "DRIVE_SA_JSON":   os.getenv("DRIVE_SA_JSON", ""),
    }
    run_once(cfg_env)
