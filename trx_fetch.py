#!/usr/bin/env python3
# trx_fetch.py - Fetch and save all TRON block data to Google Sheets
# Optimized for 24/7 Cloud Running (Environment Variable Configured)

import requests
import time
import os
import re
import pytz
import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from threading import Thread
from flask import Flask

# ---------------- Flask Web Server For Render ----------------
app = Flask('')

@app.route('/')
def home():
    return "TRON Raw Fetcher is Running Live!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, use_reloader=False)

def keep_alive():
    t = Thread(target=run_web, daemon=True)
    t.start()

# ---------------- Configuration ----------------
BLOCKS_URL = "https://apilist.tronscanapi.com/api/block?sort=-number&limit=30"
GOOGLE_SHEET_NAME = "trx_fetch"  
CREDENTIALS_FILE = "credentials.json"  

# ---------------- Global State ----------------
MYANMAR_TZ = pytz.timezone('Asia/Yangon')
UTC_TZ = pytz.UTC

last_processed_block_height = 0
written_rows = set() 
MAX_MEMORY_KEYS = 5000 

# ---------------- Google Sheets Connection ----------------
def connect_google_sheets():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        
        creds_json = os.environ.get("GOOGLE_CREDENTIALS")
        
        if not creds_json:
            print("Local Mode: Loading credentials.json file...")
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        else:
            print("Cloud Mode: Loading credentials from Environment Variable...")
            # format ပျက်နေတဲ့ \n (new line) တွေကို code ထဲကနေ အတင်းပြန်ပြုပြင်ပေးခြင်း
            fixed_json = creds_json.replace('\n', '\\n').replace('\\\\n', '\\n')
            info = json.loads(fixed_json, strict=False)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
            
        client = gspread.authorize(creds)
        sheet = client.open(GOOGLE_SHEET_NAME).sheet1
        return sheet
    except Exception as e:
        print(f"Google Sheets Connection Error (trx_fetch): {e}")
        return None

# ---------------- Utilities ----------------
def clean_hex(hex_str):
    if not hex_str: return ''
    s = str(hex_str).strip().lstrip('*').replace('0x', '')
    return re.sub(r'[^0-9a-fA-F]', '', s).lower()

def find_last_number_in_last_8(hash_value):
    h = clean_hex(hash_value)
    last_8 = h[-8:]
    for i in range(7, -1, -1):
        if last_8[i].isdigit():
            num = int(last_8[i])
            return num, 8-i, "S" if num <= 4 else "B"
    return None, None, None

def save_to_google_sheet(sheet, block_data):
    global written_rows
    key = str(block_data.get("Block height", ""))
    
    if key in written_rows: 
        return
        
    if len(written_rows) >= MAX_MEMORY_KEYS:
        written_rows.clear() 
        
    written_rows.add(key)
    
    if sheet is None:
        return
        
    try:
        row_data = [
            block_data.get("Type", ""), 
            block_data.get("Period", ""), 
            block_data.get("Block height", ""), 
            block_data.get("Block time", ""), 
            block_data.get("Hash value", ""), 
            block_data.get("hash_digit", ""), 
            block_data.get("last_5digit", ""), 
            block_data.get("Result_Digit", ""), 
            block_data.get("Result_Char", "")
        ]
        sheet.append_row(row_data)
        print(f" [Saved to Cloud] Type {block_data.get('Type')} | Period {block_data.get('Period')} | Height {key}")
    except Exception as e:
        print(f"Failed to write to Google Sheet: {e}")

def get_block_data(block):
    mmt = datetime.fromtimestamp(block['timestamp']/1000, UTC_TZ).astimezone(MYANMAR_TZ)
    last_digit, _, r_char = find_last_number_in_last_8(block['hash'])
    cleaned = clean_hex(block['hash'])
    digits = ''.join([c for c in cleaned if c.isdigit()])
    l5 = digits[-5:] if len(digits) >= 5 else digits
    
    block_time_str = mmt.strftime("%H:%M:%S")
    seconds = int(mmt.second)
    type_str = f"{seconds:02d}"
    
    return {
        "Type": type_str,
        "Period": mmt.strftime("%Y%m%d%H%M"),
        "Block height": block['number'],
        "Block time": block_time_str,
        "Hash value": f"{block['hash']}",
        "Result_Digit": str(last_digit) if last_digit is not None else "",
        "Result_Char": r_char or "",
        "hash_digit": digits,
        "last_5digit": l5
    }

def main_loop():
    global last_processed_block_height
    
    print("=" * 70)
    print("  TRON DATA FETCHER - Cloud Live Version")
    print("=" * 70)
    
    keep_alive()
    time.sleep(2)
    
    sheet = connect_google_sheets()
    
    while True:
        try:
            if sheet is None:
                print("Attempting to reconnect to Google Sheets...")
                sheet = connect_google_sheets()
                
            resp = requests.get(BLOCKS_URL, timeout=10).json()
            new_blocks = [get_block_data(b) for b in resp.get('data', []) if b['number'] > last_processed_block_height]
            new_blocks.sort(key=lambda x: x['Block height'])
            
            for bd in new_blocks:
                type_val = int(bd['Type'])
                if 0 <= type_val <= 57:
                    save_to_google_sheet(sheet, bd)
                
                last_processed_block_height = bd['Block height']
            
            time.sleep(3)
            
        except Exception as e:
            print(f"Loop Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main_loop()