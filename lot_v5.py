# TRON Multi-Strategy Master Engine
# Version 5.5 (Cloud & Google Sheets Variable Optimized)

from threading import Thread
from flask import Flask
import sys
import requests
from datetime import datetime
import time
import os
import re
import pytz
import traceback
import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials

# --- Timezone Configuration ---
MYANMAR_TZ = pytz.timezone('Asia/Yangon')
UTC_TZ = pytz.UTC

def get_myanmar_time():
    return datetime.now(UTC_TZ).astimezone(MYANMAR_TZ)

def get_utc_time():
    return datetime.now(UTC_TZ)

# --- Configuration Constants ---
API_URL = "https://apilist.tronscanapi.com/api/block?sort=-number&limit=150" 
GOOGLE_SHEET_NAME = "trx_fetch"  
WORKSHEET_NAME = "Lot_V5_Logs"       
CREDENTIALS_FILE = "credentials.json" 

POLL_INTERVAL_SECONDS = 1
GROUP_BOUNDARY_SECOND = 57  
RESULT_CHECK_SECOND = 54 

MIN_SAMPLES_THRESHOLD = 3
WIN_RATE_PLAY_THRESHOLD = 51.0

class MultiStrategyAnalyzer:
    def __init__(self):
        self.completed_group_blocks = []
        self.in_progress_blocks = []
        self.last_processed_block_height = 0
        self.current_period_key = None
        self.predictions_made_for_period = False
        self.display_completed_group_now = False 
        
        # Streak Trackers
        self.streaks = {
            "do": {"type": None, "count": 0}, 
            "hex": {"type": None, "count": 0}, 
            "dm": {"type": None, "count": 0}
        }
        
        # Strategy Internal States
        self.best_secs = {"do": -1, "hex": -1, "dm": -1}
        self.best_rates = {"do": 0.0, "hex": 0.0, "dm": 0.0}
        self.current_predictions = {"do": "Skip", "hex": "Skip", "dm": "Skip"}
        self.trigger_details = {"do": "Skip", "hex": "Skip", "dm": "Skip"}
        
        # Last Completed UI Cache
        self.last_completed_output = ""

        self.sheet = self.connect_google_sheets()

    # --- Google Sheets Tab သီးသန့်ဆောက်ပြီး ချိတ်ဆက်သည့် စနစ် ---
    def connect_google_sheets(self):
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            
            creds_json = os.environ.get("GOOGLE_CREDENTIALS")
            if not creds_json:
                print("Local Mode: Loading credentials.json file...")
                creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
            else:
                print("Cloud Mode: Loading credentials from Environment Variable...")
                # Format ပျက်နေသော \n (New Line) များကို ကုဒ်ထဲမှ အတင်းပြန်လည် ပြုပြင်ပေးခြင်း
                fixed_json = creds_json.replace('\n', '\\n').replace('\\\\n', '\\n')
                info = json.loads(fixed_json, strict=False)
                creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
                
            client = gspread.authorize(creds)
            spreadsheet = client.open(GOOGLE_SHEET_NAME)
            
            try:
                sheet = spreadsheet.worksheet(WORKSHEET_NAME)
            except gspread.exceptions.WorksheetNotFound:
                sheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows="5000", cols="20")
            
            if not sheet.get_all_values():
                sheet.append_row([
                    "date", "block_time", "block_id", "result_id",
                    "bs_do", "bs_hex", "bs_dm",
                    "pred_do", "pred_hex", "pred_dm",
                    "result", "out_do", "out_hex", "out_dm"
                ])
            return sheet
        except Exception as e:
            print(f"Google Sheets Connection Error (lot_v5): {e}")
            return None

    def _generate_prediction_id(self):
        utc_now = get_utc_time()
        date_part = utc_now.strftime("%Y%m%d")
        series_part = "01"
        current_minute = utc_now.hour * 60 + utc_now.minute
        sequence = current_minute + 1
        return f"{date_part}{series_part}{sequence:04d}"

    def get_digit_only_group(self, hash_value):
        if not hash_value: return None
        digits = "".join(re.findall(r'\d', hash_value))
        if not digits: return None
        val = int(digits[-1])
        return "B" if val in [5, 6, 7, 8, 9] else "S"

    def get_hexadecimal_group(self, hash_value):
        if not hash_value: return None
        last_char = hash_value[-1].lower()
        return "S" if last_char in "01234567" else "B"

    def get_digit_master_group(self, hash_value):
        if not hash_value: return None
        digits = "".join(re.findall(r'\d', hash_value))
        if not digits: return None
        val = int(digits[-1])
        return "S" if 0 <= val <= 4 else "B"

    def get_block_data(self, raw_block):
        if not raw_block: return None
        return {
            'Block height': raw_block.get('number'),
            'timestamp': raw_block.get('timestamp'),
            'hash': raw_block.get('hash')
        }

    def check_for_new_group(self, block):
        b_time = datetime.fromtimestamp(block['timestamp'] / 1000, tz=MYANMAR_TZ)
        period_key = block['Block height']
        
        if b_time.second >= GROUP_BOUNDARY_SECOND:
            if self.current_period_key != period_key and b_time.second == GROUP_BOUNDARY_SECOND:
                self.completed_group_blocks = list(self.in_progress_blocks)
                self.in_progress_blocks = []
                self.current_period_key = period_key
                self.predictions_made_for_period = False
                self.current_predictions = {"do": "Skip", "hex": "Skip", "dm": "Skip"}
                self.trigger_details = {"do": "Skip", "hex": "Skip", "dm": "Skip"}
                self.analyze_matrix_correlation()

    def analyze_matrix_correlation(self):
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(API_URL, headers=headers, timeout=10).json()
            blocks = response.get('data', [])

            stats = {strat: {sec: {"matches": 0, "total": 0} for sec in range(0, 54, 3)} for strat in ["do", "hex", "dm"]}
            minutes_data = {}

            for block in blocks:
                bt = datetime.fromtimestamp(block['timestamp'] / 1000, tz=MYANMAR_TZ)
                m_key = bt.strftime("%H:%M")
                if m_key not in minutes_data: minutes_data[m_key] = {}
                
                h = block.get('hash', '')
                minutes_data[m_key][bt.second] = {
                    "do": self.get_digit_only_group(h),
                    "hex": self.get_hexadecimal_group(h),
                    "dm": self.get_digit_master_group(h),
                    "target_actual": self.get_digit_master_group(h) 
                }

            for m_key, secs_dict in minutes_data.items():
                target = secs_dict.get(RESULT_CHECK_SECOND)
                if target and target["target_actual"] is not None:
                    for sec in range(0, 54, 3):
                        if sec in secs_dict:
                            if secs_dict[sec]["do"] is not None:
                                stats["do"][sec]["total"] += 1
                                if secs_dict[sec]["do"] == target["target_actual"]: stats["do"][sec]["matches"] += 1
                            if secs_dict[sec]["hex"] is not None:
                                stats["hex"][sec]["total"] += 1
                                if secs_dict[sec]["hex"] == target["target_actual"]: stats["hex"][sec]["matches"] += 1
                            if secs_dict[sec]["dm"] is not None:
                                stats["dm"][sec]["total"] += 1
                                if secs_dict[sec]["dm"] == target["target_actual"]: stats["dm"][sec]["matches"] += 1

            for strat in ["do", "hex", "dm"]:
                self.best_secs[strat] = -1
                self.best_rates[strat] = 0.0
                for sec in range(0, 54, 3):
                    total = stats[strat][sec]["total"]
                    if total >= MIN_SAMPLES_THRESHOLD:
                        rate = (stats[strat][sec]["matches"] / total) * 100
                        if rate > self.best_rates[strat]:
                            self.best_rates[strat] = rate
                            self.best_secs[strat] = sec
        except Exception:
            pass

    def make_prediction_if_needed(self, block):
        b_time = datetime.fromtimestamp(block['timestamp'] / 1000, tz=MYANMAR_TZ)
        hash_val = block['hash']
        
        if b_time.second == self.best_secs["do"] and self.best_secs["do"] != -1:
            src = self.get_digit_only_group(hash_val)
            if src:
                if self.best_secs["do"] > 42 or self.best_secs["do"] in [18]: 
                    self.current_predictions["do"] = "Skip"
                    self.trigger_details["do"] = f"{self.best_secs['do']} -> Skip"
                else:
                    self.current_predictions["do"] = src
                    self.trigger_details["do"] = f"{self.best_secs['do']} -> {src}"

        if b_time.second == self.best_secs["hex"] and self.best_secs["hex"] != -1:
            src = self.get_hexadecimal_group(hash_val)
            if src:
                if self.best_secs["hex"] > 42:
                    self.current_predictions["hex"] = "Skip"
                    self.trigger_details["hex"] = f"{self.best_secs['hex']} -> Skip"
                else:
                    self.current_predictions["hex"] = src
                    self.trigger_details["hex"] = f"{self.best_secs['hex']} -> {src}"

        if b_time.second == self.best_secs["dm"] and self.best_secs["dm"] != -1:
            src = self.get_digit_master_group(hash_val)
            if src:
                if self.best_secs["dm"] > 42 or self.best_secs["dm"] in [9, 18] or self.best_rates["dm"] < WIN_RATE_PLAY_THRESHOLD:
                    self.current_predictions["dm"] = "Skip"
                    self.trigger_details["dm"] = f"{self.best_secs['dm']} -> Skip"
                elif self.best_secs["dm"] in [3, 12]: 
                    self.current_predictions["dm"] = "B" if src == "S" else "S"
                    self.trigger_details["dm"] = f"{self.best_secs['dm']} -> {self.current_predictions['dm']}"
                else:
                    self.current_predictions["dm"] = src
                    self.trigger_details["dm"] = f"{self.best_secs['dm']} -> {src}"

        if b_time.second == RESULT_CHECK_SECOND and not self.predictions_made_for_period:
            target_period = self._generate_prediction_id()
            self.verify_and_log_results(target_period, block, b_time.strftime("%Y-%m-%d"), b_time.strftime("%H:%M:%S"))
            self.predictions_made_for_period = True
            self.display_completed_group_now = True 

    def update_streak(self, strat, outcome_type):
        if self.streaks[strat]["type"] == outcome_type:
            self.streaks[strat]["count"] += 1
        else:
            self.streaks[strat]["type"] = outcome_type
            self.streaks[strat]["count"] = 1
        return f"{'✅' if outcome_type == 'Win' else '❌'} {outcome_type}! (Streak: {self.streaks[strat]['count']})"

    def verify_and_log_results(self, target_period, current_block, mm_date, mm_time):
        actual_res = self.get_digit_master_group(current_block['hash'])
        result_block_id = current_block['Block height']
        
        outcomes = {}
        for k in ["do", "hex", "dm"]:
            pred = self.current_predictions[k]
            if pred == "Skip": outcomes[k] = "Skipped"
            else: outcomes[k] = "Win" if pred == actual_res else "Loss"

        disp_do = "Skipped" if outcomes["do"] == "Skipped" else self.update_streak("do", outcomes["do"])
        disp_hex = "Skipped" if outcomes["hex"] == "Skipped" else self.update_streak("hex", outcomes["hex"])
        disp_dm = "Skipped" if outcomes["dm"] == "Skipped" else self.update_streak("dm", outcomes["dm"])

        sb = []
        sb.append(f"--- Last Completed Group ---")
        sb.append(f"ID: {target_period} | Block: {result_block_id}")
        sb.append(f"Logic : Prediction => Final Result | Outcome")
        sb.append(f"Digit Only Logic : {self.current_predictions['do']} => {actual_res} | {disp_do}")
        sb.append(f"Hexadecimal Logic : {self.current_predictions['hex']} => {actual_res} | {disp_hex}")
        sb.append(f"Digit Master Engine: {self.current_predictions['dm']} => {actual_res} | {disp_dm}")
        sb.append(f"--------------------------")
        self.last_completed_output = "\n".join(sb)

        if self.sheet is None:
            self.sheet = self.connect_google_sheets()

        if self.sheet:
            try:
                row_data = [
                    mm_date, mm_time, result_block_id, target_period,
                    self.best_secs["do"], self.best_secs["hex"], self.best_secs["dm"],
                    self.current_predictions["do"], self.current_predictions["hex"], self.current_predictions["dm"],
                    actual_res, outcomes["do"], outcomes["hex"], outcomes["dm"]
                ]
                self.sheet.append_row(row_data)
                print(f" [Cloud Logged] Successfully appended to {WORKSHEET_NAME} tab!")
            except Exception as e:
                print(f"Google Sheets Row Appending Error: {e}")

    def print_status(self):
        now_str = get_myanmar_time().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n=================== LIVE DASHBOARD ({now_str}) ===================")
        
        if self.display_completed_group_now and self.last_completed_output:
            print(self.last_completed_output)

        print("\n---- In Progress Blocks ----")
        for block in self.in_progress_blocks:
            b_time = datetime.fromtimestamp(block['timestamp'] / 1000, tz=MYANMAR_TZ)
            sec = b_time.second
            print(f"Block: {block['Block height']} | Time: {sec:02d}s | Hash: ...{block['hash'][-8:]}")

        target_period = self._generate_prediction_id()
        print(f"\nCurrent Target Prediction ID: {target_period}")
        print("==================================================================\n")

    def run(self):
        self.analyze_matrix_correlation()
        
        while True:
            try:
                headers = {"User-Agent": "Mozilla/5.0"}
                response = requests.get(API_URL, headers=headers, timeout=10)
                raw_blocks = response.json().get('data')

                if not raw_blocks:
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                new_raw_blocks = [b for b in raw_blocks if b.get('number', 0) > self.last_processed_block_height]
                if not new_raw_blocks:
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                new_blocks = sorted(
                    [self.get_block_data(b) for b in new_raw_blocks if self.get_block_data(b)],
                    key=lambda x: x['Block height']
                )

                for block in new_blocks:
                    self.check_for_new_group(block)
                    self.in_progress_blocks.append(block)
                    self.make_prediction_if_needed(block)

                if new_blocks:
                    self.last_processed_block_height = max(b['Block height'] for b in new_blocks)
                
                self.print_status()
                time.sleep(POLL_INTERVAL_SECONDS)

            except requests.exceptions.RequestException:
                time.sleep(5)
            except KeyboardInterrupt:
                print("\nShutdown Matrix Complete.")
                sys.exit(0)
            except Exception as e:
                print(f"Loop Error (lot_v5): {e}")
                time.sleep(5)

# --- Flask Web Server For Render (lot_v5) ---
app = Flask('')

@app.route('/')
def home():
    return "TRON Prediction Engine is Running Live!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, use_reloader=False)

def keep_alive():
    t = Thread(target=run_web, daemon=True)
    t.start()

if __name__ == "__main__":
    keep_alive()
    time.sleep(2)
    
    analyzer = MultiStrategyAnalyzer()
    analyzer.run()