# TRON Multi-Strategy Master Engine
# Version 7.6 (Google Sheets Storage Engine Fully Fixed & Re-architected)
# Strategy 1: Primary Direct Engine (-12 Mins Ago :27s) | WinRate: 53.99%
# Strategy 2: Secondary Direct Engine (-8 Mins Ago :42s) | WinRate: 53.35%

from threading import Thread
from flask import Flask
import sys
import requests
from datetime import datetime, timedelta
import time
import os
import csv
import re
import pytz
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Timezone Configuration ---
MYANMAR_TZ = pytz.timezone('Asia/Yangon')
UTC_TZ = pytz.UTC

def get_myanmar_time():
    return datetime.now(UTC_TZ).astimezone(MYANMAR_TZ)

def get_utc_time():
    return datetime.now(UTC_TZ)

# --- Configuration Constants ---
API_URL = "https://apilist.tronscanapi.com/api/block?sort=-number&limit=200" 
GOOGLE_SHEET_NAME = "trx_fetch"  
WORKSHEET_NAME = "lot_v7_logs"       
CREDENTIALS_FILE = "credentials.json" 
LOG_FILENAME = "lot_v7_matrix_log.csv"

POLL_INTERVAL_SECONDS = 1
GROUP_BOUNDARY_SECOND = 57  
RESULT_CHECK_SECOND = 54 

class MultiStrategyAnalyzer:
    def __init__(self):
        self.completed_group_blocks = []
        self.in_progress_blocks = []
        self.last_processed_block_height = 0
        self.current_period_key = None
        self.predictions_made_for_period = False
        self.display_completed_group_now = False 
        
        # --- Time-Based Matrix Accumulator ---
        self.lag_time_memory = {}
        
        # Streak Trackers for Dual Engine
        self.streaks = {
            "primary_12m": {"type": None, "count": 0},
            "secondary_8m": {"type": None, "count": 0}
        }
        
        # Prediction & Trigger Details States
        self.current_predictions = {"primary_12m": "Skip", "secondary_8m": "Skip"}
        self.trigger_details = {"primary_12m": "Skip", "secondary_8m": "Skip"}
        
        # UI Cache
        self.last_completed_output = ""

        # Telegram Configuration
        self.bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.channel_id = os.environ.get("TELEGRAM_CHANNEL_ID")
        
        # Local Backup CSV Initialization
        self.init_csv_file()
        
        # Connect Google Sheets Cloud Database
        self.sheet = self.connect_google_sheets()

    def send_telegram_message(self, text):
        if not self.bot_token or not self.channel_id:
            return  
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                "chat_id": self.channel_id,
                "text": text,
                "parse_mode": "Markdown"
            }
            requests.post(url, json=payload, timeout=5)
        except Exception as e:
            print(f"[❌] Telegram Alert Error: {e}")

    def connect_google_sheets(self):
        """ Google Sheets API Connection Core Engine """
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds_json = os.environ.get("GOOGLE_CREDENTIALS")
            
            if not creds_json:
                print("[Local Mode] Detecting credentials.json file...")
                if not os.path.exists(CREDENTIALS_FILE):
                    print(f"[⚠️] Critical: {CREDENTIALS_FILE} missing locally!")
                    return None
                creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
            else:
                print("[Cloud Mode] Activating Service Account via Environment Variable...")
                cleaned_json = creds_json.strip()
                fixed_json = cleaned_json.replace('\n', '\\n').replace('\\\\n', '\\n')
                info = json.loads(fixed_json, strict=False)
                creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
                
            client = gspread.authorize(creds)
            spreadsheet = client.open(GOOGLE_SHEET_NAME)
            
            try:
                sheet_instance = spreadsheet.worksheet(WORKSHEET_NAME)
            except gspread.exceptions.WorksheetNotFound:
                print(f"[🔄] Tab '{WORKSHEET_NAME}' not found. Spawning dynamic worksheet tab...")
                sheet_instance = spreadsheet.add_worksheet(title=str(WORKSHEET_NAME), rows="10000", cols="15")
            
            # Ensure Header Setup exists
            try:
                headers = sheet_instance.get_all_values()
                if not headers or len(headers) == 0:
                    sheet_instance.append_row([
                        "date", "block_time", "block_id", "result_id",
                        "pred_primary_12m", "pred_secondary_8m", "result",
                        "out_primary_12m", "out_secondary_8m"
                    ])
            except Exception:
                pass
                
            print(f"[✅] Connected to Spreadsheet: '{GOOGLE_SHEET_NAME}' -> Tab: '{WORKSHEET_NAME}'")
            return sheet_instance
        except Exception as e:
            print(f"[❌] Google Sheets Cloud Authentication Failed: {e}")
            return None

    def init_csv_file(self):
        if not os.path.exists(LOG_FILENAME):
            with open(LOG_FILENAME, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "date", "block_time", "block_id", "result_id",
                    "pred_primary_12m", "pred_secondary_8m", "result",
                    "out_primary_12m", "out_secondary_8m"
                ])

    def _generate_prediction_id(self):
        utc_now = get_utc_time()
        date_part = utc_now.strftime("%Y%m%d")
        series_part = "01"
        current_minute = utc_now.hour * 60 + utc_now.minute
        sequence = current_minute + 1
        return f"{date_part}{series_part}{sequence:04d}"

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
                self.display_completed_group_now = True

    def initial_cold_warmup(self):
        print("[🔄] Running cold startup memory pre-load...")
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(API_URL, headers=headers, timeout=10).json()
            blocks = response.get('data', [])

            for block in blocks:
                bt = datetime.fromtimestamp(block['timestamp'] / 1000, tz=MYANMAR_TZ)
                m_key = bt.strftime("%Y-%m-%d %H:%M")
                sec = bt.second
                
                if m_key not in self.lag_time_memory:
                    self.lag_time_memory[m_key] = {}
                self.lag_time_memory[m_key][sec] = self.get_digit_master_group(block.get('hash', ''))
            print(f"[✅] Cold Warmup Complete. Map Points Logged: {len(self.lag_time_memory)} Minutes cached.")
        except Exception as e:
            print(f"[⚠️] Warmup warning: {e}")

    def make_prediction_if_needed(self, block):
        b_time = datetime.fromtimestamp(block['timestamp'] / 1000, tz=MYANMAR_TZ)
        hash_val = block['hash']
        current_m_key = b_time.strftime("%Y-%m-%d %H:%M")
        sec = b_time.second
        
        if current_m_key not in self.lag_time_memory:
            self.lag_time_memory[current_m_key] = {}
        self.lag_time_memory[current_m_key][sec] = self.get_digit_master_group(hash_val)

        prediction_triggered = False

        # --- Strategy 1: Primary Direct Engine (-12 Mins Ago :27s) ---
        if sec == 27:
            target_time_obj = b_time - timedelta(minutes=12)
            target_lag_m_key = target_time_obj.strftime("%Y-%m-%d %H:%M")
            
            if target_lag_m_key in self.lag_time_memory and 27 in self.lag_time_memory[target_lag_m_key]:
                historical_signal = self.lag_time_memory[target_lag_m_key][27]
                if historical_signal:
                    self.current_predictions["primary_12m"] = historical_signal
                    self.trigger_details["primary_12m"] = f"-12m ({target_time_obj.strftime('%H:%M')}) :27s Match -> {historical_signal}"
                    prediction_triggered = True
            else:
                self.current_predictions["primary_12m"] = "Skip"
                self.trigger_details["primary_12m"] = "Skip (Syncing 12m Memory...)"

        # --- Strategy 2: Secondary Direct Engine (-8 Mins Ago :42s) ---
        if sec == 42:
            target_time_obj = b_time - timedelta(minutes=8)
            target_lag_m_key = target_time_obj.strftime("%Y-%m-%d %H:%M")
            
            if target_lag_m_key in self.lag_time_memory and 42 in self.lag_time_memory[target_lag_m_key]:
                historical_signal = self.lag_time_memory[target_lag_m_key][42]
                if historical_signal:
                    self.current_predictions["secondary_8m"] = historical_signal
                    self.trigger_details["secondary_8m"] = f"-8m ({target_time_obj.strftime('%H:%M')}) :42s Match -> {historical_signal}"
                    prediction_triggered = True
            else:
                self.current_predictions["secondary_8m"] = "Skip"
                self.trigger_details["secondary_8m"] = "Skip (Syncing 8m Memory...)"

        # --- TELEGRAM SIGNAL ALERT ---
        if prediction_triggered:
            target_period = self._generate_prediction_id()
            tg_pred_msg = (
                f"🔮 *[v7.6 LIVE SIGNAL]* 🔮\n"
                f"🆔 *Prediction ID:* `{target_period}`\n"
                f"⏱️ *Trigger Time:* {b_time.strftime('%H:%M:%S')}\n"
                f"──────────────────\n"
                f"👑 *Primary (-12m :27s):* `{self.current_predictions['primary_12m']}`\n"
                f"🥈 *Secondary (-8m :42s):* `{self.current_predictions['secondary_8m']}`"
            )
            self.send_telegram_message(tg_pred_msg)

        # --- Process Log & Cloud Dump at Exactly :54 ---
        if sec == RESULT_CHECK_SECOND and not self.predictions_made_for_period:
            target_period = self._generate_prediction_id()
            self.verify_and_log_results(target_period, block, b_time.strftime("%Y-%m-%d"), b_time.strftime("%H:%M:%S"))
            self.predictions_made_for_period = True
            self.display_completed_group_now = True 

            # Memory Garbage Collection
            if len(self.lag_time_memory) > 200:
                sorted_keys = sorted(self.lag_time_memory.keys())
                for old_k in sorted_keys[:50]:
                    self.lag_time_memory.pop(old_k, None)

    def update_streak(self, strat, outcome_type):
        if self.streaks[strat]["type"] == outcome_type:
            self.streaks[strat]["count"] += 1
        else:
            self.streaks[strat]["type"] = outcome_type
            self.streaks[strat]["count"] = 1
        return f"{'✅' if outcome_type == 'Win' else '❌'} {outcome_type}! (Streak: {self.streaks[strat]['count']})"

    def verify_and_log_results(self, target_period, current_block, mm_date, mm_time):
        actual_res = self.get_digit_master_group(current_block['hash'])
        result_block_id = str(current_block['Block height'])
        
        outcomes = {}
        for k in ["primary_12m", "secondary_8m"]:
            pred = self.current_predictions[k]
            if pred == "Skip": outcomes[k] = "Skipped"
            else: outcomes[k] = "Win" if pred == actual_res else "Loss"

        disp_primary = "Skipped" if outcomes["primary_12m"] == "Skipped" else self.update_streak("primary_12m", outcomes["primary_12m"])
        disp_secondary = "Skipped" if outcomes["secondary_8m"] == "Skipped" else self.update_streak("secondary_8m", outcomes["secondary_8m"])

        # UI Terminal Output Construction
        sb = []
        sb.append(f"--- Last Completed Group ---")
        sb.append(f"ID: {target_period} | Block: {result_block_id}")
        sb.append(f"Logic : Prediction => Final Result | Outcome")
        sb.append(f"Primary Direct Engine (-12m :27s)  : {self.current_predictions['primary_12m']} => {actual_res} | {disp_primary}")
        sb.append(f"Secondary Direct Engine (-8m :42s)  : {self.current_predictions['secondary_8m']} => {actual_res} | {disp_secondary}")
        sb.append(f"--------------------------")
        self.last_completed_output = "\n".join(sb)

        # --- TELEGRAM MATCH RESULT ALERT ---
        tg_res_msg = (
            f"🏁 *[v7.6 MATCH RESULT]* 🏁\n"
            f"🆔 *ID:* `{target_period}` | 📦 *Block:* `{result_block_id}`\n"
            f"🎯 *Target Result:* `{actual_res}`\n"
            f"──────────────────\n"
            f"👑 *Primary (-12m):* {self.current_predictions['primary_12m']} ➔ {disp_primary}\n"
            f"🔹 *Secondary (-8m):* {self.current_predictions['secondary_8m']} ➔ {disp_secondary}\n"
            f"──────────────────\n"
            f"📊 *Time Logged:* {mm_date} {mm_time}"
        )
        self.send_telegram_message(tg_res_msg)

        # --- Local Backup CSV Write ---
        try:
            with open(LOG_FILENAME, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    mm_date, mm_time, result_block_id, target_period,
                    self.current_predictions['primary_12m'], self.current_predictions['secondary_8m'],
                    actual_res, outcomes['primary_12m'], outcomes['secondary_8m']
                ])
        except Exception as e:
            print(f"[❌] CSV Write Backup Error: {e}")

        # --- GOOGLE SHEETS DUMP ROUTINE (FIXED & RE-ARCHITECTED) ---
        row_data = [
            str(mm_date), str(mm_time), str(result_block_id), str(target_period),
            str(self.current_predictions['primary_12m']), str(self.current_predictions['secondary_8m']),
            str(actual_res), str(outcomes['primary_12m']), str(outcomes['secondary_8m'])
        ]

        # BUG FIX: Loop explicitly checks self.sheet scope with proper User Entered Token Option
        for attempt in range(2):
            if self.sheet is None:
                print("[🔄] Reconnecting Google Sheets...")
                self.sheet = self.connect_google_sheets()
                
            if self.sheet is not None:
                try:
                    # Robust cloud row appending strategy using append_rows block
                    self.sheet.append_rows([row_data], value_input_option='USER_ENTERED')
                    print(f"🚀 [Cloud Logged] Data row saved in Google Sheet '{WORKSHEET_NAME}' tab successfully!")
                    break
                except Exception as sheet_err:
                    print(f"[❌] Sheet append failed on attempt {attempt+1}: {sheet_err}")
                    self.sheet = None  # Force reconnection protocol on next attempt
            else:
                print(f"[⚠️] Google Sheet Instance is missing on attempt {attempt+1}. Retrying...")

    def print_status(self):
        now_str = get_myanmar_time().strftime("%Y-%m-%d %H:%M:%S")
        print(f"--- TRON Dual-Strategy Master Engine v7.6 | Last Update: {now_str} ---\n")
        
        if self.display_completed_group_now and self.last_completed_output:
            print(self.last_completed_output)
            print()

        print("------------------------------")
        print("---- In Progress ----")
        
        for block in self.in_progress_blocks:
            b_time = datetime.fromtimestamp(block['timestamp'] / 1000, tz=MYANMAR_TZ)
            sec = b_time.second
            print(f"Block: {block['Block height']} | Time: {sec:02d}s | Hash: ...{block['hash'][-8:]}")

        target_period = self._generate_prediction_id()
        print("...")
        print(f"Prediction for ID: {target_period}")
        print(f" Primary Direct Engine (-12m :27s)  : {self.trigger_details['primary_12m']}")
        print(f" Secondary Direct Engine (-8m :42s)  : {self.trigger_details['secondary_8m']}")
        print("--------------------------") 

    def run(self):
        self.initial_cold_warmup()
        
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
                print("\nShutdown Matrix v7.6 Complete.")
                sys.exit(0)
            except Exception as e:
                print(f"Loop Runtime Error (v7.6): {e}")
                time.sleep(5)

# --- Flask Web Server For Render ---
app = Flask('')

@app.route('/')
def home():
    return "TRON Dual-Strategy Master Engine v7.6 is Running Live on Render!"

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