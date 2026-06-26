

# 📋 TRON BOT Engine (Automated Cloud Save Engine )- Detailed Project Summary

### 1. 24/7 Cloud Platform Selection & Flask Web Server Implementation

* **Action:** Selected **Render.com (Web Service)** as the core hosting platform to run the Python scripts 24/7 continuously for free, even when the local computer is powered off.
* **Flask Web Server Integration Code:** Since Render's Web Service deployment requires an open and active port to keep the service running, a lightweight Flask server was running in the background using Python's `threading` module as follows:

```python
from threading import Thread
from flask import Flask
import os

# 1. Initialize the Flask Web Server
app = Flask('')

@app.route('/')
def home():
    return "TRON Engine is Running Live!"

# 2. Bind and run the server on the port specified by Render
def run_web():
    # Fetch the environment port allocated by Render, default to 8080 if not found
    port = int(os.environ.get("PORT", 8080))
    # use_reloader=False prevents the script from executing double loops accidentally
    app.run(host='0.0.0.0', port=port, use_reloader=False)

# 3. Background thread to keep the web server alive while the main logic runs
def keep_alive():
    t = Thread(target=run_web, daemon=True) # daemon=True ensures it shuts down cleanly with the main process
    t.start()

if __name__ == "__main__":
    keep_alive() # Initialize and kick-off the background Flask server
    
    # ---------------------------------------------
    # Your primary core logic / main loops go here
    # ---------------------------------------------

```

---

### 2. Pushing Project Files to GitHub

* **Action:** Initialized a local Git repository and pushed all vital project files (`trx_fetch.py`, `lot_v5.py`, and `requirements.txt`) to a remote GitHub repository to achieve clean version control.

---

### 3. CI/CD Integration Between Render and GitHub

* **Action:** Linked the GitHub account to the Render Dashboard and selected the project repository.
* **Result:** Successfully configured an automated Continuous Integration/Continuous Deployment (CI/CD) pipeline, allowing Render to auto-deploy and restart the engine whenever a code update is pushed (`git push`) to GitHub.

---

### 4. Google Cloud Console API Setup (Step-by-Step Guide)

To enable the backend scripts to write directly to Google Sheets, a dedicated API setup was established through the following phases:

* **Step A: Creating a New Project:**
1. Navigated to the [Google Cloud Console](https://console.cloud.google.com/) and logged into the active account.
2. Clicked on **Select a project > New Project**, provided a descriptive project name, and initialized the creation.


* **Step B: Enabling the Google Drive and Sheets APIs:**
1. Searched for **"Google Sheets API"** in the Cloud Console search bar and clicked **Enable**.
2. Repeated the process by searching for **"Google Drive API"** and clicked **Enable** to turn it on.


* **Step C: Creating a Service Account & Extracting the Credentials Key:**
1. Navigated to the left sidebar menu and went to **IAM & Admin > Service Accounts**.
2. Clicked **Create Service Account**, provided a name, and proceeded by clicking **Create and Continue**.
3. Copied the newly generated Service Account Email address (e.g., `my-bot@project.iam.gserviceaccount.com`).
4. Clicked into the newly created Service Account and navigated to the **Keys** tab.
5. Clicked **Add Key > Create new key**, selected the **JSON** format option, and clicked Create. A `.json` credentials file was downloaded locally, which was subsequently renamed to `credentials.json`.


* **Step D: Authorizing the Google Sheet (Sharing Access):**
1. Opened the target Google Sheet.
2. Clicked the **Share** button in the top-right corner, pasted the copied Service Account Email, assigned **Editor Permission**, and confirmed the share.



---

### 5. Optimizing Python Code for Secure Integration

* **Action:** Since the `credentials.json` file contains highly sensitive private authentication tokens, a local **`.gitignore`** file was created and configured with `credentials.json` listed inside it. This completely restricted the secret credentials file from being uploaded to public GitHub repositories.

---

### 6. Mitigating Platform Bottlenecks & Secret File Configuration on Render

* **Issue:** Initially, storing the `credentials.json` contents directly inside standard Environment Variable blocks caused formatting collapses on private key line breaks. This continuously triggered `Invalid JWT Signature` or JSON syntax-related failures (`Expecting property name...`).
* **Resolution (Injecting Purity using Render's Secret Files):** Migrated from standard text variables to Render’s native **Secret Files** system using these exact steps:

1. Navigated to the Render Dashboard and opened the respective Web Service (`tron-raw-fetcher` / `tron-prediction-engine`).
2. Went to the **Environment** section from the left-hand menu sidebar.
3. Inspected the **Environment Variables** section and completely deleted the faulty `GOOGLE_CREDENTIALS` variable block via the **Delete (Trash icon)** button.
4. Scrolled down slightly to the **Secret Files** module section.
5. Clicked the **Add Secret File** button and filled out the parameters as follows:
* **Filename:** `credentials.json`
* **Contents:** Opened the authentic local `credentials.json` file via Notepad, executed a full copy (`Ctrl + A` -> `Ctrl + C`), and pasted the precise structure cleanly into this box.


6. Confirmed and saved by clicking **Save Changes**.
7. This adjustment successfully allowed the native `from_json_keyfile_name('credentials.json', scope)` code implementation to access the structure flawlessly on both local machines and Render's Cloud framework without any raw string manipulation.

---

### 7. Preventing Web Service Sleep on Render's Free Tier (Keep-Alive Setup)

* **Action:** To bypass the standard behavior of Render's Free Tier web services spinning down into an idle hibernation mode after 50 minutes of inactivity, an external automated cron manager (such as `cron-job.org`) was integrated. This scheduler sends regular HTTP pings every 5 minutes to the root links (e.g., `https://tron-bot-engine.onrender.com/`), keeping the services actively awake 24/7.

---

### 8. Successful Automated Google Sheets Data Persistence

* **Result:** With all pipeline configurations stabilized, both systems are running flawlessly in sync:
* `https://tron-bot-engine.onrender.com/` (TRON Raw Fetcher) successfully retrieves raw TRON block values every 3 seconds and dumps them directly into the first tab of the designated Google Sheet.
* `https://tron-prediction-engine.onrender.com/` (TRON Prediction Engine) executes its matrix analysis and writes calculations precisely on the 54th second mark of every minute, maintaining a clean 1-row-per-minute interval logging schema within the `Lot_V5_Logs` tab.


* **Key Discovery:** Identified that Google Sheets' native `append_row` logic naturally maps data tracking starting immediately from the absolute bottom of the sheet (e.g., pushing past empty rows to cell positions beyond row 5,000), which allows for historical dataset logging.