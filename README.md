# Appointment Queue & Live Telegram Broadcast System (Safe Architecture)

This project integrates customer intake and queue management with an **autonomous live appointment monitor** that automatically scans the Spanish Immigration Portal (`icp.administracionelectronica.gob.es`) and **broadcasts real-time alerts to Telegram channels, groups, and subscribers** the moment appointment slots open.

---

## 🏛️ System Architecture

```
                    ┌────────────────────────────────────────────────────────┐
                    │       Spanish Extranjería Portal (ICP Plus)           │
                    └───────────────────────────┬────────────────────────────┘
                                                │
                                    (Live Availability Probe)
                                                ▼
┌──────────────────────────────┐    ┌────────────────────────────────────────┐
│     Admin Control Board      │◄───┤        🛰️ Live Slot Monitor            │
│   (http://127.0.0.1:8000/admin)   │    │          (`monitor.py`)                │
└──────────────┬───────────────┘    └───────────────────┬────────────────────┘
               │                                        │
     (Manual / Auto Launch)                 (🚨 Slots Open Detected!)
               ▼                                        ▼
┌──────────────────────────────┐    ┌────────────────────────────────────────┐
│   🚀 Assisted Form-Filler    │    │    📢 Telegram Broadcast Engine        │
│      (`assistant.py`)        │    │    - Public/Private Channels (@canal)  │
│ (Human solves CAPTCHA & SMS) │    │    - Group Chats & User Subscribers    │
└──────────────────────────────┘    └────────────────────────────────────────┘
```

---

## 🌟 Key Components

### 1. 📡 Live Slot Monitor & Broadcaster (`monitor.py` & `telegram_broadcaster.py`)
- Automatically monitors multiple provinces (e.g. Barcelona, Madrid, Valencia) and procedures (e.g. `POLICIA-TOMA DE HUELLA`, `ASILO`, etc.).
- Compares portal responses in real time.
- **Urgent Broadcast:** The moment open dates or offices are detected, it dispatches a high-priority alert to your Telegram channel.
- **Anti-Spam Cooldown:** Suppresses duplicate notifications for 10 minutes so channels aren't spammed while slots stay open.
- **Proxy Support:** Supports HTTP/HTTPS proxies in `config.json` if running outside Spain.

### 2. 📢 Telegram Channel Broadcast Integration
Broadcasts to unlimited users simultaneously via Telegram Channels or Groups.
- Supports channel usernames (e.g. `@mis_citas_barcelona`), group chat IDs (`-100...`), or comma-separated lists of targets.
- Includes a **"Send Test Broadcast"** button on the Admin Dashboard to test connectivity in 1 click.

### 3. 📦 Queue Server & Control Board (`queue_server.py`)
- **Customer Intake (`/`)**: Web intake with duplicate NIE/Passport blocking.
- **Admin Control Board (`/admin`)**:
  - **Orders Queue Tab**: Status tracking, GDPR auto-scrubbing, and 1-click launch of the browser form-filler.
  - **Live Monitor Tab**: Real-time scanner daemon control (Start/Stop), monitor target management, Telegram settings, and test broadcast tools.

### 4. 🚀 Assisted Browser Runner (`assistant.py`)
- Headed browser runner that navigates and fills applicant details automatically.
- Complies with safety rules: Pauses for human operator to solve visual CAPTCHAs and enter the SMS verification code.

---

## 🚀 How to Set Up & Run

### Step 1: Configure Telegram Bot & Broadcast Channel

1. Open Telegram and search for `@BotFather`.
2. Create a bot using `/newbot` and copy the **Bot Token** (e.g. `789123456:AAH...`).
3. Create a public or private **Telegram Channel** (or Group) where you want alerts broadcasted.
4. Add your bot as an **Administrator** in your channel with **"Post Messages"** permission.
5. In [http://127.0.0.1:8000/admin](http://127.0.0.1:8000/admin) under **Telegram Settings**, paste:
   - **Bot Token**
   - **Channel / Chat ID** (e.g. `@your_channel_name` or `-1001234567890`)
   - Check **"Enable Telegram Alerts"** and click **"Save Settings"**.
6. Click **"📢 Send Test Broadcast"** to verify that the message appears in your channel!

---

### Step 2: Start the Web Dashboard

```bash
cd /Users/hassan/Devsynx-projects/Bot
./start_queue.sh
```

- **Customer Intake Form:** [http://127.0.0.1:8000](http://127.0.0.1:8000)
- **Admin Control Board:** [http://127.0.0.1:8000/admin](http://127.0.0.1:8000/admin)
- **API Docs (Swagger):** [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

---

### Step 3: Run the Live Monitor Daemon

You can start the monitor directly from the Admin Dashboard by clicking **"▶️ Start Live Monitor"**, or via command line:

```bash
cd /Users/hassan/Devsynx-projects/Bot
./start_monitor.sh
```

#### Handy CLI Commands for `monitor.py`:
```bash
# Run a single scan across all active monitors:
./.venv/bin/python monitor.py --check-once

# Send a test broadcast to your Telegram channel:
./.venv/bin/python monitor.py --test-broadcast

# Add a new province and procedure to monitor:
./.venv/bin/python monitor.py --add "Madrid" "POLICIA-TOMA DE HUELLA"

# List all active targets:
./.venv/bin/python monitor.py
```


---

## ⚡ Automated Auto-Claiming & Human OTP Handoff

```
[ Monitor detects Open Slots ]
              │
              ▼
[ Auto-Booker pulls next waiting client from orders.db ]
              │
              ▼
[ Fills Province, Procedure, NIE, Name, Phone & Email automatically ]
              │
              ▼
[ Auto-selects first available appointment slot ]
              │
              ▼
[ Government sends SMS OTP to client's phone ]
              │
              ├──────────────────────────────────────────────┐
              ▼                                              ▼
[ Urgent Telegram Alert sent to Operator ]        [ Admin Board Highlights Row ]
"📲 SMS OTP SENT to +34 600... for John Doe"      Inline SMS input box appears
              │                                              │
              └──────────────────────┬───────────────────────┘
                                     ▼
                  [ Operator enters SMS Code ]
                                     ▼
                [ Auto-booker submits verification ]
                                     ▼
        [ Receipt screenshot saved in confirmations/ ]
                                     ▼
                     [ Status -> "booked" ✅ ]
```

### Running the Auto-Booker Manually:
```bash
# Auto-book a specific order ID:
./.venv/bin/python auto_booker.py --order-id ORDER-514792

# Automatically pick the oldest waiting client for a province:
./.venv/bin/python auto_booker.py --auto-pick "Barcelona" "POLICIA-TOMA DE HUELLA"
```
