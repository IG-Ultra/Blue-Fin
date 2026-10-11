# Blue Fin - Backend Service

A lightweight, robust Python/Flask backend for personal social media downloading (Instagram, YouTube, Facebook) powered by `yt-dlp` and `FFmpeg`.

## Features
- **Strict Concurrency Cap:** Maximum 2 simultaneous downloads server-wide. Additional requests enter a FIFO queue (`In queue · #N`).
- **Zero Heavy Databases:** Pure zero-dependency storage via `data/state.json` (lifetime counts, API keys, and update walls).
- **Auto-Cleanup:** Background daemon purges temporary downloaded files older than 30 minutes.
- **Admin Dashboard:** Access at `/admin` (Default: `admin` / `bluefinadmin`).
  - Real-time job stats & system health
  - API Key generator & "Require API Key" toggle
  - Mandatory Update Wall publisher for Android clients
- **cPanel / Passenger & Waitress Ready:** Runs seamlessly on shared hosting (cPanel Setup Python App) or standard Linux VPS with Waitress / Gunicorn.

## Quick Start (Local / VPS)
```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py
```
Server runs by default on `http://0.0.0.0:5000`.

## Production Start (Waitress)
```bash
python wsgi.py
```

## Android App Connection
In the Blue Fin Android App, navigate to **Settings** -> **Server Address**:
- Android Emulator: `http://10.0.2.2:5000`
- Physical Device on local Wi-Fi: `http://<YOUR_COMPUTER_IP>:5000`
- Remote VPS / cPanel domain: `https://your-domain.com`
