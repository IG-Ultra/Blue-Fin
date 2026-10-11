from waitress import serve
from app import app
import os

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting Waitress production server on port {port}...")
    serve(app, host="0.0.0.0", port=port, threads=6)
