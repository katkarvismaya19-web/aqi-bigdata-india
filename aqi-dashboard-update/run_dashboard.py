"""Start the AQI Insight dashboard.

Usage (from the project folder):
    python run_dashboard.py            # opens http://localhost:8000/dashboard/
    python run_dashboard.py 8080       # use another port

The dashboard reads the results written by the Spark backend, so run
`python src/aqi_project.py` first. Press Ctrl+C to stop the server.
"""
import http.server
import os
import socket
import sys
import threading
import webbrowser

ROOT = os.path.dirname(os.path.abspath(__file__))
NEEDED = ["outputs/dashboard_data.json", "outputs/daily_aqi_web.json"]


def free_port(preferred):
    for port in range(preferred, preferred + 20):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) != 0:
                return port
    raise SystemExit("No free port found between %d and %d." % (preferred, preferred + 19))


def main():
    missing = [f for f in NEEDED if not os.path.exists(os.path.join(ROOT, f))]
    if missing:
        print("The dashboard needs results from the Spark backend, but these files are missing:")
        for f in missing:
            print("   -", f)
        print("\nRun this first, then start the dashboard again:\n   python src/aqi_project.py")
        sys.exit(1)

    port = free_port(int(sys.argv[1]) if len(sys.argv) > 1 else 8000)
    handler = lambda *a, **kw: http.server.SimpleHTTPRequestHandler(*a, directory=ROOT, **kw)
    http.server.SimpleHTTPRequestHandler.log_message = lambda *a: None   # keep the console quiet
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), handler)

    url = "http://localhost:%d/dashboard/" % port
    print("AQI Insight dashboard is running at", url)
    print("Press Ctrl+C to stop.")
    threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
