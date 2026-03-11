#!/usr/bin/env python3
"""
Infraless Data - Local Development CLI

Run functions against real data via the platform API.
Requires Python 3.7+ and the `requests` library.

Setup:
  1. Generate an API key in Settings > API Keys
  2. Download .env.development from Settings > Environment Variables
  3. Paste your API key into .env.development

Usage:
  python dev.py run functions/extract_data.py raw_transactions sales_data
  python dev.py run functions/transform_sales.py --env production
"""

import sys
import os
import time
import argparse

try:
    import requests
except ImportError:
    print("Error: 'requests' library is required. Install with: pip install requests")
    sys.exit(1)


def load_env(env_file):
    """Parse a .env file into a dict. Handles KEY=VALUE lines, ignores comments."""
    env = {}
    if not os.path.exists(env_file):
        return env
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                    value = value[1:-1]
                env[key] = value
    return env


def get_config(args):
    """Load config from .env file based on environment."""
    environment = getattr(args, "env", None) or "development"

    # Accept both dotfile and non-dotfile names — browsers sometimes strip the
    # leading dot when downloading (e.g. .env.development → env.development).
    candidates = [f".env.{environment}", f"env.{environment}", ".env"]
    env_file = next((f for f in candidates if os.path.exists(f)), None)

    if not env_file:
        print(f"Error: No config file found. Expected .env.{environment}")
        print("Download one from Settings > Environment Variables > Download .env")
        sys.exit(1)

    env = load_env(env_file)

    api_url = env.get("INFRALESS_API_URL")
    api_key = env.get("INFRALESS_API_KEY")
    repo_id = env.get("INFRALESS_REPO_ID")

    if not api_url or not api_key or not repo_id:
        print(f"Error: {env_file} must contain INFRALESS_API_URL, INFRALESS_API_KEY, and INFRALESS_REPO_ID")
        sys.exit(1)

    if api_key == "wf_paste_your_key_here":
        print("Error: Replace the placeholder API key in your .env file with your actual key.")
        print("Generate one at Settings > API Keys")
        sys.exit(1)

    return {
        "api_url": api_url.rstrip("/"),
        "api_key": api_key,
        "repo_id": repo_id,
        "environment": env.get("INFRALESS_ENVIRONMENT", environment),
    }


def api_request(config, method, path, body=None, timeout=30):
    """Make an authenticated API request."""
    url = f"{config['api_url']}{path}"
    headers = {
        "Authorization": f"Bearer {config['api_key']}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.request(method, url, headers=headers, json=body, timeout=timeout)
    except requests.ConnectionError:
        print(f"Error: Could not connect to {config['api_url']}")
        sys.exit(1)

    if resp.status_code == 401:
        print("Error: Authentication failed. Check your API key.")
        sys.exit(1)

    if not resp.ok:
        try:
            error = resp.json().get("error", resp.text)
        except Exception:
            error = resp.text
        print(f"Error ({resp.status_code}): {error}")
        sys.exit(1)

    return resp.json()


# ── Commands ────────────────────────────────────────────────────────

def cmd_run(args):
    """Submit a function and poll every 10 seconds until complete."""
    config = get_config(args)

    file_path = args.file
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        sys.exit(1)

    with open(file_path) as f:
        function_code = f.read()

    print(f"Submitting {file_path} ({config['environment']})...")

    result = api_request(
        config, "POST",
        f"/api/repos/{config['repo_id']}/dev/run",
        {
            "functionCode": function_code,
            "entrypoint": args.entrypoint or "main",
            "args": args.args or [],
            "environment": config["environment"],
        },
        timeout=60,
    )

    run_id = result["runId"]
    start = time.time()

    while True:
        time.sleep(10)
        elapsed = int(time.time() - start)

        poll = api_request(config, "GET", f"/api/repos/{config['repo_id']}/dev/run/{run_id}")

        if poll.get("status") == "running":
            print(f"Running... {elapsed}s")
            continue

        # Complete
        print()
        if poll.get("logs"):
            print("── Logs ──")
            print(poll["logs"])
            print()

        final_status = poll.get("status", "unknown")
        duration = poll.get("duration", f"{elapsed}s")
        icon = "✓" if final_status == "success" else "✗"
        print(f"[{icon}] {final_status.title()} in {duration}")
        break


# ── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Infraless Data - Local Development CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--env", default=None, help="Environment: development (default) or production")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    run_parser = subparsers.add_parser("run", help="Run a function")
    run_parser.add_argument("file", help="Path to Python file")
    run_parser.add_argument("args", nargs="*", help="Arguments to pass to the function")
    run_parser.add_argument("--entrypoint", default="main", help="Function name to call (default: main)")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    cmd_run(args)


if __name__ == "__main__":
    main()
