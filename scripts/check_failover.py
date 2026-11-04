#!/usr/bin/env python3
"""Stop the demo Redis master and check automatic Sentinel promotion."""
import json
import os
import subprocess
import time
import urllib.request


def compose(*args):
    return subprocess.check_output(["docker", "compose", *args], text=True).strip()


def main():
    master = compose("exec", "-T", "sentinel-1", "redis-cli", "-p", "26379",
                     "--raw", "SENTINEL", "get-master-addr-by-name", "flight-cache").splitlines()[0]
    assert master in ("redis-master", "redis-replica"), f"Unexpected master: {master}"
    replacement = "redis-replica" if master == "redis-master" else "redis-master"
    compose("exec", "-T", master, "redis-cli", "PING")
    print(f"Stopping {master}...")
    try:
        compose("stop", master)
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            current = compose("exec", "-T", "sentinel-1", "redis-cli", "-p", "26379",
                              "--raw", "SENTINEL", "get-master-addr-by-name", "flight-cache").splitlines()
            if current and current[0] == replacement:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError(f"Sentinel did not promote {replacement}")
        assert "role:master" in compose("exec", "-T", replacement, "redis-cli", "INFO", "replication")
        base = os.environ.get("BASE_URL", "http://localhost:8080")
        with urllib.request.urlopen(base + "/flights/11111111-1111-4111-8111-111111111111", timeout=15) as response:
            assert json.load(response)["id"] == "11111111-1111-4111-8111-111111111111"
        print(f"OK: {replacement} is master, Booking REST is available")
    finally:
        compose("start", master)
        print("Original Redis container restarted; Sentinel will attach it as a replica.")


if __name__ == "__main__":
    main()
