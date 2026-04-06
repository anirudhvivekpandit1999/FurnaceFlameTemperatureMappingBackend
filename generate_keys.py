#!/usr/bin/env python3
"""
generate_keys.py
================
Run once to create all three encryption keys and print the
.env lines to paste into your server environment.

Usage:
    python generate_keys.py
"""

import secrets
import base64

def gen():
    raw = secrets.token_bytes(32)
    return base64.urlsafe_b64encode(raw).decode()

transit_key     = gen()
rest_enc_key    = gen()
rest_mac_key    = gen()
transit_out_key = gen()

print("# ─── Add these to your .env / systemd EnvironmentFile ───")
print(f"TRANSIT_KEY={transit_key}")
print(f"REST_ENC_KEY={rest_enc_key}")
print(f"REST_MAC_KEY={rest_mac_key}")
print()
print("# Optional separate key for outbound encryption")
print("# (omit to reuse TRANSIT_KEY for both in and out)")
print(f"# TRANSIT_OUT_KEY={transit_out_key}")
print()
print("# ─── Share TRANSIT_KEY with the frontend via .env ───────")
print(f"# VITE_TRANSIT_KEY={transit_key}")
print()
print("⚠  Store these securely. Never commit them to version control.")