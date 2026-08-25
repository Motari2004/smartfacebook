"""
Bluesky AI Vault → Facebook
AI chat: Bluesky fetch → vault → schedule / post-now (Zernio Facebook only)
Named auto pipelines, master-fetch reserve, niche sources — same logic as Instagram service.
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from atproto import Client
import json
import os
import requests
from datetime import datetime, timedelta
import traceback
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import psycopg2
from psycopg2.extras import Json, RealDictCursor
import uuid
import re
import random
import time
import base64
import pytz
import threading
from PIL import Image
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# VERCEL / ENV
# ============================================================

IS_VERCEL = os.environ.get('VERCEL') == '1' or os.environ.get('VERCEL_ENV') is not None
if IS_VERCEL:
    print("🚀 Running on Vercel (serverless mode) - background auto-pilot disabled")

BLUESKY_MASTER_HANDLE = os.environ.get('BLUESKY_MASTER_HANDLE')
BLUESKY_MASTER_PASSWORD = os.environ.get('BLUESKY_MASTER_PASSWORD')
if BLUESKY_MASTER_HANDLE and BLUESKY_MASTER_PASSWORD:
    print(f"✅ Master Bluesky account loaded: @{BLUESKY_MASTER_HANDLE}")
else:
    print("⚠️ No master Bluesky account in .env — master-fetch will need a session")

sessions = {}  # in-memory Bluesky session cache

app = Flask(__name__, static_folder='static')
CORS(app)

DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://neondb_owner:npg_3FJeskp5EoVg@ep-polished-sky-ayuedb1p-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
)

ZERNIO_API_KEY = os.environ.get('ZERNIO_API_KEY')
ZERNIO_BASE_URL = "https://zernio.com/api/v1"
SCHEDULE_TIMEZONE = "Africa/Nairobi"
TIMEZONE = "Africa/Nairobi"
LOCAL_TIMEZONE = pytz.timezone(TIMEZONE)

# Master-fetch / “fill reserve” — NEW media posts per source per click
# Cursor continues until target met or feed ends (safety max pages)
# Override: MASTER_FETCH_LIMIT=50  MASTER_FETCH_MAX_PAGES=50
MASTER_FETCH_LIMIT = int(os.environ.get('MASTER_FETCH_LIMIT', '50'))
MASTER_FETCH_MAX_PAGES = int(os.environ.get('MASTER_FETCH_MAX_PAGES', '50'))

# ============================================================
# GEMINI
# ============================================================

_env_keys = os.environ.get('GEMINI_API_KEYS', '') or os.environ.get('GEMINI_API_KEY', '')
if _env_keys:
    GEMINI_API_KEYS = [k.strip() for k in _env_keys.split(',') if k.strip()]
    print(f"✅ Loaded {len(GEMINI_API_KEYS)} Gemini keys from environment")
else:
    GEMINI_API_KEYS = []
    print("⚠️  No GEMINI_API_KEYS environment variable set!")

# Current IDs for OpenAI-compat endpoint (as of 2026-08). Avoid retired 1.5/2.0 names.
GEMINI_MODELS = [
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-3.5-flash-lite",
    "gemini-3.5-flash",
    "gemini-3.6-flash",
    "gemini-3.7-flash",
]
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai"

_gemini_model_index = 0
_gemini_key_index = 0
_gemini_key_cooldown = {}
_gemini_model_cooldown = {}


def next_gemini_key():
    global _gemini_key_index
    if not GEMINI_API_KEYS:
        return None
    for _ in range(len(GEMINI_API_KEYS) * 2):
        key_index = _gemini_key_index % len(GEMINI_API_KEYS)
        key = GEMINI_API_KEYS[key_index]
        if key in _gemini_key_cooldown:
            if datetime.now() < _gemini_key_cooldown[key]:
                _gemini_key_index += 1
                continue
        _gemini_key_index += 1
        return key
    return GEMINI_API_KEYS[0] if GEMINI_API_KEYS else None


def next_gemini_model():
    global _gemini_model_index
    if not GEMINI_MODELS:
        return "gemini-2.5-flash-lite"
    # Skip models still on cooldown when picking
    for _ in range(len(GEMINI_MODELS)):
        model = GEMINI_MODELS[_gemini_model_index % len(GEMINI_MODELS)]
        _gemini_model_index += 1
        until = _gemini_model_cooldown.get(model)
        if until and datetime.now() < until:
            continue
        return model
    return GEMINI_MODELS[_gemini_model_index % len(GEMINI_MODELS)]


def handle_model_rate_limit(model, seconds=60):
    _gemini_model_cooldown[model] = datetime.now() + timedelta(seconds=seconds)
    print(f"⏳ Model {model} on cooldown for {seconds}s")


def call_gemini(messages, tools=None, model=None, max_tokens=1200, timeout=45, _attempt=0):
    """Call Gemini OpenAI-compat API. Caps retries to avoid 404/429 spam loops."""
    max_attempts = max(len(GEMINI_MODELS) * 2, 4)
    if _attempt >= max_attempts:
        return None, "All Gemini models exhausted (404/429). Using keyword fallback."

    if model is None:
        model = next_gemini_model()

    if model in _gemini_model_cooldown:
        until = _gemini_model_cooldown[model]
        if datetime.now() < until:
            next_model = next_gemini_model()
            if next_model != model:
                return call_gemini(messages, tools, next_model, max_tokens, timeout, _attempt + 1)
            return None, "All models on cooldown"

    key = next_gemini_key()
    if not key:
        return None, "No Gemini API keys"

    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    try:
        r = requests.post(
            f"{GEMINI_BASE_URL}/chat/completions",
            headers=headers,
            json=payload,
            timeout=timeout,
        )
        print(f"📥 Gemini status: {r.status_code} model={model}")

        if r.status_code == 403:
            _gemini_key_cooldown[key] = datetime.now() + timedelta(seconds=300)
            next_key = next_gemini_key()
            if next_key and next_key != key:
                return call_gemini(messages, tools, model, max_tokens, timeout, _attempt + 1)
            return None, "All API keys invalid or on cooldown"

        if r.status_code == 429:
            handle_model_rate_limit(model, seconds=90)
            next_model = next_gemini_model()
            if next_model != model:
                return call_gemini(messages, tools, next_model, max_tokens, timeout, _attempt + 1)
            return None, "Rate limit exceeded"

        # 404 = model id not available for this key/endpoint — long cooldown
        if r.status_code == 404:
            handle_model_rate_limit(model, seconds=3600)
            print(f"⚠️ Model {model} returned 404 — cooling down 1h, trying next")
            next_model = next_gemini_model()
            if next_model != model:
                return call_gemini(messages, tools, next_model, max_tokens, timeout, _attempt + 1)
            return None, f"Gemini model not found (404): {model}"

        if r.status_code != 200:
            # Don't recurse forever on client errors
            if r.status_code >= 500:
                next_model = next_gemini_model()
                if next_model != model:
                    return call_gemini(messages, tools, next_model, max_tokens, timeout, _attempt + 1)
            return None, f"Gemini {r.status_code}: {r.text[:300]}"

        if model in _gemini_model_cooldown:
            del _gemini_model_cooldown[model]
        return r.json(), None
    except Exception as e:
        print(f"❌ Gemini exception ({model}): {e}")
        next_model = next_gemini_model()
        if next_model != model and _attempt + 1 < max_attempts:
            return call_gemini(messages, tools, next_model, max_tokens, timeout, _attempt + 1)
        return None, str(e)


def get_now():
    return datetime.now(LOCAL_TIMEZONE)


def format_datetime_for_zernio(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = LOCAL_TIMEZONE.localize(dt)
    return dt.astimezone(pytz.UTC).isoformat()


def parse_datetime_from_input(dt_str):
    if not dt_str:
        return None
    dt_str = str(dt_str).strip().replace('Z', '+00:00').replace('T', ' ')
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            dt = datetime.strptime(dt_str.split('+')[0].split('.')[0], fmt)
            if dt.tzinfo is None:
                dt = LOCAL_TIMEZONE.localize(dt)
            return dt
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            dt = LOCAL_TIMEZONE.localize(dt)
        return dt
    except Exception:
        return None


# ============================================================
# ZERNIO (Facebook accounts only)
# ============================================================

def _detect_accounts_for_key(api_key, label="key"):
    accounts = []
    if not api_key:
        return accounts
    try:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        response = requests.get(f"{ZERNIO_BASE_URL}/accounts", headers=headers, timeout=15)
        if response.status_code == 200:
            for acc in response.json().get('accounts', []):
                if (acc.get('platform') or '').lower() != 'facebook':
                    continue
                username = acc.get('username')
                if username:
                    accounts.append({
                        'username': username,
                        'platform': 'facebook',
                        'display_name': acc.get('displayName', ''),
                        'account_id': acc.get('_id'),
                        'profile_picture': acc.get('profilePicture'),
                    })
            print(f"✅ Auto-detected {len(accounts)} Facebook account(s) for {label}")
        else:
            print(f"⚠️ Could not fetch accounts for {label}: {response.status_code}")
    except Exception as e:
        print(f"⚠️ Error fetching accounts for {label}: {e}")
    return accounts


def get_zernio_api_keys():
    load_dotenv(override=False)
    keys = []
    seen = set()
    i = 1
    while True:
        key = (os.environ.get(f'ZERNIO_API_KEY{i}') or '').strip()
        if not key:
            break
        if key not in seen:
            env_var = f'ZERNIO_API_KEY{i}'
            accounts = _detect_accounts_for_key(key, env_var)
            keys.append({
                'key': key,
                'index': i,
                'accounts': accounts,
                'env_var': env_var,
                'account_count': len(accounts),
            })
            seen.add(key)
        i += 1

    csv = (os.environ.get('ZERNIO_API_KEYS') or '').strip()
    if csv:
        for part in csv.split(','):
            key = part.strip()
            if not key or key in seen:
                continue
            idx = len(keys) + 1
            accounts = _detect_accounts_for_key(key, f"ZERNIO_API_KEYS[{idx}]")
            keys.append({
                'key': key,
                'index': idx,
                'accounts': accounts,
                'env_var': 'ZERNIO_API_KEYS',
                'account_count': len(accounts),
            })
            seen.add(key)

    if not keys:
        default_key = (os.environ.get('ZERNIO_API_KEY') or '').strip()
        if default_key:
            accounts = _detect_accounts_for_key(default_key, 'ZERNIO_API_KEY')
            keys.append({
                'key': default_key,
                'index': 1,
                'accounts': accounts,
                'env_var': 'ZERNIO_API_KEY',
                'account_count': len(accounts),
            })
    return keys


def ensure_zernio_keys_loaded(for_auto: bool = False) -> dict:
    load_dotenv(override=False)
    keys = get_zernio_api_keys()
    global ZERNIO_API_KEY
    if keys:
        ZERNIO_API_KEY = keys[0]['key']
    previews = []
    for k in keys:
        prev = (k['key'][:12] + '…') if len(k.get('key') or '') > 12 else (k.get('key') or '?')
        acc_names = [a.get('username') for a in (k.get('accounts') or []) if a.get('username')]
        acc = ', '.join(acc_names) if acc_names else 'auto-detect'
        previews.append(f"{k.get('env_var', k.get('index'))}: {prev} ({acc})")
    if keys:
        msg = f"Loaded {len(keys)} Zernio API key(s) from .env"
        print(f"🔑 {msg}")
        for line in previews:
            print(f"   • {line}")
    else:
        msg = "No Zernio API keys found in .env."
        if for_auto:
            msg = "⚠️ Auto pilot cannot post: " + msg
        print(f"⚠️ {msg}")
    return {
        "success": len(keys) > 0,
        "count": len(keys),
        "keys_preview": previews,
        "message": msg,
        "keys": keys,
    }


def get_zernio_headers_for_key(api_key):
    if not api_key:
        return {}
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def get_zernio_headers():
    keys = get_zernio_api_keys()
    if keys:
        return get_zernio_headers_for_key(keys[0]['key'])
    default_key = os.environ.get('ZERNIO_API_KEY')
    if default_key:
        return get_zernio_headers_for_key(default_key)
    return {}


def get_zernio_headers_for_account(account_username=None, account_id=None):
    """Get the correct Zernio headers for a specific account."""
    
    # First try by account_id
    if account_id:
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT api_key FROM zernio_accounts
                    WHERE account_id = %s AND platform = 'facebook' AND is_active = TRUE
                    LIMIT 1
                """, (account_id,))
                row = cur.fetchone()
                cur.close()
                conn.close()
                if row and row[0]:
                    return get_zernio_headers_for_key(row[0])
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
    
    # Then try by username
    if account_username:
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT api_key FROM zernio_accounts
                    WHERE username = %s AND platform = 'facebook' AND is_active = TRUE
                    LIMIT 1
                """, (account_username,))
                row = cur.fetchone()
                cur.close()
                conn.close()
                if row and row[0]:
                    return get_zernio_headers_for_key(row[0])
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
    
    # Fallback to first key
    return get_zernio_headers()


def save_zernio_account_row(account_id, platform, display_name, username,
                            profile_picture, api_key, api_key_index=None):
    platform = (platform or 'facebook').lower()
    if platform != 'facebook' or not account_id:
        return False
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO zernio_accounts
                (account_id, platform, display_name, username, profile_picture,
                 api_key, api_key_index, is_active, last_sync)
                VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                ON CONFLICT (account_id, platform) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    username = EXCLUDED.username,
                    profile_picture = EXCLUDED.profile_picture,
                    api_key = EXCLUDED.api_key,
                    api_key_index = COALESCE(EXCLUDED.api_key_index, zernio_accounts.api_key_index),
                    is_active = TRUE,
                    last_sync = CURRENT_TIMESTAMP
            """, (
                account_id, platform, display_name, username,
                profile_picture, api_key, api_key_index,
            ))
        except Exception:
            cur.execute("""
                UPDATE zernio_accounts SET
                    display_name = %s, username = %s, profile_picture = %s,
                    api_key = %s, api_key_index = COALESCE(%s, api_key_index),
                    is_active = TRUE, last_sync = CURRENT_TIMESTAMP
                WHERE account_id = %s AND platform = %s
            """, (
                display_name, username, profile_picture,
                api_key, api_key_index, account_id, platform,
            ))
            if cur.rowcount == 0:
                cur.execute("""
                    INSERT INTO zernio_accounts
                    (account_id, platform, display_name, username, profile_picture,
                     api_key, api_key_index, is_active, last_sync)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                """, (
                    account_id, platform, display_name, username,
                    profile_picture, api_key, api_key_index,
                ))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Save error for account {username}: {e}")
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        return False


def refresh_all_zernio_accounts():
    status = ensure_zernio_keys_loaded()
    keys = status.get('keys') or []
    all_accounts = []
    for key_info in keys:
        api_key = key_info.get('key')
        index = key_info.get('index')
        prefetched = key_info.get('accounts') or []
        if prefetched:
            for acc in prefetched:
                aid = acc.get('account_id') or acc.get('_id')
                uname = acc.get('username')
                save_zernio_account_row(
                    account_id=aid,
                    platform='facebook',
                    display_name=acc.get('display_name') or acc.get('displayName') or uname,
                    username=uname,
                    profile_picture=acc.get('profile_picture') or acc.get('profilePicture'),
                    api_key=api_key,
                    api_key_index=index,
                )
                all_accounts.append(acc)
            continue
        headers = get_zernio_headers_for_key(api_key)
        if not headers:
            continue
        try:
            response = requests.get(f"{ZERNIO_BASE_URL}/accounts", headers=headers, timeout=15)
            if response.status_code == 200:
                for acc in response.json().get('accounts', []):
                    if (acc.get('platform') or '').lower() != 'facebook':
                        continue
                    save_zernio_account_row(
                        account_id=acc.get('_id'),
                        platform='facebook',
                        display_name=acc.get('displayName'),
                        username=acc.get('username'),
                        profile_picture=acc.get('profilePicture'),
                        api_key=api_key,
                        api_key_index=index,
                    )
                    all_accounts.append(acc)
        except Exception as e:
            print(f"Error fetching accounts for key {index}: {e}")
    return all_accounts


def tool_check_zernio_key(api_key: str = None, save_to_db: bool = True) -> dict:
    if not api_key or not str(api_key).strip():
        return {
            "success": False,
            "error": "No API key provided",
            "message": "Paste a Zernio key like: check key sk_xxxxx",
        }
    raw = str(api_key).strip()
    if '=' in raw:
        raw = raw.split('=', 1)[1].strip()
    raw = raw.strip().strip('"').strip("'")
    if not raw.startswith('sk_') and len(raw) < 20:
        return {
            "success": False,
            "error": "That does not look like a Zernio API key",
            "message": "Zernio keys usually start with sk_ — paste the full key.",
        }
    headers = get_zernio_headers_for_key(raw)
    try:
        response = requests.get(f"{ZERNIO_BASE_URL}/accounts", headers=headers, timeout=15)
    except Exception as e:
        return {"success": False, "error": str(e), "message": f"❌ Could not reach Zernio: {e}"}
    if response.status_code == 401:
        return {"success": False, "error": "Invalid API key", "message": "❌ Invalid or revoked key (401)."}
    if response.status_code == 429:
        return {"success": False, "error": "Rate limited", "message": "⚠️ Rate-limited. Try again later."}
    if response.status_code != 200:
        return {
            "success": False,
            "error": f"HTTP {response.status_code}",
            "message": f"❌ Zernio error {response.status_code}: {response.text[:200]}",
        }
    accounts = []
    for acc in response.json().get('accounts', []) or []:
        if (acc.get('platform') or '').lower() != 'facebook':
            continue
        username = acc.get('username')
        if not username:
            continue
        entry = {
            "username": username,
            "display_name": acc.get('displayName') or username,
            "platform": "facebook",
            "account_id": acc.get('_id'),
            "profile_picture": acc.get('profilePicture'),
        }
        accounts.append(entry)
        if save_to_db:
            save_zernio_account_row(
                account_id=entry['account_id'],
                platform='facebook',
                display_name=entry['display_name'],
                username=entry['username'],
                profile_picture=entry.get('profile_picture'),
                api_key=raw,
                api_key_index=None,
            )
    key_preview = raw[:12] + '…' if len(raw) > 12 else raw
    if not accounts:
        msg = (
            f"✅ Key valid ({key_preview})\n"
            f"But no Facebook accounts on this key yet.\n"
            f"Connect Facebook in the Zernio dashboard first."
        )
    else:
        lines = [f"✅ Key valid ({key_preview}) — {len(accounts)} Facebook account(s):"]
        for a in accounts:
            lines.append(f"  • @{a['username']} ({a['display_name']}) — id={a['account_id']}")
        msg = "\n".join(lines)
    return {
        "success": True,
        "valid": True,
        "key_preview": key_preview,
        "count": len(accounts),
        "accounts": accounts,
        "message": msg,
    }


def tool_list_api_keys():
    status = ensure_zernio_keys_loaded()
    keys = status.get('keys') or []
    total_accounts = sum(k.get('account_count', 0) for k in keys)
    return {
        "success": True,
        "count": len(keys),
        "total_accounts": total_accounts,
        "keys": keys,
        "message": status.get('message'),
    }


# ============================================================
# DATABASE
# ============================================================

def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection error: {e}")
        return None


def init_db():
    conn = get_db_connection()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                id SERIAL PRIMARY KEY,
                session_id TEXT UNIQUE NOT NULL,
                username TEXT NOT NULL,
                handle TEXT NOT NULL,
                display_name TEXT,
                avatar TEXT,
                session_string TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_sessions_session_id ON sessions(session_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_sessions_handle ON sessions(handle)')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS handlers (
                id SERIAL PRIMARY KEY,
                handle TEXT UNIQUE NOT NULL,
                display_name TEXT,
                avatar TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                selected BOOLEAN DEFAULT TRUE,
                is_default BOOLEAN DEFAULT FALSE
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS vault (
                id SERIAL PRIMARY KEY,
                uri TEXT UNIQUE NOT NULL,
                author TEXT NOT NULL,
                display_name TEXT,
                text TEXT,
                images JSONB,
                video JSONB,
                likes INTEGER DEFAULT 0,
                reposts INTEGER DEFAULT 0,
                replies INTEGER DEFAULT 0,
                created_at TIMESTAMP,
                saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                handler_handle TEXT,
                notes TEXT
            )
        ''')
        try:
            cur.execute("ALTER TABLE vault ADD COLUMN IF NOT EXISTS video JSONB")
            cur.execute("ALTER TABLE vault ADD COLUMN IF NOT EXISTS notes TEXT")
        except Exception:
            pass

        cur.execute('''
            CREATE TABLE IF NOT EXISTS deleted_posts (
                id SERIAL PRIMARY KEY,
                uri TEXT UNIQUE NOT NULL,
                handler_handle TEXT,
                deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS posted_posts (
                id SERIAL PRIMARY KEY,
                vault_id INTEGER REFERENCES vault(id),
                uri TEXT NOT NULL,
                platform VARCHAR(50) NOT NULL,
                platform_post_id VARCHAR(200),
                status VARCHAR(50) DEFAULT 'pending',
                posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                error_message TEXT,
                metadata JSONB,
                UNIQUE(uri, platform)
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS zernio_accounts (
                id SERIAL PRIMARY KEY,
                account_id VARCHAR(100) NOT NULL,
                platform VARCHAR(50) NOT NULL DEFAULT 'facebook',
                display_name VARCHAR(200),
                username VARCHAR(100),
                profile_picture TEXT,
                api_key TEXT,
                api_key_index INTEGER,
                is_active BOOLEAN DEFAULT TRUE,
                last_sync TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(account_id, platform)
            )
        ''')
        for col_sql in (
            "ALTER TABLE zernio_accounts ADD COLUMN IF NOT EXISTS api_key TEXT",
            "ALTER TABLE zernio_accounts ADD COLUMN IF NOT EXISTS api_key_index INTEGER",
            "ALTER TABLE zernio_accounts ADD COLUMN IF NOT EXISTS profile_picture TEXT",
            "ALTER TABLE zernio_accounts ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE",
            "ALTER TABLE zernio_accounts ADD COLUMN IF NOT EXISTS last_sync TIMESTAMP",
            "ALTER TABLE zernio_accounts ADD COLUMN IF NOT EXISTS display_name VARCHAR(200)",
            "ALTER TABLE zernio_accounts ADD COLUMN IF NOT EXISTS username VARCHAR(100)",
            "ALTER TABLE zernio_accounts ADD COLUMN IF NOT EXISTS platform VARCHAR(50) DEFAULT 'facebook'",
            "ALTER TABLE zernio_accounts ADD COLUMN IF NOT EXISTS account_id VARCHAR(100)",
        ):
            try:
                cur.execute(col_sql)
            except Exception as mig_e:
                print(f"zernio_accounts migrate skip: {mig_e}")

        try:
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS zernio_accounts_account_id_platform_uidx
                ON zernio_accounts (account_id, platform)
            """)
        except Exception as idx_e:
            print(f"zernio_accounts unique index attempt: {idx_e}")
            try:
                cur.execute("""
                    DELETE FROM zernio_accounts a
                    USING zernio_accounts b
                    WHERE a.account_id IS NOT NULL
                      AND a.account_id = b.account_id
                      AND COALESCE(a.platform, 'facebook') = COALESCE(b.platform, 'facebook')
                      AND a.ctid < b.ctid
                """)
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS zernio_accounts_account_id_platform_uidx
                    ON zernio_accounts (account_id, platform)
                """)
            except Exception as idx_e2:
                print(f"zernio_accounts unique index failed: {idx_e2}")

        cur.execute('''
            CREATE TABLE IF NOT EXISTS chat_history (
                id SERIAL PRIMARY KEY,
                session_key TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                tool_calls JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS auto_config (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL DEFAULT 'default',
                enabled BOOLEAN DEFAULT FALSE,
                source_handle TEXT,
                account_id TEXT,
                account_username TEXT,
                content_type TEXT DEFAULT 'feed',
                poll_interval_sec INTEGER DEFAULT 300,
                media_only BOOLEAN DEFAULT TRUE,
                include_reposts BOOLEAN DEFAULT FALSE,
                max_posts_per_run INTEGER DEFAULT 2,
                bluesky_handle TEXT,
                bluesky_app_password TEXT,
                last_run_at TIMESTAMP,
                last_error TEXT,
                last_result TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        try:
            cur.execute("ALTER TABLE auto_config ADD COLUMN IF NOT EXISTS niche TEXT")
            cur.execute("ALTER TABLE auto_config ADD COLUMN IF NOT EXISTS source_handles JSONB")
            print("✅ Added niche and source_handles columns to auto_config")
        except Exception as e:
            print(f"⚠️ Migration note: {e}")

        cur.execute('''
            CREATE TABLE IF NOT EXISTS auto_seen (
                id SERIAL PRIMARY KEY,
                config_name TEXT NOT NULL DEFAULT 'default',
                uri TEXT NOT NULL,
                posted BOOLEAN DEFAULT FALSE,
                seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(config_name, uri)
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS bluesky_accounts (
                id SERIAL PRIMARY KEY,
                handle TEXT UNIQUE NOT NULL,
                display_name TEXT,
                avatar TEXT,
                session_string TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS platform_mappings (
                id SERIAL PRIMARY KEY,
                config_name TEXT NOT NULL,
                platform VARCHAR(50) NOT NULL,
                account_username VARCHAR(100),
                account_id TEXT,
                UNIQUE(config_name, platform)
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS app_settings (
                id SERIAL PRIMARY KEY,
                key TEXT UNIQUE NOT NULL,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        cur.close()
        conn.close()
        print("✅ Database initialized (Facebook-only, multi-pipeline)")
    except Exception as e:
        print(f"❌ DB init error: {e}")
        traceback.print_exc()


init_db()


# ============================================================
# CRON STATE
# ============================================================

def get_cron_state():
    try:
        conn = get_db_connection()
        if not conn:
            return True
        cur = conn.cursor()
        cur.execute("SELECT value FROM app_settings WHERE key = 'cron_enabled'")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return row[0].lower() == 'true'
        return True
    except Exception:
        return True


def set_cron_state(enabled: bool):
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO app_settings (key, value, updated_at)
            VALUES ('cron_enabled', %s, CURRENT_TIMESTAMP)
            ON CONFLICT (key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP
        """, ('true' if enabled else 'false',))
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Cron state set to: {'ENABLED' if enabled else 'DISABLED'}")
    except Exception as e:
        print(f"Error saving cron state: {e}")


# ============================================================
# AUTO CONFIG HELPERS
# ============================================================

def _load_auto_config(name='default'):
    try:
        conn = get_db_connection()
        if not conn:
            return None
        cur = conn.cursor()
        cur.execute("SELECT * FROM auto_config WHERE name = %s", (name,))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return None
        cols = [d[0] for d in cur.description]
        cfg = dict(zip(cols, row))
        if cfg.get('source_handles'):
            if isinstance(cfg['source_handles'], str):
                try:
                    cfg['source_handles'] = json.loads(cfg['source_handles'])
                except Exception:
                    cfg['source_handles'] = [cfg['source_handles']]
        elif cfg.get('source_handle'):
            cfg['source_handles'] = [cfg['source_handle']]
        else:
            cfg['source_handles'] = []
        cur.close()
        conn.close()
        return cfg
    except Exception as e:
        print(f"load auto_config: {e}")
        return None


def _list_auto_configs():
    try:
        conn = get_db_connection()
        if not conn:
            return []
        cur = conn.cursor()
        cur.execute("SELECT * FROM auto_config ORDER BY name")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()
        conn.close()
        configs = []
        for r in rows:
            cfg = dict(zip(cols, r))
            if cfg.get('source_handles'):
                if isinstance(cfg['source_handles'], str):
                    try:
                        cfg['source_handles'] = json.loads(cfg['source_handles'])
                    except Exception:
                        cfg['source_handles'] = [cfg['source_handles']]
            elif cfg.get('source_handle'):
                cfg['source_handles'] = [cfg['source_handle']]
            else:
                cfg['source_handles'] = []
            configs.append(cfg)
        return configs
    except Exception as e:
        print(f"list auto_configs: {e}")
        return []


def _save_auto_config(cfg: dict):
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        source_handles_json = None
        if cfg.get('source_handles'):
            source_handles_json = Json(cfg['source_handles'])
        cur.execute('''
            INSERT INTO auto_config (
                name, enabled, source_handle, source_handles, niche,
                account_id, account_username, content_type, poll_interval_sec,
                media_only, include_reposts, max_posts_per_run, bluesky_handle,
                bluesky_app_password, last_run_at, last_error, last_result, updated_at
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP
            )
            ON CONFLICT (name) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source_handle = COALESCE(EXCLUDED.source_handle, auto_config.source_handle),
                source_handles = COALESCE(EXCLUDED.source_handles, auto_config.source_handles),
                niche = COALESCE(EXCLUDED.niche, auto_config.niche),
                account_id = COALESCE(EXCLUDED.account_id, auto_config.account_id),
                account_username = COALESCE(EXCLUDED.account_username, auto_config.account_username),
                content_type = COALESCE(EXCLUDED.content_type, auto_config.content_type),
                poll_interval_sec = COALESCE(EXCLUDED.poll_interval_sec, auto_config.poll_interval_sec),
                media_only = COALESCE(EXCLUDED.media_only, auto_config.media_only),
                include_reposts = COALESCE(EXCLUDED.include_reposts, auto_config.include_reposts),
                max_posts_per_run = COALESCE(EXCLUDED.max_posts_per_run, auto_config.max_posts_per_run),
                bluesky_handle = COALESCE(EXCLUDED.bluesky_handle, auto_config.bluesky_handle),
                bluesky_app_password = COALESCE(EXCLUDED.bluesky_app_password, auto_config.bluesky_app_password),
                last_run_at = COALESCE(EXCLUDED.last_run_at, auto_config.last_run_at),
                last_error = EXCLUDED.last_error,
                last_result = EXCLUDED.last_result,
                updated_at = CURRENT_TIMESTAMP
        ''', (
            cfg.get('name') or 'default',
            bool(cfg.get('enabled', False)),
            cfg.get('source_handle'),
            source_handles_json,
            cfg.get('niche'),
            cfg.get('account_id'),
            cfg.get('account_username'),
            cfg.get('content_type') or 'feed',
            int(cfg.get('poll_interval_sec') or 300),
            bool(cfg.get('media_only', True)),
            bool(cfg.get('include_reposts', False)),
            int(cfg.get('max_posts_per_run') or 2),
            cfg.get('bluesky_handle'),
            cfg.get('bluesky_app_password'),
            cfg.get('last_run_at'),
            cfg.get('last_error'),
            cfg.get('last_result'),
        ))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"save auto_config: {e}")
        traceback.print_exc()
        return False


def _resolve_pipeline_name(name):
    if not name:
        return None
    configs = _list_auto_configs()
    for c in configs:
        if c.get('name') == name:
            return name
    lower = name.lower()
    for c in configs:
        if (c.get('name') or '').lower() == lower:
            return c.get('name')
    return None


# ============================================================
# IMAGE HELPERS
# ============================================================

def data_url_to_jpeg_bytes(image_data: str):
    try:
        raw = image_data
        if ',' in raw and str(raw).strip().lower().startswith('data:'):
            raw = raw.split(',', 1)[1]
        binary = base64.b64decode(raw)
        img = Image.open(BytesIO(binary))
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        out = BytesIO()
        img.save(out, format='JPEG', quality=92, optimize=True)
        out.seek(0)
        return out, None
    except Exception as e:
        traceback.print_exc()
        return None, f"Invalid image data: {e}"


def fix_image_for_feed(image_bytes):
    try:
        image_bytes.seek(0)
        img = Image.open(image_bytes)
        if img.mode != 'RGB':
            img = img.convert('RGB')
        max_side = 2048
        w, h = img.size
        if max(w, h) > max_side:
            ratio = max_side / max(w, h)
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
        out = BytesIO()
        img.save(out, format='JPEG', quality=90, optimize=True)
        out.seek(0)
        return out
    except Exception:
        image_bytes.seek(0)
        return image_bytes


def upload_media_to_zernio(image_bytes, filename="upload.jpg"):
    try:
        image_bytes.seek(0)
        if filename.lower().endswith('.png'):
            content_type = 'image/png'
        else:
            content_type = 'image/jpeg'
        response = requests.post(
            f"{ZERNIO_BASE_URL}/media/presign",
            headers=get_zernio_headers(),
            json={"filename": filename, "contentType": content_type},
            timeout=30,
        )
        if response.status_code not in (200, 201):
            print(f"Presign error: {response.text}")
            return None
        data = response.json()
        upload_url = data.get('uploadUrl')
        public_url = data.get('publicUrl')
        if not upload_url or not public_url:
            return None
        image_bytes.seek(0)
        upload_response = requests.put(
            upload_url,
            headers={'Content-Type': content_type},
            data=image_bytes,
            timeout=60,
            verify=False,
        )
        if upload_response.status_code not in (200, 201, 204):
            print(f"Upload error: {upload_response.text}")
            return None
        return public_url
    except Exception as e:
        print(f"Error uploading media: {e}")
        traceback.print_exc()
        return None


# ============================================================
# FACEBOOK POSTING
# ============================================================

def create_facebook_post(text, account_id, media_urls=None, scheduled_for=None,
                         topic_tag=None, is_draft=False, content_type='feed'):
    try:
        platform_config = {"platform": "facebook", "accountId": account_id}
        if content_type and content_type != 'feed':
            platform_config["platformSpecificData"] = {"contentType": content_type}
        if topic_tag:
            platform_config.setdefault("platformSpecificData", {})["topic_tag"] = topic_tag

        payload = {"platforms": [platform_config]}
        if text and str(text).strip():
            payload["content"] = str(text).strip()[:2000]
        if media_urls:
            payload["mediaItems"] = []
            for url in media_urls:
                if url and str(url).startswith(('http://', 'https://')):
                    payload["mediaItems"].append({"type": "image", "url": url})

        if is_draft:
            payload["isDraft"] = True
        elif scheduled_for:
            payload["scheduledFor"] = scheduled_for
            payload["timezone"] = TIMEZONE
        else:
            payload["publishNow"] = True

        # ✅ FIX: Get the correct headers for this specific account
        headers = get_zernio_headers_for_account(account_id=account_id)
        if not headers:
            # Fallback to first key if account not found
            headers = get_zernio_headers()
            print(f"⚠️ No specific key for account {account_id}, using fallback")

        response = requests.post(
            f"{ZERNIO_BASE_URL}/posts",
            headers=headers,
            json=payload,
            timeout=30,
        )
        print(f"Facebook post response: {response.status_code}")
        if response.status_code == 201:
            data = response.json()
            post = data.get('post') or {}
            platforms = post.get('platforms') or [{}]
            return {
                "success": True,
                "post_id": post.get('_id'),
                "status": post.get('status'),
                "url": platforms[0].get('platformPostUrl') if platforms else None,
                "scheduled_for": post.get('scheduledFor'),
            }
        if response.status_code == 409:
            error_data = response.json() if response.text else {}
            return {
                "success": False,
                "error": "Duplicate content",
                "message": error_data.get('message', 'This content was already posted recently'),
            }
        try:
            error_data = response.json()
            error_msg = error_data.get('message', response.text)
        except Exception:
            error_msg = response.text
        return {"success": False, "error": f"API Error {response.status_code}", "message": error_msg}
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "error": str(e)}


def get_facebook_account_id(account_id=None, account_username=None):
    if account_id:
        return account_id
    if account_username:
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT account_id FROM zernio_accounts
                    WHERE username = %s AND platform = 'facebook' AND is_active = TRUE
                    LIMIT 1
                """, (account_username,))
                row = cur.fetchone()
                cur.close()
                conn.close()
                if row:
                    return row[0]
            except Exception as e:
                print(f"resolve username: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT account_id FROM zernio_accounts
                WHERE platform = 'facebook' AND is_active = TRUE
                ORDER BY last_sync DESC NULLS LAST, created_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                return row[0]
        except Exception as e:
            print(f"get first facebook: {e}")
            try:
                conn.close()
            except Exception:
                pass
    accounts = refresh_all_zernio_accounts()
    for acc in accounts:
        aid = acc.get('account_id') or acc.get('_id')
        if aid:
            return aid
    return None


def resolve_facebook_account_id(account_id=None, account_username=None):
    return get_facebook_account_id(account_id, account_username)


def post_to_facebook(image_url=None, image_bytes=None, caption="", account_id=None,
                     account_username=None, scheduled_time=None, content_type='feed'):
    resolved = resolve_facebook_account_id(account_id, account_username)
    if not resolved:
        return {"success": False, "error": "No Facebook account resolved. Connect one in Zernio."}

    media_urls = []
    if image_url:
        media_urls.append(image_url)
    elif image_bytes:
        public = upload_media_to_zernio(image_bytes)
        if not public:
            return {"success": False, "error": "Media upload failed"}
        media_urls.append(public)

    scheduled_for = None
    if scheduled_time:
        if isinstance(scheduled_time, datetime):
            scheduled_for = format_datetime_for_zernio(scheduled_time)
        else:
            scheduled_for = scheduled_time

    result = create_facebook_post(
        text=caption or "",
        account_id=resolved,
        media_urls=media_urls or None,
        scheduled_for=scheduled_for,
        content_type=content_type or 'feed',
    )
    if result.get('success'):
        result['account_id'] = resolved
        result['message'] = result.get('message') or (
            f"Scheduled to Facebook" if scheduled_for else "Posted to Facebook"
        )
    return result


# ============================================================
# BLUESKY LOGIN / FETCH
# ============================================================

def tool_login(username, password):
    try:
        username = (username or '').strip().lstrip('@')
        password = (password or '').strip()
        if not username or not password:
            return {"success": False, "error": "Handle and app password required"}
        client = Client()
        client.login(username, password)
        profile = client.me
        handle = profile.handle
        display_name = getattr(profile, 'display_name', None) or handle
        session_string = client.export_session_string()
        session_id = str(uuid.uuid4())
        expires = datetime.now() + timedelta(days=30)

        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO sessions (session_id, username, handle, display_name, session_string, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (session_id) DO UPDATE SET
                    session_string = EXCLUDED.session_string,
                    last_used_at = CURRENT_TIMESTAMP,
                    expires_at = EXCLUDED.expires_at
            """, (session_id, username, handle, display_name, session_string, expires))
            cur.execute("""
                INSERT INTO bluesky_accounts (handle, display_name, session_string, is_active, last_used_at)
                VALUES (%s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                ON CONFLICT (handle) DO UPDATE SET
                    session_string = EXCLUDED.session_string,
                    display_name = EXCLUDED.display_name,
                    is_active = TRUE,
                    last_used_at = CURRENT_TIMESTAMP
            """, (handle, display_name, session_string))
            conn.commit()
            cur.close()
            conn.close()

        sessions[session_id] = {
            'client': client,
            'handle': handle,
            'session_string': session_string,
            'display_name': display_name,
        }
        return {
            "success": True,
            "session_id": session_id,
            "handle": handle,
            "display_name": display_name,
            "message": f"Logged in as @{handle}",
        }
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "error": str(e)}


def tool_restore_session(handle):
    handle = (handle or '').strip().lstrip('@')
    if '.' not in handle:
        handle = handle + '.bsky.social'
    try:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "DB unavailable"}
        cur = conn.cursor()
        cur.execute("""
            SELECT session_id, session_string, handle, display_name FROM sessions
            WHERE handle = %s AND expires_at > CURRENT_TIMESTAMP
            ORDER BY last_used_at DESC LIMIT 1
        """, (handle,))
        row = cur.fetchone()
        if not row:
            cur.execute("""
                SELECT session_string, handle, display_name FROM bluesky_accounts
                WHERE handle = %s AND is_active = TRUE LIMIT 1
            """, (handle,))
            row2 = cur.fetchone()
            cur.close()
            conn.close()
            if not row2:
                return {"success": False, "error": f"No saved session for @{handle}"}
            session_string, h, display_name = row2
            client = Client()
            client.login(session_string=session_string)
            session_id = str(uuid.uuid4())
            sessions[session_id] = {
                'client': client,
                'handle': h,
                'session_string': session_string,
                'display_name': display_name or h,
            }
            return {
                "success": True,
                "session_id": session_id,
                "handle": h,
                "message": f"Restored session for @{h}",
            }
        session_id, session_string, h, display_name = row
        cur.close()
        conn.close()
        client = Client()
        client.login(session_string=session_string)
        sessions[session_id] = {
            'client': client,
            'handle': h,
            'session_string': session_string,
            'display_name': display_name or h,
        }
        return {
            "success": True,
            "session_id": session_id,
            "handle": h,
            "message": f"Restored session for @{h}",
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def _get_client_for_session(session_id):
    if session_id and session_id in sessions:
        return sessions[session_id]['client'], sessions[session_id]
    return None, None


def _get_any_bluesky_client():
    if sessions:
        sid = list(sessions.keys())[0]
        return sessions[sid]['client'], sessions[sid]
    if BLUESKY_MASTER_HANDLE and BLUESKY_MASTER_PASSWORD:
        try:
            client = Client()
            client.login(BLUESKY_MASTER_HANDLE, BLUESKY_MASTER_PASSWORD)
            return client, {'handle': BLUESKY_MASTER_HANDLE}
        except Exception as e:
            print(f"Master login failed: {e}")
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT session_id, session_string, handle FROM sessions
                WHERE expires_at > CURRENT_TIMESTAMP
                ORDER BY last_used_at DESC LIMIT 1
            """)
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                sid, ss, handle = row
                client = Client()
                client.login(session_string=ss)
                sessions[sid] = {'client': client, 'handle': handle, 'session_string': ss}
                return client, sessions[sid]
    except Exception as e:
        print(f"Restore any session: {e}")
    return None, None


def _client_auth_headers(client):
    """Bearer token for raw XRPC calls (avoids atproto Pydantic video-embed crashes)."""
    token = None
    try:
        # Common locations across atproto versions
        for attr in ('_session', 'session', '_me'):
            sess = getattr(client, attr, None)
            if sess is None:
                continue
            if isinstance(sess, dict):
                token = sess.get('accessJwt') or sess.get('access_jwt')
            else:
                token = getattr(sess, 'access_jwt', None) or getattr(sess, 'accessJwt', None)
            if token:
                break
        if not token and hasattr(client, 'request'):
            # Some versions store on request session
            hdrs = getattr(client.request, '_headers', None) or {}
            auth = hdrs.get('Authorization') or hdrs.get('authorization')
            if auth and str(auth).lower().startswith('bearer '):
                token = str(auth).split(' ', 1)[1]
    except Exception as e:
        print(f"auth header extract: {e}")
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _extract_images_from_embed(embed):
    """Parse images, video thumbs, and external thumbs from a raw Bluesky embed dict."""
    images = []
    if not embed or not isinstance(embed, dict):
        return images
    etype = str(embed.get('$type') or embed.get('py_type') or '')

    # app.bsky.embed.images#view
    if 'embed.images' in etype or embed.get('images'):
        for im in embed.get('images') or []:
            if not isinstance(im, dict):
                continue
            url = im.get('fullsize') or im.get('fullSize') or im.get('thumb')
            if url:
                images.append({"url": url, "thumb": im.get('thumb') or url})

    # app.bsky.embed.video#view
    if 'embed.video' in etype or (embed.get('playlist') and embed.get('thumbnail')):
        thumb = embed.get('thumbnail') or embed.get('thumb')
        if isinstance(thumb, dict):
            thumb = thumb.get('url') or thumb.get('$link')
        if thumb and isinstance(thumb, str):
            images.append({"url": thumb, "thumb": thumb, "is_video_thumb": True})

    # app.bsky.embed.external#view
    if 'embed.external' in etype or isinstance(embed.get('external'), dict):
        ext = embed.get('external') if isinstance(embed.get('external'), dict) else {}
        thumb = ext.get('thumb') or ext.get('thumbnail')
        if isinstance(thumb, dict):
            thumb = thumb.get('url') if isinstance(thumb.get('url'), str) else None
        if thumb and isinstance(thumb, str):
            images.append({"url": thumb, "thumb": thumb, "is_external_thumb": True})

    # recordWithMedia — nested media
    media = embed.get('media')
    if isinstance(media, dict):
        images.extend(_extract_images_from_embed(media))

    seen = set()
    uniq = []
    for im in images:
        u = im.get('url')
        if u and u not in seen:
            seen.add(u)
            uniq.append(im)
    return uniq


def _parse_feed_items(feed_items, actor):
    """Normalize raw getAuthorFeed feed[] items to post dicts."""
    posts = []
    for item in feed_items or []:
        post = item.get('post') or {}
        record = post.get('record') or {}
        author = post.get('author') or {}
        embed = post.get('embed') or {}
        images = _extract_images_from_embed(embed)
        posts.append({
            "uri": post.get('uri'),
            "author": author.get('handle') or actor,
            "display_name": author.get('displayName') or author.get('handle'),
            "text": record.get('text') or '',
            "images": images,
            "likes": post.get('likeCount') or 0,
            "reposts": post.get('repostCount') or 0,
            "replies": post.get('replyCount') or 0,
            "created_at": record.get('createdAt'),
            "is_repost": item.get('reason') is not None,
        })
    return posts


def _http_get_author_feed_page(client, actor, limit=50, cursor=None):
    """
    One page of getAuthorFeed via raw HTTP (avoids Pydantic video-embed crashes).
    Returns (posts_list, next_cursor).
    """
    actor = (actor or '').strip().lstrip('@')
    if actor and '.' not in actor:
        actor = actor + '.bsky.social'
    page_limit = max(1, min(int(limit or 50), 100))

    base = 'https://bsky.social'
    try:
        host = getattr(getattr(client, 'request', None), 'base_url', None) or getattr(client, '_base_url', None)
        if host:
            base = str(host).rstrip('/')
    except Exception:
        pass

    url = f"{base}/xrpc/app.bsky.feed.getAuthorFeed"
    headers = _client_auth_headers(client) if client else {}
    params = {"actor": actor, "limit": page_limit}
    if cursor:
        params["cursor"] = cursor

    def _try(u, h, p):
        return requests.get(u, headers=h or None, params=p, timeout=30)

    r = _try(url, headers, params)
    if r.status_code in (401, 403) or r.status_code != 200:
        r2 = _try(
            "https://api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed",
            headers if r.status_code != 401 else None,
            params,
        )
        if r2.status_code == 200:
            r = r2
        elif r.status_code != 200:
            raise RuntimeError(f"getAuthorFeed HTTP {r.status_code}: {r.text[:200]}")

    data = r.json()
    posts = _parse_feed_items(data.get('feed') or [], actor)
    next_cursor = data.get('cursor') or None
    return posts, next_cursor


def _raw_get_author_feed(client, actor, limit=20, max_pages=1):
    """
    Fetch author feed via raw HTTP so video embeds don't break atproto models.
    Paginates with cursor when max_pages > 1 (or when limit > one page).

    Args:
        limit: target number of posts to return (capped across pages)
        max_pages: max XRPC pages to walk (each page up to 100 items)
    """
    actor = (actor or '').strip().lstrip('@')
    if actor and '.' not in actor:
        actor = actor + '.bsky.social'

    target = max(1, int(limit or 20))
    # Auto page when caller asks for more than one Bluesky page
    pages = max(1, int(max_pages or 1))
    if target > 50 and pages < 2:
        pages = min(10, (target + 49) // 50)

    page_size = min(100, max(20, min(target, 100)))
    all_posts = []
    cursor = None
    seen_uris = set()

    for page_i in range(pages):
        try:
            batch, cursor = _http_get_author_feed_page(
                client, actor, limit=page_size, cursor=cursor
            )
        except Exception as e:
            if page_i == 0:
                # Last resort: typed client (may fail on video)
                print(f"raw feed failed ({e}); trying typed client")
                try:
                    feed = client.get_author_feed(actor=actor, limit=min(target, 50))
                    posts = []
                    for item in feed.feed:
                        post = item.post
                        record = post.record
                        images = []
                        view_embed = getattr(post, 'embed', None)
                        if view_embed:
                            imgs = getattr(view_embed, 'images', None)
                            if imgs:
                                for im in imgs:
                                    u = getattr(im, 'fullsize', None) or getattr(im, 'thumb', None)
                                    if u:
                                        images.append({"url": u, "thumb": getattr(im, 'thumb', u)})
                        posts.append({
                            "uri": post.uri,
                            "author": post.author.handle if post.author else actor,
                            "display_name": getattr(post.author, 'display_name', None),
                            "text": getattr(record, 'text', '') or '',
                            "images": images,
                            "likes": getattr(post, 'like_count', 0) or 0,
                            "reposts": getattr(post, 'repost_count', 0) or 0,
                            "replies": getattr(post, 'reply_count', 0) or 0,
                            "created_at": getattr(record, 'created_at', None),
                            "is_repost": getattr(item, 'reason', None) is not None,
                        })
                    return posts[:target]
                except Exception as e2:
                    print(f"typed client also failed: {e2}")
                    raise e
            print(f"feed page {page_i + 1} failed for @{actor}: {e}")
            break

        if not batch:
            break

        for p in batch:
            uri = p.get('uri')
            if uri and uri in seen_uris:
                continue
            if uri:
                seen_uris.add(uri)
            all_posts.append(p)
            if len(all_posts) >= target:
                break

        print(f"📄 @{actor} page {page_i + 1}: +{len(batch)} items (total {len(all_posts)}, cursor={'yes' if cursor else 'end'})")

        if len(all_posts) >= target:
            break
        if not cursor:
            break

    return all_posts[:target]


def tool_fetch_posts(session_id, actor, limit=15, media_only=True, include_reposts=False):
    try:
        client, meta = _get_client_for_session(session_id)
        if not client:
            client, meta = _get_any_bluesky_client()
        if not client:
            return {"success": False, "error": "Not logged in. Login with handle and app-password."}

        actor = (actor or '').strip().lstrip('@')
        if '.' not in actor:
            actor = actor + '.bsky.social'

        want = max(1, int(limit or 15))
        raw_posts = _raw_get_author_feed(
            client, actor,
            limit=min(want * 3 if media_only else want, 200),
            max_pages=max(1, min(8, (want + 49) // 50 + (2 if media_only else 0))),
        )
        posts = []
        for p in raw_posts:
            if not include_reposts and p.get('is_repost'):
                continue
            if media_only and not (p.get('images') or []):
                continue
            posts.append({
                "uri": p.get('uri'),
                "author": p.get('author'),
                "display_name": p.get('display_name'),
                "text": p.get('text') or '',
                "images": p.get('images') or [],
                "likes": p.get('likes') or 0,
                "reposts": p.get('reposts') or 0,
                "replies": p.get('replies') or 0,
                "created_at": p.get('created_at'),
            })

        if session_id and session_id in sessions:
            sessions[session_id]['_last_fetched'] = posts
            sessions[session_id]['_last_actor'] = actor

        return {
            "success": True,
            "posts": posts,
            "count": len(posts),
            "actor": actor,
            "message": f"Fetched {len(posts)} posts from @{actor}",
        }
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "error": str(e)}


def tool_add_to_vault(posts, handler_handle=None, retag=False):
    """
    Save posts to vault. URI is unique globally.
    retag=True (master-fetch): if URI already exists under another handler_handle,
    update handler_handle to this pipeline so the niche reserve can use it.
    """
    if not posts:
        return {"success": False, "error": "No posts to save"}
    saved = 0
    skipped = 0
    retagged = 0
    hh = handler_handle
    try:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "Database unavailable"}
        cur = conn.cursor()
        for p in posts:
            uri = p.get('uri')
            if not uri:
                continue
            tag = hh or p.get('author')
            try:
                if retag and tag:
                    # Insert or claim for this pipeline
                    cur.execute("""
                        INSERT INTO vault (uri, author, display_name, text, images, likes, reposts, replies, created_at, handler_handle)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (uri) DO UPDATE SET
                            handler_handle = EXCLUDED.handler_handle,
                            images = COALESCE(EXCLUDED.images, vault.images),
                            text = COALESCE(NULLIF(EXCLUDED.text, ''), vault.text),
                            likes = GREATEST(vault.likes, EXCLUDED.likes),
                            reposts = GREATEST(vault.reposts, EXCLUDED.reposts),
                            replies = GREATEST(vault.replies, EXCLUDED.replies)
                        WHERE vault.handler_handle IS DISTINCT FROM EXCLUDED.handler_handle
                           OR vault.images IS NULL
                    """, (
                        uri,
                        p.get('author') or 'unknown',
                        p.get('display_name'),
                        p.get('text'),
                        Json(p.get('images') or []),
                        int(p.get('likes') or 0),
                        int(p.get('reposts') or 0),
                        int(p.get('replies') or 0),
                        p.get('created_at'),
                        tag,
                    ))
                    if cur.rowcount:
                        # Could be insert or update — distinguish
                        cur.execute(
                            "SELECT handler_handle FROM vault WHERE uri = %s",
                            (uri,),
                        )
                        row = cur.fetchone()
                        # rowcount 1 on both insert and update; check if was pure skip
                        saved += 1  # count as acquired for this niche
                        # We'll refine: if already had this handler, it's skip
                    else:
                        # Already tagged for this pipeline
                        skipped += 1
                else:
                    cur.execute("""
                        INSERT INTO vault (uri, author, display_name, text, images, likes, reposts, replies, created_at, handler_handle)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (uri) DO NOTHING
                    """, (
                        uri,
                        p.get('author') or 'unknown',
                        p.get('display_name'),
                        p.get('text'),
                        Json(p.get('images') or []),
                        int(p.get('likes') or 0),
                        int(p.get('reposts') or 0),
                        int(p.get('replies') or 0),
                        p.get('created_at'),
                        tag,
                    ))
                    if cur.rowcount:
                        saved += 1
                    else:
                        skipped += 1
            except Exception as e:
                print(f"vault insert: {e}")
                skipped += 1
        conn.commit()

        # Recount retagged vs new for clearer message when retag=True
        if retag and tag and posts:
            try:
                uris = [p.get('uri') for p in posts if p.get('uri')]
                cur.execute(
                    """
                    SELECT COUNT(*) FROM vault
                    WHERE handler_handle = %s AND uri = ANY(%s)
                    """,
                    (tag, uris),
                )
                in_pipeline = cur.fetchone()[0]
            except Exception:
                in_pipeline = saved
        else:
            in_pipeline = saved

        cur.close()
        conn.close()
        msg = f"Saved {saved} to vault ({skipped} already present)"
        if retag:
            msg = (
                f"Pipeline «{tag}»: {in_pipeline} posts available in reserve "
                f"(new/claimed {saved}, already same-pipeline {skipped})"
            )
        return {
            "success": True,
            "saved": saved,
            "skipped": skipped,
            "retagged": retagged,
            "in_pipeline": in_pipeline if retag else saved,
            "message": msg,
        }
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "error": str(e)}


def _normalize_pipeline_filter(pipeline=None, handler_handle=None):
    """Resolve user-facing pipeline/niche name to handler_handle used in vault."""
    key = (pipeline or handler_handle or '').strip()
    if not key:
        return None
    resolved = _resolve_pipeline_name(key)
    if resolved:
        return resolved
    for c in _list_auto_configs():
        if (c.get('name') or '').lower() == key.lower():
            return c.get('name')
        if (c.get('niche') or '').lower() == key.lower():
            return c.get('name')
    return key


def _format_vault_list_message(items, total, label="Vault"):
    if not items:
        return f"{label}: empty."
    lines = [f"📦 {label} — showing {len(items)} of {total}:"]
    for it in items:
        imgs = it.get('images') or []
        if isinstance(imgs, str):
            try:
                imgs = json.loads(imgs)
            except Exception:
                imgs = []
        nimg = len(imgs) if isinstance(imgs, list) else 0
        text = (it.get('text') or '').strip()
        if len(text) > 70:
            text = text[:70] + "…"
        if not text:
            text = "(image only)" if nimg else "(no text)"
        media = f" 📸{nimg}" if nimg else ""
        lines.append(f"  #{it.get('id')} @{it.get('author')}: {text}{media}")
    return "\n".join(lines)


def tool_list_vault(limit=15, pipeline=None, handler_handle=None):
    """List vault items. Optional pipeline filters one reserve (e.g. wildlife)."""
    try:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "DB unavailable"}
        cur = conn.cursor(cursor_factory=RealDictCursor)
        limit = int(limit or 15)
        hh = _normalize_pipeline_filter(pipeline, handler_handle)

        if hh:
            cur.execute("""
                SELECT id, uri, author, display_name, text, images, likes, reposts, replies,
                       created_at, saved_at, handler_handle, notes
                FROM vault
                WHERE handler_handle = %s
                ORDER BY saved_at DESC
                LIMIT %s
            """, (hh, limit))
            rows = cur.fetchall()
            cur.execute("SELECT COUNT(*) AS count FROM vault WHERE handler_handle = %s", (hh,))
            total = cur.fetchone()['count']
            label = f"Vault / reserve «{hh}»"
        else:
            cur.execute("""
                SELECT id, uri, author, display_name, text, images, likes, reposts, replies,
                       created_at, saved_at, handler_handle, notes
                FROM vault
                ORDER BY saved_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            cur.execute("SELECT COUNT(*) AS count FROM vault")
            total = cur.fetchone()['count']
            label = "Vault"

        cur.close()
        conn.close()
        vault = []
        for r in rows:
            item = dict(r)
            if item.get('images') and isinstance(item['images'], str):
                try:
                    item['images'] = json.loads(item['images'])
                except Exception:
                    pass
            vault.append(item)
        return {
            "success": True,
            "vault": vault,
            "count": total,
            "pipeline": hh,
            "message": _format_vault_list_message(vault, total, label),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_list_vault_by_status(status='all', limit=50, pipeline=None, handler_handle=None):
    """List vault by status; optional pipeline filters that niche's reserve."""
    try:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "DB unavailable"}
        cur = conn.cursor(cursor_factory=RealDictCursor)
        limit = int(limit or 50)
        hh = _normalize_pipeline_filter(pipeline, handler_handle)
        status = (status or 'all').lower()

        where = []
        params = []
        if hh:
            where.append("v.handler_handle = %s")
            params.append(hh)

        if status == 'unposted':
            # 'duplicate' is a terminal state (Facebook/Zernio rejected as
            # already-posted content) — exclude it same as posted/scheduled
            # so unposted counts reflect what's actually still postable.
            where.append("""NOT EXISTS (
                SELECT 1 FROM posted_posts p
                WHERE p.uri = v.uri AND p.platform = 'facebook'
                  AND p.status IN ('completed', 'posted', 'scheduled', 'duplicate')
            )""")
        elif status == 'posted':
            where.append("""EXISTS (
                SELECT 1 FROM posted_posts p
                WHERE p.uri = v.uri AND p.platform = 'facebook'
                  AND p.status IN ('completed', 'posted')
            )""")
        elif status == 'scheduled':
            where.append("""EXISTS (
                SELECT 1 FROM posted_posts p
                WHERE p.uri = v.uri AND p.platform = 'facebook' AND p.status = 'scheduled'
            )""")
        elif status == 'duplicate':
            where.append("""EXISTS (
                SELECT 1 FROM posted_posts p
                WHERE p.uri = v.uri AND p.platform = 'facebook' AND p.status = 'duplicate'
            )""")

        sql_where = ("WHERE " + " AND ".join(where)) if where else ""
        cur.execute(
            f"SELECT COUNT(*) AS cnt FROM vault v {sql_where}",
            params,
        )
        total = int(cur.fetchone()['cnt'] or 0)

        cur.execute(
            f"SELECT v.* FROM vault v {sql_where} ORDER BY v.saved_at DESC LIMIT %s",
            params + [limit],
        )
        rows = [dict(r) for r in cur.fetchall()]
        for item in rows:
            if item.get('images') and isinstance(item['images'], str):
                try:
                    item['images'] = json.loads(item['images'])
                except Exception:
                    pass
        cur.close()
        conn.close()

        label_bits = []
        if hh:
            label_bits.append(f"«{hh}»")
        label_bits.append(status)
        label = "Vault " + " · ".join(label_bits)
        # Short count-first line for "how many" questions
        count_line = f"📊 {label}: **{total}** posts"
        if total > len(rows):
            count_line += f" (showing {len(rows)} most recent)"
        return {
            "success": True,
            "vault": rows,
            "count": total,
            "showing": len(rows),
            "status": status,
            "pipeline": hh,
            "message": count_line + "\n" + _format_vault_list_message(rows, total, label),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_delete_vault_items(ids=None, status=None, all=False):
    try:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "DB unavailable"}
        cur = conn.cursor()
        deleted = 0
        if all:
            cur.execute("DELETE FROM vault")
            deleted = cur.rowcount
        elif ids:
            cur.execute("DELETE FROM vault WHERE id = ANY(%s)", (list(ids),))
            deleted = cur.rowcount
        elif status == 'unposted':
            cur.execute("""
                DELETE FROM vault v WHERE NOT EXISTS (
                    SELECT 1 FROM posted_posts p WHERE p.uri = v.uri AND p.platform = 'facebook'
                )
            """)
            deleted = cur.rowcount
        elif status in ('posted', 'scheduled'):
            st = 'posted' if status == 'posted' else 'scheduled'
            cur.execute("""
                DELETE FROM vault v WHERE EXISTS (
                    SELECT 1 FROM posted_posts p
                    WHERE p.uri = v.uri AND p.platform = 'facebook' AND p.status = %s
                )
            """, (st if st == 'scheduled' else 'posted',))
            deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return {"success": True, "deleted_count": deleted, "message": f"Deleted {deleted} vault item(s)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def _mark_posted(vault_id, uri, platform_post_id=None, status='posted', error=None, metadata=None):
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO posted_posts (vault_id, uri, platform, platform_post_id, status, error_message, metadata)
            VALUES (%s, %s, 'facebook', %s, %s, %s, %s)
            ON CONFLICT (uri, platform) DO UPDATE SET
                status = EXCLUDED.status,
                platform_post_id = COALESCE(EXCLUDED.platform_post_id, posted_posts.platform_post_id),
                error_message = EXCLUDED.error_message,
                posted_at = CURRENT_TIMESTAMP,
                metadata = COALESCE(EXCLUDED.metadata, posted_posts.metadata)
        """, (vault_id, uri, platform_post_id, status, error, Json(metadata) if metadata else None))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"mark posted: {e}")


def tool_post_now(vault_id=None, uri=None, caption=None, account_username=None, account_id=None, content_type='feed'):
    try:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "DB unavailable"}
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if vault_id is not None:
            cur.execute("SELECT * FROM vault WHERE id = %s", (int(vault_id),))
        elif uri:
            cur.execute("SELECT * FROM vault WHERE uri = %s", (uri,))
        else:
            cur.close()
            conn.close()
            return {"success": False, "error": "vault_id or uri required"}
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return {"success": False, "error": "Vault item not found"}

        item = dict(row)
        images = item.get('images') or []
        if isinstance(images, str):
            try:
                images = json.loads(images)
            except Exception:
                images = []
        image_url = None
        if images:
            first = images[0]
            image_url = first.get('url') if isinstance(first, dict) else first

        text = caption if caption is not None else (item.get('text') or '')
        result = post_to_facebook(
            image_url=image_url,
            caption=text,
            account_id=account_id,
            account_username=account_username,
            content_type=content_type or 'feed',
        )

        # Distinguish terminal failures (will never succeed on retry) from
        # transient ones. Duplicate-content 409s are permanent for this
        # vault item — mark them 'duplicate' so pipelines skip them forever
        # instead of retrying every cron run.
        if result.get('success'):
            status = 'posted'
        else:
            err_text = (result.get('error') or result.get('message') or '').lower()
            if 'duplicate' in err_text:
                status = 'duplicate'
            else:
                status = 'failed'

        _mark_posted(
            vault_id=item.get('id'),
            uri=item.get('uri'),
            platform_post_id=result.get('post_id'),
            status=status,
            error=None if result.get('success') else (result.get('error') or result.get('message')),
            metadata=result,
        )
        if result.get('success'):
            result['message'] = f"Posted vault #{item.get('id')} to Facebook"
            result['vault_id'] = item.get('id')
        elif status == 'duplicate':
            result['message'] = f"Vault #{item.get('id')} is a duplicate on Facebook — skipping permanently"
            result['vault_id'] = item.get('id')
        return result
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "error": str(e)}


def tool_post_vault_batch(count=3, account_username=None, account_id=None, content_type='feed'):
    r = tool_list_vault_by_status(status='unposted', limit=int(count or 3))
    items = r.get('vault') or []
    if not items:
        return {"success": False, "error": "No unposted vault items"}
    posted = 0
    errors = []
    for it in items[: int(count or 3)]:
        res = tool_post_now(
            vault_id=it.get('id'),
            account_username=account_username,
            account_id=account_id,
            content_type=content_type,
        )
        if res.get('success'):
            posted += 1
        else:
            errors.append(res.get('error') or res.get('message'))
    return {
        "success": posted > 0,
        "posted_count": posted,
        "message": f"Posted {posted}/{len(items[:int(count or 3)])} to Facebook",
        "errors": errors[:3],
    }


def tool_post_unposted(account_username=None, limit=10):
    return tool_post_vault_batch(count=limit, account_username=account_username)


def tool_list_accounts(platform='facebook', refresh=False):
    """List active Facebook accounts from DB. Set refresh=True to re-sync from Zernio."""
    try:
        if refresh:
            refresh_all_zernio_accounts()
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "DB unavailable"}
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT account_id, username, display_name, platform, profile_picture, is_active
            FROM zernio_accounts
            WHERE platform = %s AND is_active = TRUE
            ORDER BY username
        """, (platform,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        accounts = []
        for r in rows:
            accounts.append({
                "account_id": r['account_id'],
                "label": r['username'] or r['display_name'],
                "username": r['username'],
                "display_name": r['display_name'],
                "platform": r['platform'],
                "profile_picture": r.get('profile_picture'),
            })
        return {
            "success": True,
            "accounts": accounts,
            "count": len(accounts),
            "message": f"{len(accounts)} Facebook account(s):\n" + "\n".join(
                f"  {i}. {a.get('display_name') or a.get('username')} (@{a.get('username')})"
                for i, a in enumerate(accounts, 1)
            ),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_remove_facebook_account(username=None, account_id=None, permanent=False):
    """
    Remove a Facebook account from this app's DB (soft by default).
    Does not disconnect the page in Zernio — only stops this service from using it.
    Match by display name, username, or account_id.
    """
    if not username and not account_id:
        return {"success": False, "error": "Provide username or account_id"}
    try:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "DB unavailable"}
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Resolve row by id, username, or display_name (case-insensitive)
        row = None
        if account_id:
            cur.execute(
                "SELECT * FROM zernio_accounts WHERE platform = 'facebook' AND account_id = %s",
                (str(account_id),),
            )
            row = cur.fetchone()
        if not row and username:
            q = username.lstrip('@').strip()
            cur.execute(
                """
                SELECT * FROM zernio_accounts
                WHERE platform = 'facebook'
                  AND (
                    LOWER(username) = LOWER(%s)
                    OR LOWER(display_name) = LOWER(%s)
                    OR LOWER(username) LIKE LOWER(%s)
                    OR LOWER(display_name) LIKE LOWER(%s)
                  )
                ORDER BY is_active DESC
                LIMIT 1
                """,
                (q, q, f"%{q}%", f"%{q}%"),
            )
            row = cur.fetchone()

        if not row:
            cur.close()
            conn.close()
            return {
                "success": False,
                "error": f"Facebook account not found: {username or account_id}",
                "message": f"❌ No matching Facebook account for «{username or account_id}»",
            }

        label = row.get('display_name') or row.get('username') or row.get('account_id')
        aid = row.get('account_id')

        if permanent:
            cur.execute(
                "DELETE FROM zernio_accounts WHERE platform = 'facebook' AND account_id = %s",
                (aid,),
            )
            action = "deleted"
        else:
            cur.execute(
                """
                UPDATE zernio_accounts SET is_active = FALSE, last_sync = CURRENT_TIMESTAMP
                WHERE platform = 'facebook' AND account_id = %s
                """,
                (aid,),
            )
            action = "deactivated"

        # Clear destination on pipelines that pointed here
        try:
            cur.execute(
                """
                UPDATE auto_config
                SET account_username = NULL, account_id = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE account_username = %s OR account_id = %s
                """,
                (row.get('username'), aid),
            )
        except Exception:
            pass

        conn.commit()
        cur.close()
        conn.close()
        return {
            "success": True,
            "action": action,
            "username": row.get('username'),
            "display_name": row.get('display_name'),
            "account_id": aid,
            "message": (
                f"✅ Facebook account **{label}** ({action}) in this app.\n"
                f"Pipelines that used it no longer have a destination.\n"
                f"To fully disconnect the page, remove it in the Zernio dashboard too."
            ),
        }
    except Exception as e:
        return {"success": False, "error": str(e), "message": f"❌ {e}"}


def tool_get_status():
    try:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "DB unavailable"}
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM vault")
        vault_count = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM posted_posts
            WHERE platform = 'facebook' AND status IN ('posted', 'completed')
        """)
        posted_count = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM posted_posts
            WHERE platform = 'facebook' AND status = 'scheduled'
        """)
        scheduled_count = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM zernio_accounts
            WHERE platform = 'facebook' AND is_active = TRUE
        """)
        accounts_count = cur.fetchone()[0]
        active_handle = None
        if sessions:
            active_handle = list(sessions.values())[0].get('handle')
        else:
            cur.execute("""
                SELECT handle FROM sessions
                WHERE expires_at > CURRENT_TIMESTAMP
                ORDER BY last_used_at DESC LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                active_handle = row[0]
        cur.close()
        conn.close()
        return {
            "success": True,
            "vault_count": vault_count,
            "posted_count": posted_count,
            "scheduled_count": scheduled_count,
            "accounts_count": accounts_count,
            "active_handle": active_handle,
            "message": (
                f"📊 Status:\n"
                f"  • Vault: {vault_count}\n"
                f"  • Posted: {posted_count}\n"
                f"  • Scheduled: {scheduled_count}\n"
                f"  • Facebook accounts: {accounts_count}\n"
                f"  • Bluesky: @{active_handle}" if active_handle else
                f"📊 Status: vault={vault_count} posted={posted_count} accounts={accounts_count}"
            ),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_list_scheduled():
    try:
        headers = get_zernio_headers()
        if not headers:
            return {"success": False, "error": "No Zernio key"}
        response = requests.get(
            f"{ZERNIO_BASE_URL}/posts",
            headers=headers,
            params={"status": "scheduled", "limit": 50},
            timeout=20,
        )
        if response.status_code != 200:
            return {"success": False, "error": f"Zernio {response.status_code}"}
        posts = response.json().get('posts') or []
        fb = []
        for p in posts:
            platforms = p.get('platforms') or []
            if any((pl.get('platform') or '').lower() == 'facebook' for pl in platforms):
                fb.append({
                    "id": p.get('_id'),
                    "text": (p.get('content') or '')[:120],
                    "scheduled_for": p.get('scheduledFor'),
                    "status": p.get('status'),
                })
        return {"success": True, "scheduled": fb, "count": len(fb)}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============================================================
# AUTO PILOT / PIPELINES
# ============================================================

_auto_thread = None
_auto_running = False
_auto_lock = threading.Lock()


def tool_auto_setup(name='default', source_handle=None, account_username=None,
                    account_id=None, poll_interval_sec=300, max_posts_per_run=2,
                    media_only=True, include_reposts=False, enabled=False,
                    source_handles=None, niche=None, content_type='feed'):
    """Create/update a pipeline. Facebook destination is OPTIONAL — can set later."""
    name = (name or 'default').strip()
    sources = source_handles
    if not sources and source_handle:
        sources = [source_handle.strip().lstrip('@')]
    if sources:
        sources = [
            (s if '.' in s else s + '.bsky.social') if isinstance(s, str) else s
            for s in sources
        ]
        sources = [s.lstrip('@') for s in sources if s]
    if not sources:
        return {"success": False, "error": "source_handle or source_handles required"}

    # Optional FB account: auto-pick only if exactly one connected; never fail if missing
    if not account_username and not account_id:
        accs = tool_list_accounts('facebook')
        alist = accs.get('accounts') or []
        if len(alist) == 1:
            account_username = alist[0].get('username')
            account_id = alist[0].get('account_id')

    dest_label = None
    if account_username or account_id:
        dest_label = f"Facebook @{account_username or account_id}"
    else:
        dest_label = "Facebook (destination not set yet)"

    cfg = {
        'name': name,
        'enabled': bool(enabled),
        'source_handle': sources[0],
        'source_handles': sources,
        'niche': niche or name,
        'account_username': account_username,
        'account_id': account_id,
        'poll_interval_sec': int(poll_interval_sec or 300),
        'max_posts_per_run': int(max_posts_per_run or 2),
        'media_only': bool(media_only),
        'include_reposts': bool(include_reposts),
        'content_type': content_type or 'feed',
        'last_error': None,
        'last_result': 'configured',
    }
    if not _save_auto_config(cfg):
        return {"success": False, "error": "Failed to save config"}
    tip = ""
    if not account_username and not account_id:
        tip = "\nTip: later say “set destination for Lifestyle to @YourPage” or list accounts."
    return {
        "success": True,
        "message": (
            f"✅ Pipeline '{name}' configured: "
            f"@{' + @'.join(sources)} → {dest_label} "
            f"(max {cfg['max_posts_per_run']}/run · driven by external cron)"
            f"{tip}"
        ),
        "config": cfg,
    }


def tool_auto_set_destination(name, account_username=None, account_id=None):
    """Attach/update Facebook destination on an existing pipeline."""
    if not name:
        return {"success": False, "error": "pipeline name required"}
    resolved = _resolve_pipeline_name(name) or name
    cfg = _load_auto_config(resolved)
    if not cfg:
        return {"success": False, "error": f"Pipeline '{name}' not found"}
    if not account_username and not account_id:
        return {"success": False, "error": "account_username or account_id required"}
    if account_username and not account_id:
        account_id = get_facebook_account_id(account_username=account_username)
    cfg['account_username'] = account_username or cfg.get('account_username')
    cfg['account_id'] = account_id or cfg.get('account_id')
    cfg['last_result'] = f"destination set → @{cfg.get('account_username') or cfg.get('account_id')}"
    if not _save_auto_config(cfg):
        return {"success": False, "error": "Failed to save"}
    return {
        "success": True,
        "message": (
            f"✅ Pipeline '{resolved}' destination: "
            f"Facebook @{cfg.get('account_username') or cfg.get('account_id')}"
        ),
        "config": cfg,
    }


def tool_auto_start(name=None):
    """Enable pipeline(s), persist cron_enabled=true in DB (survives restarts)."""
    global _auto_running
    configs = _list_auto_configs()
    if not configs:
        return {"success": False, "error": "No pipelines. Use auto setup first."}
    if name:
        resolved = _resolve_pipeline_name(name)
        if not resolved:
            return {"success": False, "error": f"Pipeline '{name}' not found"}
        cfg = _load_auto_config(resolved)
        cfg['enabled'] = True
        _save_auto_config(cfg)
        names = [resolved]
    else:
        for c in configs:
            c['enabled'] = True
            _save_auto_config(c)
        names = [c.get('name') for c in configs]

    # Persist so Vercel/cron and restarts keep treating auto as ON
    set_cron_state(True)
    pilot = None
    if not IS_VERCEL:
        pilot = start_auto_pilot()
    msg = f"Started pipeline(s): {', '.join(names)} · cron enabled in DB"
    if IS_VERCEL:
        msg += " · use GET /api/cron/auto-run on a schedule"
    elif pilot and not pilot.get('success'):
        msg += f" · pilot: {pilot.get('message')}"
    return {
        "success": True,
        "message": msg,
        "pipelines": names,
        "cron_enabled": True,
    }


def tool_auto_stop(name=None):
    global _auto_running
    configs = _list_auto_configs()
    if name:
        resolved = _resolve_pipeline_name(name)
        if not resolved:
            return {"success": False, "error": f"Pipeline '{name}' not found"}
        cfg = _load_auto_config(resolved)
        cfg['enabled'] = False
        _save_auto_config(cfg)
        remaining = [c for c in _list_auto_configs() if c.get('enabled')]
        if not remaining:
            _auto_running = False
            set_cron_state(False)
        return {"success": True, "message": f"Stopped pipeline '{resolved}'"}
    for c in configs:
        c['enabled'] = False
        _save_auto_config(c)
    _auto_running = False
    set_cron_state(False)
    return {"success": True, "message": "All pipelines stopped"}


def tool_auto_remove(name):
    if not name:
        return {"success": False, "error": "name required"}
    resolved = _resolve_pipeline_name(name) or name
    try:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "DB unavailable"}
        cur = conn.cursor()
        cur.execute("DELETE FROM auto_seen WHERE config_name = %s", (resolved,))
        cur.execute("DELETE FROM platform_mappings WHERE config_name = %s", (resolved,))
        cur.execute("DELETE FROM auto_config WHERE name = %s", (resolved,))
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        if deleted:
            return {"success": True, "message": f"Removed pipeline '{resolved}'"}
        return {"success": False, "error": f"Pipeline '{name}' not found"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_auto_status():
    configs = _list_auto_configs()
    pipelines = []
    for c in configs:
        pipelines.append({
            "name": c.get('name'),
            "enabled": bool(c.get('enabled')),
            "source_handle": c.get('source_handle'),
            "source_handles": c.get('source_handles') or [],
            "account_username": c.get('account_username'),
            "account_id": c.get('account_id'),
            "poll_interval_sec": c.get('poll_interval_sec'),
            "max_posts_per_run": c.get('max_posts_per_run'),
            "last_run_at": str(c.get('last_run_at')) if c.get('last_run_at') else None,
            "last_result": c.get('last_result'),
            "last_error": c.get('last_error'),
            "niche": c.get('niche'),
        })
    enabled = [p for p in pipelines if p['enabled']]
    cron_on = get_cron_state()
    # "running" = pipelines enabled + (background thread OR cron flag in DB)
    # so Start on a pipeline shows ON even on Vercel where only cron ticks
    is_on = bool(enabled) and (bool(_auto_running) or cron_on)
    return {
        "success": True,
        "running": is_on,
        "cron_enabled": cron_on,
        "pipelines": pipelines,
        "enabled_count": len(enabled),
        "message": (
            f"Auto {'ON' if is_on else 'idle'} · "
            f"{len(enabled)}/{len(pipelines)} enabled · cron={'yes' if cron_on else 'no'}"
        ),
    }


def _seen_uri(config_name, uri):
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM auto_seen WHERE config_name = %s AND uri = %s",
            (config_name, uri),
        )
        found = cur.fetchone() is not None
        cur.close()
        conn.close()
        return found
    except Exception:
        return False


def _mark_seen(config_name, uri, posted=False):
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO auto_seen (config_name, uri, posted)
            VALUES (%s, %s, %s)
            ON CONFLICT (config_name, uri) DO UPDATE SET posted = EXCLUDED.posted
        """, (config_name, uri, posted))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"mark seen: {e}")


def _run_one_pipeline(cfg):
    """
    Instagram-style cycle:
      1) Check Bluesky sources for NEW posts → vault → post them
      2) If not enough new posts, fill the rest from niche vault reserve
    Cron/enabled state is persisted in DB (app_settings + auto_config.enabled).
    """
    name = cfg.get('name') or 'default'
    sources = cfg.get('source_handles') or []
    if not sources and cfg.get('source_handle'):
        sources = [cfg['source_handle']]
    if not sources:
        return {"success": False, "error": "No source handles"}

    max_posts = int(cfg.get('max_posts_per_run') or 2)
    media_only = bool(cfg.get('media_only', True))
    include_reposts = bool(cfg.get('include_reposts', False))
    account_username = cfg.get('account_username')
    account_id = cfg.get('account_id')
    content_type = cfg.get('content_type') or 'feed'
    can_post = bool(account_username or account_id)

    # Debug logging
    print(f"\n{'='*60}")
    print(f"🔍 PIPELINE DEBUG: {name}")
    print(f"   Enabled: {cfg.get('enabled')}")
    print(f"   Sources: {sources}")
    print(f"   Account: {account_username or account_id or 'NONE'}")
    print(f"   Max posts: {max_posts}")
    print(f"   Media only: {media_only}")
    print(f"   Can post: {can_post}")
    print(f"{'='*60}")

    if not can_post:
        print(f"⚠️ Pipeline {name}: no FB destination — fetch only into vault")

    # ---------- 1) NEW posts from Bluesky ----------
    posted_new = 0
    fetched_posts = []
    client, _meta = _get_any_bluesky_client()
    if client:
        for src in sources:
            try:
                actor = src.lstrip('@')
                if '.' not in actor:
                    actor = actor + '.bsky.social'
                print(f"   📡 Fetching from @{actor}")
                for p in _raw_get_author_feed(client, actor, limit=40, max_pages=3):
                    if not include_reposts and p.get('is_repost'):
                        continue
                    uri = p.get('uri')
                    if not uri or _seen_uri(name, uri):
                        continue
                    images = p.get('images') or []
                    if media_only and not images:
                        continue
                    fetched_posts.append({
                        "uri": uri,
                        "author": p.get('author') or actor,
                        "display_name": p.get('display_name'),
                        "text": p.get('text') or '',
                        "images": images,
                        "likes": p.get('likes') or 0,
                        "reposts": p.get('reposts') or 0,
                        "replies": p.get('replies') or 0,
                        "created_at": p.get('created_at'),
                    })
            except Exception as e:
                print(f"   ❌ fetch {src}: {e}")

        print(f"   📊 Fetched {len(fetched_posts)} new posts")

        if fetched_posts:
            tool_add_to_vault(fetched_posts, handler_handle=name)

        if can_post:
            for p in fetched_posts[:max_posts]:
                print(f"   📤 Attempting new post: {p.get('uri')[:50]}...")
                _mark_seen(name, p['uri'], posted=False)
                try:
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("SELECT id FROM vault WHERE uri = %s", (p['uri'],))
                    row = cur.fetchone()
                    cur.close()
                    conn.close()
                    vid = row[0] if row else None
                except Exception:
                    vid = None
                if vid:
                    res = tool_post_now(
                        vault_id=vid,
                        account_username=account_username,
                        account_id=account_id,
                        content_type=content_type,
                    )
                    print(f"      Result: success={res.get('success')}")
                    if not res.get('success'):
                        print(f"      Error: {res.get('error') or res.get('message')}")
                    if res.get('success'):
                        posted_new += 1
                        _mark_seen(name, p['uri'], posted=True)
                    else:
                        # Mark as seen with posted=False to avoid immediate retry
                        _mark_seen(name, p['uri'], posted=False)
                else:
                    print(f"      ⚠️ Vault item not found for URI")
        else:
            for p in fetched_posts:
                if p.get('uri'):
                    _mark_seen(name, p['uri'], posted=False)
    else:
        print(f"⚠️ Pipeline {name}: no Bluesky session — will use vault reserve only")

    # ---------- 2) Fill from vault reserve if not enough new ----------
    posted_from_reserve = 0
    remaining = max(0, max_posts - posted_new)
    print(f"   📊 New posts: {posted_new}, Remaining slots: {remaining}")

    if can_post and remaining > 0:
        print(f"   🔍 Checking reserve for {name} (need {remaining} posts)")
        try:
            conn = get_db_connection()
            if conn:
                cur = conn.cursor(cursor_factory=RealDictCursor)

                # Fetch MORE posts than needed (3x) to handle duplicates/failures
                fetch_limit = remaining * 3
                print(f"   🔍 Fetching up to {fetch_limit} reserve posts (3x needed)")

                # UPDATED RESERVE QUERY - Skips external links, recently failed
                # posts, AND permanently-duplicate posts (409 from Zernio).
                cur.execute("""
                    SELECT v.* FROM vault v
                    WHERE v.handler_handle = %s
                    AND NOT EXISTS (
                        SELECT 1 FROM posted_posts p
                        WHERE p.uri = v.uri AND p.platform = 'facebook'
                          AND p.status IN ('completed', 'posted', 'duplicate')
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM auto_seen s
                        WHERE s.config_name = %s AND s.uri = v.uri 
                        AND s.posted = FALSE 
                        AND s.seen_at > NOW() - INTERVAL '10 minutes'
                    )
                    -- Skip external links that aren't valid images
                    AND (
                        v.images IS NULL 
                        OR (
                            v.images::text NOT LIKE '%%youtube.com%%'
                            AND v.images::text NOT LIKE '%%facebook.com%%'
                            AND v.images::text NOT LIKE '%%reel%%'
                            AND v.images::text NOT LIKE '%%shorts%%'
                            AND v.images::text NOT LIKE '%%tiktok.com%%'
                            AND v.images::text NOT LIKE '%%instagram.com%%'
                            AND v.images::text NOT LIKE '%%twitter.com%%'
                            AND v.images::text NOT LIKE '%%x.com%%'
                            AND v.images::text NOT LIKE '%%vimeo.com%%'
                            AND v.images::text NOT LIKE '%%dailymotion.com%%'
                        )
                    )
                    ORDER BY v.saved_at ASC
                    LIMIT %s
                """, (name, name, fetch_limit))

                reserve = cur.fetchall()
                print(f"   📦 Found {len(reserve)} unposted posts in reserve")

                cur.close()
                conn.close()

                # Loop through reserve until we post enough or run out
                for item in reserve:
                    # Check if we've posted enough
                    if posted_from_reserve >= remaining:
                        print(f"   ✅ Reached target of {remaining} posts, stopping")
                        break

                    # Check if the image is actually a valid image URL
                    images = item.get('images') or []
                    if isinstance(images, str):
                        try:
                            images = json.loads(images)
                        except Exception:
                            images = []

                    image_url = None
                    if images:
                        first = images[0]
                        if isinstance(first, dict):
                            image_url = first.get('url')
                        else:
                            image_url = first

                    # Skip if it's an external link (double-check)
                    if image_url:
                        external_domains = ['youtube.com', 'facebook.com', 'reel', 'shorts', 
                                          'tiktok.com', 'instagram.com', 'twitter.com', 
                                          'x.com', 'vimeo.com', 'dailymotion.com']
                        if any(domain in image_url.lower() for domain in external_domains):
                            print(f"   ⏭️ Skipping external link: {image_url[:60]}...")
                            _mark_seen(name, item.get('uri'), posted=False)
                            continue

                    print(f"   📤 Attempting reserve post: vault #{item['id']} - {item.get('text', '')[:40]}...")

                    res = tool_post_now(
                        vault_id=item['id'],
                        account_username=account_username,
                        account_id=account_id,
                        content_type=content_type,
                    )

                    print(f"      Result: success={res.get('success')}")
                    if not res.get('success'):
                        print(f"      Error: {res.get('error') or res.get('message')}")

                    if res.get('success'):
                        posted_from_reserve += 1
                        _mark_seen(name, item.get('uri'), posted=True)
                        print(f"   ✅ Posted {posted_from_reserve}/{remaining} reserve posts")
                    else:
                        # Mark as seen with posted=False so it won't be retried immediately
                        _mark_seen(name, item.get('uri'), posted=False)
                        print(f"   ⏭️ Skipping failed post, continuing to next")

                print(f"   📊 Final reserve posts posted: {posted_from_reserve}/{remaining}")
        except Exception as e:
            print(f"   ❌ Reserve query error: {e}")
            traceback.print_exc()

    total = posted_new + posted_from_reserve

    # Summary
    print(f"\n📊 Pipeline {name} summary:")
    print(f"   New posts: {posted_new}")
    print(f"   Reserve posts: {posted_from_reserve}")
    print(f"   Total: {total}/{max_posts}")
    print(f"{'='*60}\n")

    if can_post:
        msg = f"Posted {total} (new={posted_new}, reserve={posted_from_reserve})"
        cfg['last_error'] = None
    else:
        msg = f"Fetched {len(fetched_posts)} into vault (no destination)"
        cfg['last_error'] = None

    cfg['last_result'] = msg
    cfg['last_run_at'] = datetime.now()
    _save_auto_config(cfg)

    print(f"🤖 Pipeline {name}: {msg}")
    return {"success": True, "posted": total, "message": msg}


def tool_auto_run_now(name=None):
    """Run one named pipeline, or every enabled pipeline (cron / auto-run)."""
    results = []
    if name:
        resolved = _resolve_pipeline_name(name) or name
        cfg = _load_auto_config(resolved)
        if not cfg:
            return {"success": False, "error": f"Pipeline '{name}' not found"}
        r = _run_one_pipeline(cfg)
        results.append({"name": resolved, **r})
    else:
        for cfg in _list_auto_configs():
            if not cfg.get('enabled'):
                continue
            try:
                r = _run_one_pipeline(cfg)
            except Exception as e:
                r = {"success": False, "error": str(e), "posted": 0}
            results.append({"name": cfg.get('name'), **r})
    if not results and name is None:
        return {
            "success": True,
            "results": [],
            "posted_count": 0,
            "message": "No enabled pipelines — say: start pipeline <name>",
        }
    total_posted = sum(int(r.get('posted') or 0) for r in results if r.get('success'))
    return {
        "success": True,
        "results": results,
        "posted_count": total_posted,
        "message": f"Ran {len(results)} pipeline(s), posted {total_posted}",
    }


def _auto_loop():
    global _auto_running
    print("🤖 Auto pilot loop started (Facebook)")
    while _auto_running:
        try:
            if not get_cron_state():
                time.sleep(15)
                continue
            configs = [c for c in _list_auto_configs() if c.get('enabled')]
            if not configs:
                time.sleep(20)
                continue
            min_interval = min(int(c.get('poll_interval_sec') or 300) for c in configs)
            for cfg in configs:
                try:
                    _run_one_pipeline(cfg)
                except Exception as e:
                    print(f"Pipeline {cfg.get('name')} error: {e}")
                    cfg['last_error'] = str(e)
                    _save_auto_config(cfg)
            time.sleep(max(60, min_interval))
        except Exception as e:
            print(f"Auto loop error: {e}")
            time.sleep(30)
    print("🤖 Auto pilot loop stopped")


def start_auto_pilot():
    global _auto_thread, _auto_running
    with _auto_lock:
        if IS_VERCEL:
            return {"success": False, "message": "Background pilot disabled on Vercel; use cron endpoint"}
        if _auto_running and _auto_thread and _auto_thread.is_alive():
            return {"success": True, "message": "Already running"}
        _auto_running = True
        _auto_thread = threading.Thread(target=_auto_loop, daemon=True)
        _auto_thread.start()
        return {"success": True, "message": "Auto pilot started"}


# ============================================================
# MASTER FETCH (reserve)
# ============================================================

def _vault_known_uris(handler_handle=None):
    """URIs already in vault (optionally for one pipeline). Used to skip on refill."""
    known = set()
    try:
        conn = get_db_connection()
        if not conn:
            return known
        cur = conn.cursor()
        if handler_handle:
            cur.execute("SELECT uri FROM vault WHERE handler_handle = %s", (handler_handle,))
        else:
            cur.execute("SELECT uri FROM vault")
        for row in cur.fetchall():
            if row and row[0]:
                known.add(row[0])
        cur.close()
        conn.close()
    except Exception as e:
        print(f"vault known uris: {e}")
    return known


def tool_master_fetch_niche(name=None, limit_per_source=None, max_pages=None):
    """
    Fill niche reserve from Bluesky sources.

    Each click aims for `limit_per_source` **NEW** media posts (not already in vault).
    Defaults: MASTER_FETCH_LIMIT (env or 50), MASTER_FETCH_MAX_PAGES (env or 10).
    """
    if not name:
        return {"success": False, "error": "name required"}
    cfg = _load_auto_config(name)
    if not cfg:
        return {"success": False, "error": f"Pipeline '{name}' not found"}
    sources = cfg.get('source_handles') or []
    if not sources and cfg.get('source_handle'):
        sources = [cfg['source_handle']]
    if not sources:
        return {"success": False, "error": "No source handles on this pipeline"}

    client, _ = _get_any_bluesky_client()
    if not client:
        return {"success": False, "error": "No Bluesky session / master account"}

    # Per-click NEW posts target (ignore ones we already have)
    target_new = max(5, int(limit_per_source if limit_per_source is not None else MASTER_FETCH_LIMIT))
    # Always allow a high page budget so cursor can run until target is met
    pages = max(
        15,
        min(int(max_pages if max_pages is not None else MASTER_FETCH_MAX_PAGES), 100),
    )
    media_only = bool(cfg.get('media_only', True))

    known = _vault_known_uris(handler_handle=None)
    print(
        f"📦 vault already has {len(known)} URIs — filling «{name}» "
        f"until +{target_new} NEW media/source (max {pages} pages)"
    )

    all_posts = []
    per_source = {}

    for src in sources:
        actor = src.lstrip('@')
        if '.' not in actor:
            actor = actor + '.bsky.social'

        new_for_source = 0
        skipped_known = 0
        skipped_no_media = 0
        scanned = 0
        cursor = None
        page_i = 0

        try:
            # Keep following cursor until we have enough NEW posts or feed ends
            while new_for_source < target_new and page_i < pages:
                page_i += 1
                try:
                    batch, cursor = _http_get_author_feed_page(
                        client, actor, limit=100, cursor=cursor
                    )
                except Exception as e:
                    print(f"master-fetch page {page_i} @{actor}: {e}")
                    break

                if not batch:
                    print(f"📄 @{actor} page {page_i}: empty — stop")
                    break

                scanned += len(batch)
                page_media = sum(1 for p in batch if p.get('images'))
                print(
                    f"📄 @{actor} page {page_i}: +{len(batch)} "
                    f"(media={page_media}, new {new_for_source}/{target_new}, "
                    f"known {skipped_known}, no_media {skipped_no_media}, "
                    f"cursor={'yes' if cursor else 'end'})"
                )

                for p in batch:
                    uri = p.get('uri')
                    if not uri:
                        continue
                    images = p.get('images') or []
                    if media_only and not images:
                        skipped_no_media += 1
                        continue
                    if uri in known:
                        skipped_known += 1
                        continue
                    known.add(uri)
                    all_posts.append({
                        "uri": uri,
                        "author": p.get('author') or actor,
                        "display_name": p.get('display_name'),
                        "text": p.get('text') or '',
                        "images": images,
                        "likes": p.get('likes') or 0,
                        "reposts": p.get('reposts') or 0,
                        "replies": p.get('replies') or 0,
                        "created_at": p.get('created_at'),
                    })
                    new_for_source += 1
                    if new_for_source >= target_new:
                        break

                if not cursor:
                    print(f"📄 @{actor}: no more cursor after page {page_i}")
                    break

            per_source[actor] = {
                "new": new_for_source,
                "skipped_known": skipped_known,
                "skipped_no_media": skipped_no_media,
                "scanned": scanned,
                "pages": page_i,
            }
            print(
                f"📥 master-fetch @{actor}: {new_for_source}/{target_new} NEW media "
                f"over {page_i} pages (scanned {scanned}, known {skipped_known}, no_media {skipped_no_media})"
            )
            if new_for_source < target_new:
                print(
                    f"⚠️ @{actor}: only {new_for_source}/{target_new} new — "
                    f"feed exhausted or all remaining media already in vault"
                )
        except Exception as e:
            print(f"master fetch {src}: {e}")
            per_source[actor] = {"new": 0, "error": str(e)}

    result = tool_add_to_vault(all_posts, handler_handle=name, retag=False)
    detail_parts = []
    for a, info in per_source.items():
        if isinstance(info, dict):
            detail_parts.append(f"@{a}: +{info.get('new', 0)} new")
        else:
            detail_parts.append(f"@{a}: {info}")
    detail = ", ".join(detail_parts)
    saved = result.get('saved', 0)
    skipped = result.get('skipped', 0)
    return {
        "success": True,
        "saved": saved,
        "skipped": skipped,
        "new_candidates": len(all_posts),
        "per_source": per_source,
        "message": (
            f"Reserve for '{name}': +{saved} new "
            f"(dup insert skips {skipped}) · {detail}"
        ),
    }


def tool_master_fetch_all_niches(limit_per_source=None, max_pages=None):
    configs = _list_auto_configs()
    if not configs:
        return {"success": False, "error": "No pipelines configured"}
    limit_per_source = int(limit_per_source if limit_per_source is not None else MASTER_FETCH_LIMIT)
    max_pages = int(max_pages if max_pages is not None else MASTER_FETCH_MAX_PAGES)
    total_saved = 0
    parts = []
    for c in configs:
        r = tool_master_fetch_niche(
            name=c.get('name'),
            limit_per_source=limit_per_source,
            max_pages=max_pages,
        )
        if r.get('success'):
            total_saved += int(r.get('saved') or 0)
            parts.append(f"{c.get('name')}: +{r.get('saved', 0)}")
        else:
            parts.append(f"{c.get('name')}: {r.get('error')}")
    return {
        "success": True,
        "saved": total_saved,
        "message": f"All niches: saved {total_saved}. " + "; ".join(parts),
    }


def tool_add_source_to_niche(niche, source):
    configs = _list_auto_configs()
    target = None
    for c in configs:
        if (c.get('name') or '').lower() == niche.lower() or (c.get('niche') or '').lower() == niche.lower():
            target = c
            break
    if not target:
        return {"success": False, "error": f"Niche/pipeline '{niche}' not found"}
    source = source.strip().lstrip('@')
    if '.' not in source:
        source = source + '.bsky.social'
    handles = list(target.get('source_handles') or [])
    if source not in handles:
        handles.append(source)
    target['source_handles'] = handles
    target['source_handle'] = handles[0]
    _save_auto_config(target)
    return {"success": True, "message": f"Added @{source} to {target.get('name')}"}


def tool_remove_source_from_niche(niche, source):
    configs = _list_auto_configs()
    target = None
    for c in configs:
        if (c.get('name') or '').lower() == niche.lower() or (c.get('niche') or '').lower() == niche.lower():
            target = c
            break
    if not target:
        return {"success": False, "error": f"Niche '{niche}' not found"}
    source = source.strip().lstrip('@')
    handles = [h for h in (target.get('source_handles') or []) if h.lstrip('@') != source and h != source + '.bsky.social']
    target['source_handles'] = handles
    target['source_handle'] = handles[0] if handles else None
    _save_auto_config(target)
    return {"success": True, "message": f"Removed source from {target.get('name')}"}


def tool_list_niche_sources(niche):
    configs = _list_auto_configs()
    for c in configs:
        if (c.get('name') or '').lower() == niche.lower() or (c.get('niche') or '').lower() == niche.lower():
            srcs = c.get('source_handles') or []
            return {
                "success": True,
                "sources": srcs,
                "message": f"{c.get('name')}: " + (", ".join(f"@{s}" for s in srcs) or "none"),
            }
    return {"success": False, "error": f"Niche '{niche}' not found"}


def tool_list_all_niches():
    configs = _list_auto_configs()
    lines = []
    for c in configs:
        srcs = c.get('source_handles') or []
        lines.append(
            f"• {c.get('name')}: {'🟢' if c.get('enabled') else '🔴'} "
            f"{len(srcs)} source(s) → @{c.get('account_username') or '?'}"
        )
    return {
        "success": True,
        "niches": configs,
        "message": "Niches:\n" + ("\n".join(lines) if lines else "None"),
    }


# ============================================================
# TOOLS SCHEMA + EXECUTE
# ============================================================

TOOLS_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "login",
            "description": "Login to Bluesky with handle and app password",
            "parameters": {
                "type": "object",
                "properties": {
                    "username": {"type": "string"},
                    "password": {"type": "string"},
                },
                "required": ["username", "password"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "fetch_posts",
            "description": "Fetch posts from a Bluesky handle",
            "parameters": {
                "type": "object",
                "properties": {
                    "actor": {"type": "string"},
                    "limit": {"type": "integer", "default": 15},
                    "media_only": {"type": "boolean", "default": True},
                    "include_reposts": {"type": "boolean", "default": False},
                },
                "required": ["actor"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_to_vault",
            "description": "Save last fetched posts to vault",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_vault",
            "description": "List vault items. Pass pipeline to filter one niche reserve (e.g. wildlife).",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "default": 15},
                    "pipeline": {
                        "type": "string",
                        "description": "Pipeline/niche name, e.g. wildlife",
                    },
                    "handler_handle": {
                        "type": "string",
                        "description": "Same as pipeline — vault handler_handle filter",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_vault_by_status",
            "description": "List vault by status (unposted/posted/scheduled/all). Optional pipeline filters that niche reserve.",
            "parameters": {
                "type": "object",
                "properties": {
                    "status": {"type": "string", "enum": ["unposted", "posted", "scheduled", "all"]},
                    "limit": {"type": "integer", "default": 50},
                    "pipeline": {
                        "type": "string",
                        "description": "Pipeline/niche name e.g. wildlife",
                    },
                    "handler_handle": {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "delete_vault_items",
            "description": "Delete vault items by ids, status, or all (confirm YES_DELETE_ALL)",
            "parameters": {
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "ids": {"type": "array", "items": {"type": "integer"}},
                    "all": {"type": "boolean"},
                    "confirm": {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "post_now",
            "description": "Post a vault item to Facebook now",
            "parameters": {
                "type": "object",
                "properties": {
                    "vault_id": {"type": "integer"},
                    "uri": {"type": "string"},
                    "caption": {"type": "string"},
                    "account_username": {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "post_vault_batch",
            "description": "Post multiple unposted vault items to Facebook",
            "parameters": {
                "type": "object",
                "properties": {
                    "count": {"type": "integer", "default": 3},
                    "account_username": {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "post_unposted",
            "description": "Post all unposted vault items to Facebook",
            "parameters": {
                "type": "object",
                "properties": {
                    "account_username": {"type": "string"},
                    "limit": {"type": "integer", "default": 10},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_accounts",
            "description": "List connected Facebook accounts",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "remove_facebook_account",
            "description": "Remove a Facebook account from this app DB (soft deactivate by default). Match by display name or username e.g. Daily Wisdom.",
            "parameters": {
                "type": "object",
                "properties": {
                    "username": {
                        "type": "string",
                        "description": "Username or display name e.g. Daily Wisdom",
                    },
                    "account_id": {"type": "string"},
                    "permanent": {
                        "type": "boolean",
                        "description": "If true, DELETE row; default false only sets is_active=false",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_status",
            "description": "Vault / posted / accounts status",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_scheduled",
            "description": "List scheduled Facebook posts",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "auto_setup",
            "description": "Configure auto pipeline. Facebook account is OPTIONAL — can set destination later with auto_set_destination.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Pipeline name e.g. Lifestyle"},
                    "source_handle": {"type": "string"},
                    "source_handles": {"type": "array", "items": {"type": "string"}},
                    "account_username": {
                        "type": "string",
                        "description": "Optional Facebook username; omit to set later",
                    },
                    "poll_interval_sec": {"type": "integer"},
                    "max_posts_per_run": {"type": "integer"},
                    "enabled": {"type": "boolean"},
                },
                "required": ["source_handle"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "auto_set_destination",
            "description": "Set or change Facebook destination for an existing pipeline",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Pipeline name"},
                    "account_username": {"type": "string"},
                    "account_id": {"type": "string"},
                },
                "required": ["name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "auto_start",
            "description": "Start auto pilot (optional pipeline name)",
            "parameters": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "auto_stop",
            "description": "Stop auto pilot (optional pipeline name)",
            "parameters": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "auto_status",
            "description": "Auto pilot status and pipelines",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "auto_run_now",
            "description": "Run one auto cycle now",
            "parameters": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "auto_remove",
            "description": "Delete a pipeline by name",
            "parameters": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_zernio_key",
            "description": "Validate Zernio API key and list Facebook accounts",
            "parameters": {
                "type": "object",
                "properties": {"api_key": {"type": "string"}},
                "required": ["api_key"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_api_keys",
            "description": "List configured Zernio API keys",
            "parameters": {"type": "object", "properties": {}},
        },
    },
]


def execute_tool(name, args, session_id=None):
    args = args or {}
    try:
        if name == 'login':
            return tool_login(args.get('username'), args.get('password'))
        if name == 'fetch_posts':
            return tool_fetch_posts(
                session_id,
                args.get('actor'),
                limit=int(args.get('limit') or 15),
                media_only=bool(args.get('media_only', True)),
                include_reposts=bool(args.get('include_reposts', False)),
            )
        if name == 'add_to_vault':
            posts = []
            if session_id and session_id in sessions:
                posts = sessions[session_id].get('_last_fetched') or []
            return tool_add_to_vault(
                posts,
                handler_handle=sessions.get(session_id, {}).get('_last_actor'),
            )
        if name == 'list_vault':
            return tool_list_vault(
                limit=int(args.get('limit') or 15),
                pipeline=args.get('pipeline'),
                handler_handle=args.get('handler_handle'),
            )
        if name == 'list_vault_by_status':
            return tool_list_vault_by_status(
                status=args.get('status', 'all'),
                limit=int(args.get('limit', 50)),
                pipeline=args.get('pipeline'),
                handler_handle=args.get('handler_handle'),
            )
        if name == 'delete_vault_items':
            if args.get('all') and args.get('confirm') != 'YES_DELETE_ALL':
                return {
                    "success": False,
                    "error": "Confirmation required",
                    "message": "⚠️ Reply with confirm YES_DELETE_ALL to wipe vault.",
                }
            return tool_delete_vault_items(
                ids=args.get('ids'),
                status=args.get('status'),
                all=args.get('all', False),
            )
        if name == 'post_now':
            return tool_post_now(
                vault_id=args.get('vault_id'),
                uri=args.get('uri'),
                caption=args.get('caption'),
                account_username=args.get('account_username'),
                account_id=args.get('account_id'),
            )
        if name == 'post_vault_batch':
            return tool_post_vault_batch(
                count=args.get('count', 3),
                account_username=args.get('account_username'),
                account_id=args.get('account_id'),
            )
        if name == 'post_unposted':
            return tool_post_unposted(
                account_username=args.get('account_username'),
                limit=int(args.get('limit', 10)),
            )
        if name == 'list_accounts':
            return tool_list_accounts('facebook')
        if name == 'remove_facebook_account':
            return tool_remove_facebook_account(
                username=args.get('username') or args.get('name') or args.get('display_name'),
                account_id=args.get('account_id'),
                permanent=bool(args.get('permanent', False)),
            )
        if name == 'get_status':
            return tool_get_status()
        if name == 'list_scheduled':
            return tool_list_scheduled()
        if name == 'auto_setup':
            return tool_auto_setup(
                name=args.get('name') or 'default',
                source_handle=args.get('source_handle'),
                source_handles=args.get('source_handles'),
                account_username=args.get('account_username'),
                account_id=args.get('account_id'),
                poll_interval_sec=args.get('poll_interval_sec') or 300,
                max_posts_per_run=args.get('max_posts_per_run') or 2,
                enabled=bool(args.get('enabled', False)),
            )
        if name == 'auto_set_destination':
            return tool_auto_set_destination(
                name=args.get('name'),
                account_username=args.get('account_username'),
                account_id=args.get('account_id'),
            )
        if name == 'auto_start':
            return tool_auto_start(args.get('name'))
        if name == 'auto_stop':
            return tool_auto_stop(args.get('name'))
        if name == 'auto_status':
            return tool_auto_status()
        if name == 'auto_run_now':
            return tool_auto_run_now(args.get('name'))
        if name == 'auto_remove':
            return tool_auto_remove(args.get('name'))
        if name == 'check_zernio_key':
            return tool_check_zernio_key(args.get('api_key'))
        if name == 'list_api_keys':
            return tool_list_api_keys()
        return {"success": False, "error": f"Unknown tool {name}"}
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "error": str(e)}


SYSTEM_PROMPT = """You are the AI for Bluesky AI Vault → Facebook.

Source: Bluesky. Destination: Facebook only (via Zernio). Never mention Instagram or other networks.

You can:
- Login to Bluesky, fetch posts, save to vault
- Post vault items to Facebook (ask which account if multiple)
- Auto pipelines: named configs watching Bluesky → Facebook
- list pipelines / start pipeline NAME / stop pipeline NAME / auto status
- Master-fetch fills a pipeline's vault reserve

PIPELINE SETUP:
- Facebook destination is OPTIONAL at creation. If the user says "set destination later" / "leave destination", call auto_setup with only name + source_handle (no account_username).
- Example: name="Lifestyle", source_handle="sundaedivine.lol"
- Later: auto_set_destination(name="Lifestyle", account_username="...")
- Never refuse setup just because Facebook is missing.

VAULT / RESERVE:
- "list vault" → list_vault()
- "show Health vault" → list_vault(pipeline="Health") or list_vault_by_status(pipeline="Health")
- "how many unposted in Health" / "posts from Health not yet posted" → list_vault_by_status(status="unposted", pipeline="Health")
- Always pass pipeline when the user names a niche (Health, Family, Lifestyle, Sport, wildlife).
- Reply with the **count** number clearly when asked "how many".

ACCOUNTS:
- "list accounts" / "see facebook accounts" → list_accounts
- "remove facebook account Daily Wisdom" → remove_facebook_account(username="Daily Wisdom")
- Soft-removes from this app DB (is_active=false). Does not disconnect the page in Zernio.

Be concise. Timezone for schedules: Africa/Nairobi (GMT+3).
"""


def format_tool_summary(tool_results):
    parts = []
    for tr in tool_results:
        name = tr.get('name')
        r = tr.get('result') or {}
        if not r.get('success'):
            parts.append(f"❌ {name}: {r.get('error') or r.get('message') or 'failed'}")
            continue
        if r.get('message'):
            parts.append(r['message'])
        else:
            parts.append(f"✅ {name} completed")
    return "\n".join(parts) if parts else "Done."


def simple_fallback(msg, session_id):
    lower = msg.lower().strip()

    if lower.startswith('login ') or 'login with' in lower:
        m = re.search(r'login(?:\s+with)?\s+([^\s]+)\s+(?:and\s+)?(.+)', msg.strip(), re.I)
        if m:
            result = tool_login(m.group(1).strip().rstrip(','), m.group(2).strip().rstrip('.,!'))
            if result.get('success'):
                return f"✅ {result.get('message')}\nSession ID: {result.get('session_id')}"
            return f"❌ Login failed: {result.get('error')}"
        return "Format: Login with <handle> and <app-password>"

    sk = re.search(r'(sk_[A-Za-z0-9]{20,})', msg)
    if sk:
        return tool_check_zernio_key(api_key=sk.group(1)).get('message')

    if any(w in lower for w in ('api key', 'api keys', 'zernio key', 'zernio keys')):
        return tool_list_api_keys().get('message') or str(tool_list_api_keys())

    # --- Pipeline / niche vault (must run BEFORE generic "how many" status) ---
    pipe = None
    for c in _list_auto_configs():
        n = (c.get('name') or '')
        if n and n.lower() in lower:
            pipe = n
            break
    if not pipe:
        m = re.search(
            r'(?:in|for|of|from)\s+(?:the\s+)?([a-zA-Z0-9._-]+)\s+(?:vault|reserve|pipeline|niche)',
            msg, re.I,
        )
        if m:
            pipe = m.group(1)
        else:
            m2 = re.search(
                r'(?:vault|reserve|pipeline|niche)\s+(?:for\s+|named\s+)?([a-zA-Z0-9._-]+)',
                msg, re.I,
            )
            if m2:
                pipe = m2.group(1)

    wants_unposted = any(
        w in lower
        for w in (
            'unposted',
            'not yet posted',
            'not posted',
            "haven't posted",
            'have not posted',
            'not yet',
            'remaining',
            'still to post',
            'to post',
        )
    )
    wants_list = any(
        w in lower
        for w in ('list', 'show', 'what', 'see', 'posts', 'how many', 'count', "what's in")
    )

    if pipe and (wants_list or wants_unposted or 'vault' in lower or 'reserve' in lower):
        status = 'unposted' if wants_unposted else 'all'
        if 'posted' in lower and not wants_unposted and 'unposted' not in lower:
            status = 'posted'
        r = tool_list_vault_by_status(status=status, limit=20, pipeline=pipe)
        # For pure "how many" questions, lead with the number
        if 'how many' in lower or 'count' in lower:
            total = r.get('count', 0)
            return (
                f"📊 **{pipe}** — {status}: **{total}** posts\n\n"
                + (r.get('message') or '')
            )
        return r.get('message') or r.get('error') or str(r)

    if any(w in lower for w in ('reserve', 'wildlife')) or (
        ('vault' in lower or 'unposted' in lower)
        and any(w in lower for w in ('list', 'show', 'what', 'see', 'posts in'))
    ):
        status = 'unposted' if wants_unposted or 'unposted' in lower else 'all'
        r = tool_list_vault_by_status(status=status, limit=15, pipeline=pipe)
        return r.get('message') or r.get('error') or str(r)

    if any(w in lower for w in ('status', 'how many', "what's in", 'counts')) and 'auto' not in lower and 'cron' not in lower:
        r = tool_get_status()
        return r.get('message', str(r))

    if 'vault' in lower and any(w in lower for w in ('list', 'show', 'what')):
        r = tool_list_vault(limit=10)
        return r.get('message') or r.get('error') or str(r)

    if 'account' in lower and 'api' not in lower:
        r = tool_list_accounts('facebook')
        accs = r.get('accounts') or []
        if not accs:
            return "No Facebook accounts. Connect one in Zernio."
        return "Facebook accounts:\n" + "\n".join(f"• @{a.get('label')}" for a in accs)

    if any(w in lower for w in ('list pipelines', 'show pipelines', 'pipelines')):
        configs = _list_auto_configs()
        if not configs:
            return "No pipelines configured."
        lines = ["📋 Pipelines:"]
        for c in configs:
            status = "🟢" if c.get('enabled') else "🔴"
            lines.append(
                f"  {status} {c.get('name')}: @{c.get('source_handle')} → @{c.get('account_username')} · {c.get('last_result') or 'never'}"
            )
        return "\n".join(lines)

    if any(w in lower for w in ('auto status', 'pipeline status', 'status all')):
        return tool_auto_status().get('message', str(tool_auto_status()))

    if any(w in lower for w in ('cron status', 'is cron')):
        on = get_cron_state()
        return f"Cron: {'🟢 ENABLED' if on else '🔴 DISABLED'}"

    if any(w in lower for w in ('start pipeline', 'enable pipeline')):
        m = re.search(r'(?:pipeline\s+)?([a-zA-Z0-9._-]+)', msg)
        name = m.group(1) if m else None
        if name and name.lower() not in ('pipeline', 'start', 'enable'):
            return tool_auto_start(name=name).get('message')
        return "Say: start pipeline <name>"

    if any(w in lower for w in ('stop pipeline', 'disable pipeline')):
        m = re.search(r'(?:pipeline\s+)?([a-zA-Z0-9._-]+)', msg)
        name = m.group(1) if m else None
        if name and name.lower() not in ('pipeline', 'stop', 'disable'):
            return tool_auto_stop(name=name).get('message')
        return "Say: stop pipeline <name>"

    if any(w in lower for w in ('run pipeline', 'run auto', 'auto run')):
        m = re.search(r'(?:pipeline\s+)?([a-zA-Z0-9._-]+)', msg)
        name = m.group(1) if m and m.group(1).lower() not in ('pipeline', 'run', 'auto', 'now') else None
        return tool_auto_run_now(name=name).get('message', str(tool_auto_run_now(name=name)))

    if any(w in lower for w in ('remove pipeline', 'delete pipeline')):
        m = re.search(r'(?:remove|delete)\s+(?:pipeline|auto)\s+([a-zA-Z0-9._-]+)', msg, re.I)
        if m:
            return tool_auto_remove(m.group(1)).get('message')
        return "Say: Remove pipeline <name>"

    # Remove Facebook account from this app (not from Zernio)
    if any(w in lower for w in ('remove facebook', 'delete facebook', 'remove account', 'delete account')) or (
        ('remove' in lower or 'delete' in lower) and 'account' in lower
    ):
        m = re.search(
            r'(?:remove|delete)\s+(?:this\s+)?(?:facebook\s+)?(?:account\s+)?(.+)$',
            msg.strip(), re.I,
        )
        name_part = (m.group(1).strip() if m else '').strip(' "\'')
        # strip trailing words
        name_part = re.sub(r'\s+(account|from\s+here|please)\s*$', '', name_part, flags=re.I).strip()
        if name_part and name_part.lower() not in ('account', 'facebook', 'the'):
            permanent = 'permanent' in lower or 'forever' in lower
            return tool_remove_facebook_account(username=name_part, permanent=permanent).get('message')
        return "Say: remove facebook account Daily Wisdom"

    if any(w in lower for w in ('stop auto', 'auto stop')):
        return tool_auto_stop().get('message')
    if any(w in lower for w in ('start auto', 'auto start', 'go autonomous')):
        return tool_auto_start().get('message')

    if 'fetch' in lower:
        m = re.search(r'@?([a-zA-Z0-9._-]+\.bsky\.social|[a-zA-Z0-9._-]+)', msg)
        limit_m = re.search(r'(\d+)\s*posts?', lower)
        limit = int(limit_m.group(1)) if limit_m else 15
        if not m:
            return "Say: Fetch 15 posts from @handle"
        actor = m.group(1)
        if '.' not in actor:
            actor = actor + '.bsky.social'
        r = tool_fetch_posts(session_id, actor, limit=limit)
        if not r.get('success'):
            return f"❌ {r.get('error')}"
        posts = r.get('posts') or []
        if session_id and session_id in sessions:
            sessions[session_id]['_last_fetched'] = posts
            sessions[session_id]['_last_actor'] = actor
        lines = [f"Fetched {len(posts)} from @{actor}:"]
        for i, p in enumerate(posts[:8], 1):
            media = f" [{len(p.get('images') or [])} img]" if p.get('images') else ""
            lines.append(f"{i}. {(p.get('text') or '')[:70]}{media}")
        lines.append("\nSay “save them to vault” to store them.")
        return "\n".join(lines)

    if any(w in lower for w in ('save', 'add to vault', 'vault them')):
        if not session_id or session_id not in sessions:
            return "Fetch posts first."
        posts = sessions[session_id].get('_last_fetched') or []
        if not posts:
            return "No recent fetch."
        return tool_add_to_vault(posts, handler_handle=sessions[session_id].get('_last_actor')).get('message')

    id_m = re.search(r'post\s+(?:id\s+)?(\d+)', lower)
    if id_m:
        return tool_post_now(vault_id=int(id_m.group(1))).get('message') or str(
            tool_post_now(vault_id=int(id_m.group(1)))
        )

    if 'post' in lower and ('facebook' in lower or 'vault' in lower or 'now' in lower):
        count_m = re.search(r'(\d+)\s*posts?', lower)
        count = int(count_m.group(1)) if count_m else 1
        if count > 1:
            return tool_post_vault_batch(count=count).get('message')
        r = tool_list_vault(limit=5)
        items = r.get('vault') or []
        if not items:
            return "No vault items. Fetch and save first."
        chosen = next((it for it in items if it.get('images')), items[0])
        return tool_post_now(vault_id=chosen.get('id')).get('message')

    # auto setup with optional destination
    m = re.search(
        r'auto\s+setup.*?watch\s+@?([a-zA-Z0-9._-]+).*?(?:post\s+to|to)\s+@?([a-zA-Z0-9._-]+)',
        msg, re.I,
    )
    if m:
        return tool_auto_setup(
            name='default',
            source_handle=m.group(1),
            account_username=m.group(2),
            enabled=False,
        ).get('message')

    # "pipeline Lifestyle source sundaedivine.lol" / "set up Lifestyle with sundaedivine"
    m2 = re.search(
        r'(?:pipeline|auto\s+setup|set\s*up)\s+([a-zA-Z0-9._-]+).*?(?:source|watch|from)\s+@?([a-zA-Z0-9._-]+(?:\.[a-zA-Z0-9._-]+)*)',
        msg, re.I,
    )
    if m2:
        return tool_auto_setup(
            name=m2.group(1),
            source_handle=m2.group(2),
            enabled=False,
        ).get('message')

    if 'auto setup' in lower and 'watch' in lower:
        return "Say: Auto setup watch @blueskyhandle  (Facebook optional)  or  pipeline Lifestyle source sundaedivine.lol"

    # set destination for existing pipeline
    m3 = re.search(
        r'(?:set\s+)?destination\s+(?:for\s+)?([a-zA-Z0-9._-]+)\s+(?:to\s+)?@?([a-zA-Z0-9._-]+)',
        msg, re.I,
    )
    if m3:
        return tool_auto_set_destination(name=m3.group(1), account_username=m3.group(2)).get('message')

    return (
        "Bluesky → Facebook vault.\n"
        "Examples:\n"
        "  Login with handle and app-password\n"
        "  Fetch 10 posts from @someone.bsky.social\n"
        "  Save them to vault · Post id 2\n"
        "  Auto setup watch zorrito post to MyPage every 5 minutes\n"
        "  start pipeline default · stop pipeline default · auto status · list pipelines"
    )


def _sanitize_reply_facebook_only(reply: str) -> str:
    if not reply or not isinstance(reply, str):
        return reply or ""
    text = reply
    for pat, repl in [
        (r'(?i)\binstagram\b', 'Facebook'),
        (r'(?i)\bthreads\b', 'Facebook'),
    ]:
        text = re.sub(pat, repl, text)
    return text


# ============================================================
# CHAT API
# ============================================================

@app.route('/api/chat', methods=['POST'])
def api_chat():
    data = request.json or {}
    message = (data.get('message') or '').strip()
    history = data.get('history') or []
    session_id = data.get('session_id')
    chat_key = data.get('chat_key') or str(uuid.uuid4())

    if not message:
        return jsonify({"success": False, "error": "Empty message"}), 400

    if not GEMINI_API_KEYS:
        reply = simple_fallback(message, session_id)
        return jsonify({
            "success": True,
            "reply": _sanitize_reply_facebook_only(reply),
            "tool_results": [],
            "chat_key": chat_key,
            "session_id": session_id,
        })

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history[-10:]:
        if h.get('role') in ('user', 'assistant') and h.get('content'):
            content = h.get('content') or ''
            if h.get('role') == 'assistant' and re.search(r'(?i)\b(instagram|threads)\b', content):
                continue
            messages.append({"role": h['role'], "content": content})
    messages.append({"role": "user", "content": message})

    data_g, err = call_gemini(messages, tools=TOOLS_SCHEMA)
    tool_results = []

    if err or not data_g:
        reply = simple_fallback(message, session_id)
        return jsonify({
            "success": True,
            "reply": _sanitize_reply_facebook_only(reply),
            "tool_results": [],
            "chat_key": chat_key,
            "session_id": session_id,
        })

    try:
        choice = data_g['choices'][0]['message']
        tool_calls = choice.get('tool_calls') or []
        if tool_calls:
            messages.append(choice)
            for tc in tool_calls:
                fn = tc.get('function') or {}
                name = fn.get('name')
                try:
                    args = json.loads(fn.get('arguments') or '{}')
                except Exception:
                    args = {}
                result = execute_tool(name, args, session_id=session_id)
                if result.get('session_id'):
                    session_id = result['session_id']
                if name == 'fetch_posts' and result.get('success') and session_id in sessions:
                    sessions[session_id]['_last_fetched'] = result.get('posts') or []
                    sessions[session_id]['_last_actor'] = result.get('actor')
                tool_results.append({"name": name, "result": result})
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get('id'),
                    "content": json.dumps(result),
                })
            final_data, final_err = call_gemini(messages)
            if final_err or not final_data:
                reply = format_tool_summary(tool_results)
            else:
                reply = final_data['choices'][0]['message'].get('content') or format_tool_summary(tool_results)
        else:
            reply = choice.get('content') or simple_fallback(message, session_id)

        return jsonify({
            "success": True,
            "reply": _sanitize_reply_facebook_only(reply),
            "tool_results": tool_results,
            "chat_key": chat_key,
            "session_id": session_id,
        })
    except Exception as e:
        print(f"chat error: {e}")
        reply = simple_fallback(message, session_id)
        return jsonify({
            "success": True,
            "reply": _sanitize_reply_facebook_only(reply),
            "tool_results": tool_results,
            "chat_key": chat_key,
            "session_id": session_id,
        })


# ============================================================
# REST / UI ENDPOINTS
# ============================================================

@app.route('/api/status', methods=['GET'])
def api_status():
    return jsonify(tool_get_status())


@app.route('/api/accounts', methods=['GET'])
def api_accounts():
    return jsonify(tool_list_accounts('facebook'))


@app.route('/api/auto/status', methods=['GET'])
def api_auto_status():
    return jsonify(tool_auto_status())


@app.route('/api/auto/start', methods=['POST'])
def api_auto_start():
    data = request.json or {}
    name = data.get('name')
    return jsonify(tool_auto_start(name=name) if name else tool_auto_start())


@app.route('/api/auto/stop', methods=['POST'])
def api_auto_stop():
    data = request.json or {}
    name = data.get('name')
    return jsonify(tool_auto_stop(name=name) if name else tool_auto_stop())


@app.route('/api/auto/remove', methods=['POST'])
def api_auto_remove():
    data = request.json or {}
    name = data.get('name')
    if not name:
        return jsonify({"success": False, "error": "name required"}), 400
    return jsonify(tool_auto_remove(name))


@app.route('/api/niche/master-fetch', methods=['POST'])
def api_master_fetch_niche():
    data = request.json or {}
    name = data.get('name')
    limit_per_source = int(data.get('limit_per_source', MASTER_FETCH_LIMIT))
    max_pages = int(data.get('max_pages', MASTER_FETCH_MAX_PAGES))
    result = tool_master_fetch_niche(
        name=name,
        limit_per_source=limit_per_source,
        max_pages=max_pages,
    )
    return jsonify(result), (200 if result.get('success') else 400)


@app.route('/api/niche/master-fetch-all', methods=['POST'])
def api_master_fetch_all():
    data = request.json or {}
    limit_per_source = int(data.get('limit_per_source', MASTER_FETCH_LIMIT))
    max_pages = int(data.get('max_pages', MASTER_FETCH_MAX_PAGES))
    result = tool_master_fetch_all_niches(
        limit_per_source=limit_per_source,
        max_pages=max_pages,
    )
    return jsonify(result), (200 if result.get('success') else 400)


@app.route('/api/master/status', methods=['GET'])
def api_master_status():
    try:
        configs = _list_auto_configs()
        niches = []
        for cfg in configs:
            name = cfg.get('name')
            count = 0
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT COUNT(*) FROM vault v
                    WHERE v.handler_handle = %s
                    AND NOT EXISTS (
                        SELECT 1 FROM posted_posts p
                        WHERE p.uri = v.uri AND p.platform = 'facebook'
                          AND p.status IN ('completed', 'posted')
                    )
                """, (name,))
                count = cur.fetchone()[0]
                cur.close()
                conn.close()
            niches.append({
                "niche": name,
                "count": count,
                "enabled": cfg.get('enabled', False),
                "source_handles": cfg.get('source_handles') or [],
                "destination": cfg.get('account_username'),
            })
        return jsonify({
            "success": True,
            "master_handle": BLUESKY_MASTER_HANDLE or "Not configured",
            "niches": niches,
            "total_reserve": sum(n.get('count', 0) for n in niches),
            "total_niches": len(niches),
            "timestamp": datetime.now().isoformat(),
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/cron/auto-run', methods=['GET'])
def cron_auto_run():
    """
    External / Vercel cron endpoint — same pattern as Instagram service.
    Runs ALL enabled pipelines once (reserve → post if FB destination set).
    Hit this on a schedule, e.g. every 5–10 minutes.
    """
    try:
        # Mirror Instagram: always attempt enabled pipelines (no global gate that blocks cron)
        enabled = [c for c in _list_auto_configs() if c.get('enabled')]
        if not enabled:
            msg = "No enabled pipelines — start one in chat or UI first"
            print(f"🔄 Cron auto-run skipped: {msg}")
            return jsonify({
                "success": True,
                "skipped": True,
                "reason": msg,
                "timestamp": datetime.now().isoformat(),
            })

        set_cron_state(True)  # mark cron active when external scheduler is hitting us
        result = tool_auto_run_now()
        print(f"🔄 Cron auto-run: {result.get('message', 'done')}")
        return jsonify({
            "success": True,
            "result": result,
            "pipelines_run": len(enabled),
            "timestamp": datetime.now().isoformat(),
        })
    except Exception as e:
        print(f"❌ Cron auto-run error: {e}")
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }), 500


@app.route('/api/post-now/accounts', methods=['GET'])
def api_post_now_accounts():
    return jsonify(tool_list_accounts('facebook'))


@app.route('/api/post-now', methods=['POST'])
def api_post_now_image():
    data = request.json or {}
    vault_id = data.get('vault_id')
    image_data = data.get('image_data') or data.get('image')
    caption = (data.get('caption') or '').strip()
    account_id = data.get('account_id')
    account_username = data.get('account_username')

    if vault_id is not None and str(vault_id).strip() != '':
        try:
            vid = int(vault_id)
        except (TypeError, ValueError):
            return jsonify({"success": False, "error": "vault_id must be an integer"}), 400
        result = tool_post_now(
            vault_id=vid,
            caption=caption or None,
            account_id=account_id,
            account_username=account_username,
        )
        return jsonify(result), (200 if result.get('success') else 500)

    if not image_data:
        return jsonify({"success": False, "error": "Provide vault_id or image_data"}), 400

    resolved = resolve_facebook_account_id(account_id, account_username)
    if not resolved:
        return jsonify({"success": False, "error": "Could not resolve Facebook account"}), 400

    jpeg, err = data_url_to_jpeg_bytes(image_data)
    if not jpeg:
        return jsonify({"success": False, "error": err or "Invalid image"}), 400
    jpeg = fix_image_for_feed(jpeg)
    public_url = upload_media_to_zernio(jpeg)
    if not public_url:
        return jsonify({"success": False, "error": "Upload failed"}), 500

    result = post_to_facebook(
        image_url=public_url,
        caption=caption or 'Posted via AI Vault',
        account_id=resolved,
    )
    if result.get('success'):
        result['message'] = "Posted to Facebook"
        result['account_id'] = resolved
    return jsonify(result), (200 if result.get('success') else 500)


@app.route('/api/post-now/schedule', methods=['POST'])
def api_post_now_schedule():
    data = request.json or {}
    image_data = data.get('image_data') or data.get('image')
    caption = (data.get('caption') or '').strip() or 'Scheduled via AI Vault'
    account_id = data.get('account_id')
    account_username = data.get('account_username')
    schedule_time_raw = data.get('schedule_time')

    if not image_data:
        return jsonify({"success": False, "error": "image_data required"}), 400

    resolved = resolve_facebook_account_id(account_id, account_username)
    if not resolved:
        return jsonify({"success": False, "error": "Could not resolve Facebook account"}), 400

    scheduled_for = parse_datetime_from_input(schedule_time_raw) if schedule_time_raw else None
    jpeg, err = data_url_to_jpeg_bytes(image_data)
    if not jpeg:
        return jsonify({"success": False, "error": err or "Invalid image"}), 400
    jpeg = fix_image_for_feed(jpeg)
    public_url = upload_media_to_zernio(jpeg)
    if not public_url:
        return jsonify({"success": False, "error": "Upload failed"}), 500

    result = post_to_facebook(
        image_url=public_url,
        caption=caption,
        account_id=resolved,
        scheduled_time=scheduled_for,
    )
    if result.get('success'):
        when = scheduled_for.strftime('%Y-%m-%d %H:%M') if scheduled_for else 'ASAP'
        result['message'] = f"Scheduled to Facebook for {when}"
        result['scheduled_for'] = when
        result['account_id'] = resolved
    return jsonify(result), (200 if result.get('success') else 500)


@app.route('/api/vault/add-image', methods=['POST'])
def api_vault_add_image():
    data = request.json or {}
    image_data = data.get('image_data') or data.get('image')
    caption = (data.get('caption') or '').strip() or 'Saved from AI Vault'
    if not image_data:
        return jsonify({"success": False, "error": "image_data required"}), 400

    public_url = None
    jpeg, err = data_url_to_jpeg_bytes(image_data)
    if jpeg:
        jpeg = fix_image_for_feed(jpeg)
        public_url = upload_media_to_zernio(jpeg)

    image_entry = {
        "url": public_url or image_data,
        "thumb": public_url or image_data,
        "alt": caption[:120],
    }
    uri = f"local:upload:{uuid.uuid4()}"
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"success": False, "error": "Database unavailable"}), 500
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO vault (uri, author, display_name, text, images, likes, reposts, replies, created_at, handler_handle, notes)
            VALUES (%s, %s, %s, %s, %s, 0, 0, 0, %s, %s, %s)
            RETURNING id
        ''', (
            uri, 'upload', 'Manual upload', caption, Json([image_entry]),
            datetime.now().isoformat(), 'manual', "Uploaded via UI · platform=facebook",
        ))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({
            "success": True,
            "vault_id": row[0] if row else None,
            "uri": uri,
            "message": "Image saved to vault",
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


handler = app

if __name__ == '__main__':
    print("🚀 Bluesky AI Vault → Facebook starting...")
    if GEMINI_API_KEYS:
        print(f"✅ Gemini keys: {len(GEMINI_API_KEYS)}")
    else:
        print("⚠️  No GEMINI_API_KEYS — keyword fallback only")

    if not IS_VERCEL:
        try:
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT session_id, session_string, handle, display_name
                    FROM sessions
                    WHERE expires_at > CURRENT_TIMESTAMP
                    ORDER BY last_used_at DESC LIMIT 1
                """)
                row = cur.fetchone()
                cur.close()
                conn.close()
                if row:
                    session_id, session_string, handle, display_name = row
                    try:
                        client = Client()
                        client.login(session_string=session_string)
                        sessions[session_id] = {
                            'client': client,
                            'handle': handle,
                            'session_string': session_string,
                            'display_name': display_name or handle,
                        }
                        print(f"✅ Restored Bluesky session @{handle}")
                    except Exception as e:
                        print(f"⚠️ Session restore: {e}")
        except Exception as e:
            print(f"⚠️ Session restore error: {e}")

        zernio_status = ensure_zernio_keys_loaded()
        if zernio_status.get('success'):
            try:
                synced = refresh_all_zernio_accounts()
                print(f"✅ Synced {len(synced) if synced else 0} Facebook account(s)")
            except Exception as e:
                print(f"⚠️ Account sync: {e}")
        else:
            print("⚠️  Set ZERNIO_API_KEY for Facebook posting")

        try:
            enabled = [c for c in _list_auto_configs() if c.get('enabled')]
            if enabled:
                start_result = start_auto_pilot()
                print(f"🤖 Auto pilot: {start_result.get('message')}")
            else:
                print("🤖 Auto pilot idle")
        except Exception as e:
            print(f"Auto pilot init: {e}")

    port = int(os.environ.get('PORT', 10000))
    print(f"🚀 Server on port {port}")
    app.run(debug=False, host='0.0.0.0', port=port)