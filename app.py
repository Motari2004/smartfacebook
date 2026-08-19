
# ============================================================
# VERCEL COMPATIBILITY
# ============================================================

import os

# Check if running on Vercel
IS_VERCEL = os.environ.get('VERCEL') == '1' or os.environ.get('VERCEL_ENV') is not None

if IS_VERCEL:
    print("🚀 Running on Vercel (serverless mode) - Auto-pilot disabled")




"""
Bluesky AI Vault → Facebook only
AI chat on top of Bluesky fetch → vault → schedule / post-now (Zernio Facebook)
Source: Bluesky · Destination: Facebook
"""

from flask import Flask, request, jsonify, send_from_directory, Response
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

app = Flask(__name__, static_folder='static')
CORS(app)

# ============================================================
# CONFIG
# ============================================================

DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://neondb_owner:npg_3FJeskp5EoVg@ep-polished-sky-ayuedb1p-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
)

ZERNIO_API_KEY = os.environ.get('ZERNIO_API_KEY')
ZERNIO_BASE_URL = "https://zernio.com/api/v1"
SCHEDULE_TIMEZONE = "Africa/Nairobi"
TIMEZONE = "Africa/Nairobi"
LOCAL_TIMEZONE = pytz.timezone(TIMEZONE)




# Google Gemini API — multiple keys, round-robin
_env_keys = os.environ.get('GEMINI_API_KEYS', '') or os.environ.get('GEMINI_API_KEY', '')
if _env_keys:
    GEMINI_API_KEYS = [k.strip() for k in _env_keys.split(',') if k.strip()]
else:
    # No hardcoded defaults - must be set in environment
    GEMINI_API_KEYS = []
    print("⚠️  No GEMINI_API_KEYS set in environment variables!")

# Log how many keys were loaded
if GEMINI_API_KEYS:
    print(f"✅ Loaded {len(GEMINI_API_KEYS)} Gemini API key(s) from environment")
else:
    print("⚠️  No Gemini API keys found - AI features will use fallback mode")
    
    
    
    

GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai"
GEMINI_MODEL = "gemini-3.1-flash-lite"  # Use a valid model
_gemini_key_index = 0

sessions = {}  # in-memory session cache


def next_gemini_key():
    global _gemini_key_index
    if not GEMINI_API_KEYS:
        return None
    key = GEMINI_API_KEYS[_gemini_key_index % len(GEMINI_API_KEYS)]
    _gemini_key_index += 1
    return key


def get_now():
    return datetime.now(LOCAL_TIMEZONE)


def format_datetime_for_zernio(dt):
    """Convert Africa/Nairobi datetime to UTC ISO for Zernio."""
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
# MULTI-ZERNIO-API CONFIG (Facebook accounts)
# ============================================================

def _detect_accounts_for_key(api_key, label="key"):
    """Query Zernio for accounts; keep only Facebook."""
    accounts = []
    if not api_key:
        return accounts
    try:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        response = requests.get(
            f"{ZERNIO_BASE_URL}/accounts",
            headers=headers,
            timeout=15
        )
        if response.status_code == 200:
            zernio_accounts = response.json().get('accounts', [])
            for acc in zernio_accounts:
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
                'account_count': len(accounts)
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
                'account_count': len(accounts)
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
                'account_count': len(accounts)
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
        msg = "No Zernio API keys found in .env. Set ZERNIO_API_KEY / ZERNIO_API_KEY1 / ZERNIO_API_KEYS."
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
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }


def get_zernio_headers():
    """Default headers using first available key."""
    keys = get_zernio_api_keys()
    if keys:
        return get_zernio_headers_for_key(keys[0]['key'])
    default_key = os.environ.get('ZERNIO_API_KEY')
    if default_key:
        return get_zernio_headers_for_key(default_key)
    return {}


def get_zernio_headers_for_account(account_username=None):
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

    return get_zernio_headers()


def save_zernio_account_row(account_id, platform, display_name, username,
                            profile_picture, api_key, api_key_index=None):
    platform = (platform or 'facebook').lower()
    if platform != 'facebook':
        return False
    if not account_id:
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
                profile_picture, api_key, api_key_index
            ))
        except Exception:
            cur.execute("""
                UPDATE zernio_accounts SET
                    display_name = %s,
                    username = %s,
                    profile_picture = %s,
                    api_key = %s,
                    api_key_index = COALESCE(%s, api_key_index),
                    is_active = TRUE,
                    last_sync = CURRENT_TIMESTAMP
                WHERE account_id = %s AND platform = %s
            """, (
                display_name, username, profile_picture,
                api_key, api_key_index, account_id, platform
            ))
            if cur.rowcount == 0:
                cur.execute("""
                    INSERT INTO zernio_accounts
                    (account_id, platform, display_name, username, profile_picture,
                     api_key, api_key_index, is_active, last_sync)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                """, (
                    account_id, platform, display_name, username,
                    profile_picture, api_key, api_key_index
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
            response = requests.get(
                f"{ZERNIO_BASE_URL}/accounts",
                headers=headers,
                timeout=15
            )
            if response.status_code == 200:
                accounts = response.json().get('accounts', [])
                for acc in accounts:
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
            "message": "Paste a Zernio key like: check key sk_xxxxx"
        }

    raw = str(api_key).strip()
    if '=' in raw:
        raw = raw.split('=', 1)[1].strip()
    raw = raw.strip().strip('"').strip("'")

    if not raw.startswith('sk_') and len(raw) < 20:
        return {
            "success": False,
            "error": "That does not look like a Zernio API key",
            "message": "Zernio keys usually start with sk_ — paste the full key."
        }

    headers = get_zernio_headers_for_key(raw)
    try:
        response = requests.get(
            f"{ZERNIO_BASE_URL}/accounts",
            headers=headers,
            timeout=15
        )
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": f"❌ Could not reach Zernio: {e}"
        }

    if response.status_code == 401:
        return {
            "success": False,
            "error": "Invalid API key",
            "message": "❌ This Zernio API key is invalid or revoked (401)."
        }
    if response.status_code == 429:
        return {
            "success": False,
            "error": "Rate limited",
            "message": "⚠️ Zernio rate-limited this key. Try again in a minute."
        }
    if response.status_code != 200:
        return {
            "success": False,
            "error": f"HTTP {response.status_code}",
            "message": f"❌ Zernio error {response.status_code}: {response.text[:200]}"
        }

    zernio_accounts = response.json().get('accounts', []) or []
    accounts = []
    for acc in zernio_accounts:
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
            f"But no Facebook accounts are connected on this Zernio key yet.\n"
            f"Connect a Facebook account in the Zernio dashboard first."
        )
    else:
        lines = [f"✅ Key valid ({key_preview}) — {len(accounts)} Facebook account(s):"]
        for a in accounts:
            lines.append(
                f"  • @{a['username']} ({a['display_name']}) — "
                f"facebook — id={a['account_id']}"
            )
        msg = "\n".join(lines)

    return {
        "success": True,
        "valid": True,
        "key_preview": key_preview,
        "count": len(accounts),
        "accounts": accounts,
        "message": msg,
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

        conn.commit()
        cur.close()
        conn.close()
        print("✅ Database initialized (Facebook-only)")
    except Exception as e:
        print(f"❌ DB init error: {e}")
        traceback.print_exc()


init_db()


# ============================================================
# FACEBOOK POSTING (via Zernio)
# ============================================================

def upload_media_to_zernio(image_bytes, filename="upload.jpg"):
    """Upload JPEG/PNG bytes to Zernio; returns public_url or None."""
    try:
        image_bytes.seek(0)
        if filename.lower().endswith('.png'):
            content_type = 'image/png'
        elif filename.lower().endswith('.gif'):
            content_type = 'image/gif'
        elif filename.lower().endswith('.webp'):
            content_type = 'image/webp'
        else:
            content_type = 'image/jpeg'

        presign_payload = {
            "filename": filename,
            "contentType": content_type
        }

        response = requests.post(
            f"{ZERNIO_BASE_URL}/media/presign",
            headers=get_zernio_headers(),
            json=presign_payload,
            timeout=30
        )

        if response.status_code not in [200, 201]:
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
            timeout=60
        )

        if upload_response.status_code not in [200, 201, 204]:
            print(f"Upload error: {upload_response.text}")
            return None

        return public_url

    except Exception as e:
        print(f"Error uploading media: {e}")
        traceback.print_exc()
        return None


def create_facebook_post(text, account_id, media_urls=None, scheduled_for=None, topic_tag=None, is_draft=False, content_type='feed'):
    """
    Create a Facebook post via Zernio API (pattern from quote-generator Facebook flow).
    text: can be empty string for image-only posts.
    scheduled_for: UTC ISO string (or None for publish now).
    content_type: 'feed' (default) or other Zernio Facebook content types if supported.
    """
    try:
        platform_config = {
            "platform": "facebook",
            "accountId": account_id
        }

        if content_type and content_type != 'feed':
            platform_config["platformSpecificData"] = {
                "contentType": content_type
            }
        if topic_tag:
            if "platformSpecificData" not in platform_config:
                platform_config["platformSpecificData"] = {}
            platform_config["platformSpecificData"]["topic_tag"] = topic_tag

        payload = {
            "platforms": [platform_config]
        }

        if text and str(text).strip():
            payload["content"] = str(text).strip()[:2000]  # Facebook practical caption limit

        if media_urls and len(media_urls) > 0:
            payload["mediaItems"] = []
            for url in media_urls:
                if url and str(url).startswith(('http://', 'https://')):
                    payload["mediaItems"].append({
                        "type": "image",
                        "url": url
                    })

        if is_draft:
            payload["isDraft"] = True
        elif scheduled_for:
            payload["scheduledFor"] = scheduled_for
            payload["timezone"] = TIMEZONE
        else:
            payload["publishNow"] = True

        print(f"📤 Sending to Facebook: {json.dumps({k: v for k, v in payload.items() if k != 'mediaItems'}, indent=2)}")

        response = requests.post(
            f"{ZERNIO_BASE_URL}/posts",
            headers=get_zernio_headers(),
            json=payload,
            timeout=30
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
                "scheduled_for": post.get('scheduledFor')
            }
        elif response.status_code == 409:
            error_data = response.json() if response.text else {}
            return {
                "success": False,
                "error": "Duplicate content",
                "message": error_data.get('message', 'This content was already posted recently')
            }
        else:
            try:
                error_data = response.json()
                error_msg = error_data.get('message', response.text)
            except Exception:
                error_msg = response.text
            return {
                "success": False,
                "error": f"API Error {response.status_code}",
                "message": error_msg
            }

    except Exception as e:
        print(f"Error creating Facebook post: {e}")
        traceback.print_exc()
        return {"success": False, "error": str(e)}


def get_facebook_account_id(account_id=None, account_username=None):
    """Resolve Facebook account_id from args or DB."""
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

    # First active Facebook account
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

    # Fallback: live Zernio list
    accounts = refresh_all_zernio_accounts()
    for acc in accounts:
        aid = acc.get('account_id') or acc.get('_id')
        if aid:
            return aid
    return None


def resolve_facebook_account_id(account_id=None, account_username=None):
    return get_facebook_account_id(account_id, account_username)


def post_to_facebook(image_url=None, image_bytes=None, caption="", account_id=None,
                    account_username=None, scheduled_time=None, topic_tag=None, is_draft=False):
    """
    Post to Facebook via Zernio.
    Provide either image_url (already on Zernio/CDN) or image_bytes to upload.
    scheduled_time: aware datetime in Africa/Nairobi, or None for now.
    """
    try:
        facebook_account_id = get_facebook_account_id(account_id, account_username)
        if not facebook_account_id:
            return {
                "success": False,
                "error": "No Facebook account connected. Connect one in Zernio and sync keys."
            }

        public_url = image_url
        if not public_url and image_bytes:
            public_url = upload_media_to_zernio(image_bytes)
            if not public_url:
                return {
                    "success": False,
                    "error": "Failed to upload image to Zernio."
                }

        media_urls = [public_url] if public_url else None

        scheduled_for = None
        if scheduled_time:
            scheduled_for = format_datetime_for_zernio(scheduled_time)
            print(f"📅 Scheduling GMT+3: {scheduled_time} → UTC: {scheduled_for}")

        result = create_facebook_post(
            text=caption or "",
            account_id=facebook_account_id,
            media_urls=media_urls,
            scheduled_for=scheduled_for,
            topic_tag=topic_tag,
            is_draft=is_draft
        )

        if result.get('success'):
            status_text = "Draft saved" if is_draft else ("Scheduled" if scheduled_time else "Posted")
            result['message'] = f"✅ {status_text} to Facebook!"
            result['platform'] = 'facebook'
            result['account_id'] = facebook_account_id
            result['caption'] = caption or ""
        return result

    except Exception as e:
        print(f"Error posting to Facebook: {e}")
        traceback.print_exc()
        return {"success": False, "error": str(e)}


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
    """Ensure reasonable JPEG for Facebook feed."""
    try:
        image_bytes.seek(0)
        img = Image.open(image_bytes)
        if img.mode != 'RGB':
            img = img.convert('RGB')
        # Facebook is flexible; keep under ~4k on long side
        max_side = 2048
        w, h = img.size
        if max(w, h) > max_side:
            scale = max_side / max(w, h)
            img = img.resize((int(w * scale), int(h * scale)), Image.Resampling.LANCZOS)
        out = BytesIO()
        img.save(out, format='JPEG', quality=90, optimize=True)
        out.seek(0)
        return out
    except Exception:
        image_bytes.seek(0)
        return image_bytes


# ============================================================
# BLUESKY TOOLS
# ============================================================

def tool_login(username, password):
    try:
        client = Client()
        client.login(username, password)
        profile = client.me
        handle = profile.handle
        session_id = str(uuid.uuid4())
        session_string = client.export_session_string()
        expires = datetime.utcnow() + timedelta(days=30)

        sessions[session_id] = {
            'client': client,
            'handle': handle,
            'session_string': session_string,
            'display_name': getattr(profile, 'display_name', None) or handle,
        }

        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO sessions (session_id, username, handle, display_name, session_string, expires_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (session_id) DO UPDATE SET
                        session_string = EXCLUDED.session_string,
                        last_used_at = CURRENT_TIMESTAMP,
                        expires_at = EXCLUDED.expires_at
                """, (session_id, username, handle, sessions[session_id]['display_name'], session_string, expires))
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                print(f"session save: {e}")

        return {
            "success": True,
            "session_id": session_id,
            "handle": handle,
            "message": f"Logged in as @{handle}"
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_restore_session(handle_or_sid):
    try:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "DB unavailable"}
        cur = conn.cursor()
        cur.execute("""
            SELECT session_id, session_string, handle FROM sessions
            WHERE (handle = %s OR session_id = %s) AND expires_at > CURRENT_TIMESTAMP
            ORDER BY last_used_at DESC LIMIT 1
        """, (handle_or_sid.lstrip('@'), handle_or_sid))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return {"success": False, "error": f"No valid session for {handle_or_sid}"}
        client = Client()
        client.login(session_string=row[1])
        sid = row[0]
        sessions[sid] = {
            'client': client,
            'handle': row[2],
            'session_string': row[1]
        }
        return {
            "success": True,
            "session_id": sid,
            "handle": row[2],
            "message": f"Restored session for @{row[2]}"
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_fetch_posts(session_id, actor, limit=15, include_reposts=False, media_only=True):
    if session_id not in sessions:
        return {"success": False, "error": "Not logged in / invalid session"}
    client = sessions[session_id]['client']
    try:
        if not actor.endswith('.bsky.social') and '.' not in actor:
            actor = actor + '.bsky.social'
        actor = actor.lstrip('@')

        # Prefer get_author_feed
        feed = client.get_author_feed(actor=actor, limit=min(limit * 2, 50))
        posts = []
        for item in feed.feed:
            post = item.post
            # Skip reposts unless requested
            if hasattr(item, 'reason') and item.reason and not include_reposts:
                continue
            record = post.record
            text = getattr(record, 'text', '') or ''
            images = []
            video = None
            embed = getattr(record, 'embed', None)
            if embed:
                # images
                if hasattr(embed, 'images') and embed.images:
                    for im in embed.images:
                        thumb = None
                        full = None
                        if hasattr(im, 'thumb') and im.thumb:
                            thumb = client.uri_to_url(im.thumb) if hasattr(client, 'uri_to_url') else None
                        if hasattr(im, 'fullsize') and im.fullsize:
                            full = client.uri_to_url(im.fullsize) if hasattr(client, 'uri_to_url') else None
                        # Prefer CDN from post.embed if available
                        images.append({
                            "url": full or thumb or "",
                            "thumb": thumb or full or "",
                            "alt": getattr(im, 'alt', '') or ''
                        })
                # recordWithMedia / external etc.
                if hasattr(embed, 'media') and embed.media and hasattr(embed.media, 'images'):
                    for im in embed.media.images:
                        images.append({
                            "url": getattr(im, 'fullsize', None) or getattr(im, 'thumb', None) or "",
                            "thumb": getattr(im, 'thumb', None) or "",
                            "alt": getattr(im, 'alt', '') or ''
                        })

            # Pull richer image URLs from view
            view_embed = getattr(post, 'embed', None)
            if view_embed:
                if hasattr(view_embed, 'images') and view_embed.images:
                    images = []
                    for im in view_embed.images:
                        images.append({
                            "url": getattr(im, 'fullsize', None) or getattr(im, 'thumb', None) or "",
                            "thumb": getattr(im, 'thumb', None) or "",
                            "alt": getattr(im, 'alt', '') or ''
                        })
                if hasattr(view_embed, 'media') and view_embed.media and hasattr(view_embed.media, 'images'):
                    images = []
                    for im in view_embed.media.images:
                        images.append({
                            "url": getattr(im, 'fullsize', None) or getattr(im, 'thumb', None) or "",
                            "thumb": getattr(im, 'thumb', None) or "",
                            "alt": getattr(im, 'alt', '') or ''
                        })

            if media_only and not images and not video:
                continue

            author = post.author
            posts.append({
                "uri": post.uri,
                "cid": post.cid,
                "author": author.handle,
                "display_name": getattr(author, 'display_name', None) or author.handle,
                "text": text,
                "images": images,
                "video": video,
                "likes": getattr(post, 'like_count', 0) or 0,
                "reposts": getattr(post, 'repost_count', 0) or 0,
                "replies": getattr(post, 'reply_count', 0) or 0,
                "created_at": getattr(record, 'created_at', None),
            })
            if len(posts) >= limit:
                break

        return {"success": True, "posts": posts, "count": len(posts), "actor": actor}
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "error": str(e)}


def tool_add_to_vault(posts, handler_handle=None):
    if not posts:
        return {"success": False, "error": "No posts to save"}
    conn = get_db_connection()
    if not conn:
        return {"success": False, "error": "DB unavailable"}
    saved = 0
    try:
        cur = conn.cursor()
        for p in posts:
            uri = p.get('uri')
            if not uri:
                continue
            try:
                cur.execute("""
                    INSERT INTO vault (uri, author, display_name, text, images, video, likes, reposts, replies, created_at, handler_handle)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (uri) DO NOTHING
                """, (
                    uri,
                    p.get('author') or '',
                    p.get('display_name'),
                    p.get('text'),
                    Json(p.get('images') or []),
                    Json(p.get('video')) if p.get('video') else None,
                    p.get('likes') or 0,
                    p.get('reposts') or 0,
                    p.get('replies') or 0,
                    p.get('created_at'),
                    handler_handle or p.get('author'),
                ))
                if cur.rowcount > 0:
                    saved += 1
            except Exception as e:
                print(f"vault insert {uri}: {e}")
        conn.commit()
        cur.close()
        conn.close()
        return {
            "success": True,
            "saved": saved,
            "message": f"Saved {saved} post(s) to vault"
        }
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "error": str(e)}


def tool_list_vault(limit=20, offset=0):
    conn = get_db_connection()
    if not conn:
        return {"success": False, "error": "DB unavailable"}
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, uri, author, display_name, text, images, video, likes, reposts, replies,
                   created_at, saved_at, handler_handle, notes
            FROM vault
            ORDER BY saved_at DESC
            LIMIT %s OFFSET %s
        """, (limit, offset))
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM vault")
        total = cur.fetchone()['count']
        cur.close()
        conn.close()
        vault = []
        for r in rows:
            vault.append({
                "id": r['id'],
                "uri": r['uri'],
                "author": r['author'],
                "display_name": r['display_name'],
                "text": r['text'],
                "images": r['images'] or [],
                "video": r['video'],
                "likes": r['likes'],
                "reposts": r['reposts'],
                "replies": r['replies'],
                "created_at": r['created_at'].isoformat() if r['created_at'] else None,
                "saved_at": r['saved_at'].isoformat() if r['saved_at'] else None,
                "handler_handle": r['handler_handle'],
                "notes": r['notes'],
            })
        return {"success": True, "vault": vault, "count": total}
    except Exception as e:
        return {"success": False, "error": str(e)}


def _get_vault_item(vault_id=None, uri=None):
    conn = get_db_connection()
    if not conn:
        return None
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if vault_id is not None:
            cur.execute("SELECT * FROM vault WHERE id = %s", (int(vault_id),))
        elif uri:
            cur.execute("SELECT * FROM vault WHERE uri = %s", (uri,))
        else:
            cur.close()
            conn.close()
            return None
        row = cur.fetchone()
        cur.close()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        print(f"_get_vault_item: {e}")
        return None


def _download_image_to_bytes(url):
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            return BytesIO(r.content)
    except Exception as e:
        print(f"download image: {e}")
    return None


def tool_post_now(uri=None, vault_id=None, caption=None, account_id=None, account_username=None, content_type='feed'):
    """Post a vault item (or by uri) to Facebook now."""
    item = _get_vault_item(vault_id=vault_id, uri=uri)
    if not item:
        return {"success": False, "error": "Vault item not found"}

    images = item.get('images') or []
    image_url = None
    if images:
        first = images[0]
        if isinstance(first, str):
            image_url = first
        else:
            image_url = first.get('url') or first.get('fullsize') or first.get('thumb')

    text = (caption if caption is not None and str(caption).strip() != '' else (item.get('text') or ''))[:2000]

    # Prefer re-upload via Zernio for reliability
    image_bytes = None
    public_url = None
    if image_url and image_url.startswith(('http://', 'https://')):
        image_bytes = _download_image_to_bytes(image_url)
        if image_bytes:
            image_bytes = fix_image_for_feed(image_bytes)
            public_url = upload_media_to_zernio(image_bytes)
        else:
            public_url = image_url  # try direct URL

    if not public_url and not image_bytes:
        # Text-only Facebook post
        result = post_to_facebook(
            image_url=None,
            caption=text,
            account_id=account_id,
            account_username=account_username,
            scheduled_time=None
        )
    else:
        result = post_to_facebook(
            image_url=public_url,
            caption=text,
            account_id=account_id,
            account_username=account_username,
            scheduled_time=None
        )

    if result.get('success'):
        # record posted
        try:
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO posted_posts (vault_id, uri, platform, platform_post_id, status, metadata)
                    VALUES (%s, %s, 'facebook', %s, 'posted', %s)
                    ON CONFLICT (uri, platform) DO UPDATE SET
                        platform_post_id = EXCLUDED.platform_post_id,
                        status = 'posted',
                        posted_at = CURRENT_TIMESTAMP
                """, (
                    item.get('id'),
                    item.get('uri'),
                    result.get('post_id'),
                    Json({"account_id": result.get('account_id')})
                ))
                conn.commit()
                cur.close()
                conn.close()
        except Exception as e:
            print(f"posted_posts: {e}")
        result['message'] = f"✅ Posted to Facebook (vault id={item.get('id')})"
        result['vault_id'] = item.get('id')

    return result


def tool_post_vault_batch(count=3, account_id=None, account_username=None, content_type='feed'):
    r = tool_list_vault(limit=max(count * 2, 10))
    items = r.get('vault') or []
    # prefer items with images that aren't already posted
    posted_uris = set()
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT uri FROM posted_posts WHERE platform = 'facebook' AND status = 'posted'")
            posted_uris = {row[0] for row in cur.fetchall()}
            cur.close()
            conn.close()
    except Exception:
        pass

    chosen = []
    for it in items:
        if it.get('uri') in posted_uris:
            continue
        chosen.append(it)
        if len(chosen) >= count:
            break

    results = []
    posted = 0
    for it in chosen:
        res = tool_post_now(
            vault_id=it.get('id'),
            account_id=account_id,
            account_username=account_username
        )
        results.append(res)
        if res.get('success'):
            posted += 1
        time.sleep(1.5)

    return {
        "success": posted > 0,
        "posted_count": posted,
        "results": results,
        "message": f"Posted {posted}/{len(chosen)} items to Facebook"
    }


def tool_list_accounts(platform='facebook'):
    """List Facebook accounts only."""
    platform = (platform or 'facebook').lower()
    if platform != 'facebook':
        return {
            "success": True,
            "accounts": [],
            "count": 0,
            "message": "Only Facebook accounts are supported."
        }

    # Deactivate any non-Facebook rows left from older installs
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE zernio_accounts
                SET is_active = FALSE
                WHERE LOWER(COALESCE(platform, '')) NOT IN ('facebook')
                  AND is_active = TRUE
            """)
            if cur.rowcount:
                print(f"🧹 Deactivated {cur.rowcount} non-Facebook account row(s)")
            conn.commit()
            cur.close()
        except Exception as e:
            print(f"deactivate non-facebook: {e}")
        try:
            conn.close()
        except Exception:
            pass

    accounts = []
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT account_id, username, display_name, profile_picture, platform, is_active, last_sync
                FROM zernio_accounts
                WHERE LOWER(platform) = 'facebook' AND is_active = TRUE
                ORDER BY last_sync DESC NULLS LAST
            """)
            for row in cur.fetchall():
                accounts.append({
                    "account_id": row['account_id'],
                    "label": row['username'] or row['display_name'],
                    "username": row['username'],
                    "display_name": row['display_name'],
                    "platform": "facebook",
                    "profile_picture": row['profile_picture'],
                })
            cur.close()
            conn.close()
        except Exception as e:
            print(f"list accounts: {e}")

    if not accounts:
        refresh_all_zernio_accounts()
        # one-shot re-read without infinite recursion
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor(cursor_factory=RealDictCursor)
                cur.execute("""
                    SELECT account_id, username, display_name, profile_picture
                    FROM zernio_accounts
                    WHERE LOWER(platform) = 'facebook' AND is_active = TRUE
                    ORDER BY last_sync DESC NULLS LAST
                """)
                for row in cur.fetchall():
                    accounts.append({
                        "account_id": row['account_id'],
                        "label": row['username'] or row['display_name'],
                        "username": row['username'],
                        "display_name": row['display_name'],
                        "platform": "facebook",
                        "profile_picture": row['profile_picture'],
                    })
                cur.close()
                conn.close()
            except Exception as e:
                print(f"list accounts retry: {e}")

    if not accounts:
        return {
            "success": True,
            "accounts": [],
            "count": 0,
            "message": "No Facebook accounts connected. Connect a Facebook account in Zernio and set ZERNIO_API_KEY."
        }

    lines = [f"Facebook accounts ({len(accounts)}):"]
    for a in accounts:
        lines.append(f"• @{a.get('label')} — facebook — id={a.get('account_id')}")
    return {
        "success": True,
        "accounts": accounts,
        "count": len(accounts),
        "message": "\n".join(lines),
        "destination": "facebook",
    }


def tool_list_api_keys():
    status = ensure_zernio_keys_loaded()
    return {
        "success": status.get('success'),
        "count": status.get('count'),
        "message": status.get('message'),
        "keys_preview": status.get('keys_preview'),
    }


def tool_get_status():
    vault_count = scheduled_count = posted_count = accounts_count = 0
    active_handle = None
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM vault")
            vault_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM posted_posts WHERE platform = 'facebook'")
            posted_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM zernio_accounts WHERE platform = 'facebook' AND is_active = TRUE")
            accounts_count = cur.fetchone()[0]
            cur.close()
            conn.close()
    except Exception as e:
        print(f"status: {e}")

    for s in sessions.values():
        if s.get('handle'):
            active_handle = s['handle']
            break

    return {
        "success": True,
        "vault_count": vault_count,
        "scheduled_count": scheduled_count,
        "posted_count": posted_count,
        "accounts_count": accounts_count,
        "active_handle": active_handle,
        "platform": "facebook",
        "destination": "facebook",
        "message": (
            f"Destination: Facebook only · Vault: {vault_count} · "
            f"Posted (Facebook): {posted_count} · Facebook accounts: {accounts_count}"
            + (f" · Bluesky login: @{active_handle}" if active_handle else " · No Bluesky session")
        )
    }


def tool_list_scheduled():
    # Zernio scheduled posts listing — optional lightweight
    try:
        headers = get_zernio_headers()
        if not headers:
            return {"success": True, "scheduled": [], "count": 0, "message": "No Zernio key"}
        response = requests.get(
            f"{ZERNIO_BASE_URL}/posts",
            headers=headers,
            params={"status": "scheduled", "limit": 50},
            timeout=15
        )
        if response.status_code != 200:
            return {"success": True, "scheduled": [], "count": 0}
        data = response.json()
        posts = data.get('posts') or data.get('data') or []
        items = []
        for p in posts:
            platforms = p.get('platforms') or []
            is_facebook = any((pl.get('platform') or '').lower() == 'facebook' for pl in platforms)
            if not is_facebook:
                continue
            items.append({
                "id": p.get('_id'),
                "text": (p.get('content') or '')[:120],
                "scheduled_for": p.get('scheduledFor'),
                "has_image": bool(p.get('mediaItems')),
                "status": p.get('status'),
            })
        return {"success": True, "scheduled": items, "count": len(items)}
    except Exception as e:
        return {"success": False, "error": str(e), "scheduled": [], "count": 0}


# ============================================================
# AUTO PILOT (Bluesky → Facebook)
# ============================================================

_auto_thread = None
_auto_stop = threading.Event()


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
        cur.close()
        conn.close()
        return dict(zip(cols, row))
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
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        print(f"list auto_configs: {e}")
        return []


def _save_auto_config(cfg: dict):
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO auto_config (
                name, enabled, source_handle, account_id, account_username,
                content_type, poll_interval_sec, media_only, include_reposts,
                max_posts_per_run, bluesky_handle, bluesky_app_password,
                last_run_at, last_error, last_result, updated_at
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP
            )
            ON CONFLICT (name) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source_handle = COALESCE(EXCLUDED.source_handle, auto_config.source_handle),
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
            cfg.get('name', 'default'),
            bool(cfg.get('enabled', False)),
            cfg.get('source_handle'),
            cfg.get('account_id'),
            cfg.get('account_username'),
            cfg.get('content_type', 'feed'),
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


def _auto_seen(uri, config_name='default'):
    try:
        conn = get_db_connection()
        if not conn:
            return True
        cur = conn.cursor()
        cur.execute("SELECT id FROM auto_seen WHERE config_name=%s AND uri=%s", (config_name, uri))
        exists = cur.fetchone() is not None
        cur.close()
        conn.close()
        return exists
    except Exception:
        return True


def _auto_mark_seen(uri, posted=False, config_name='default'):
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO auto_seen (config_name, uri, posted)
            VALUES (%s, %s, %s)
            ON CONFLICT (config_name, uri) DO UPDATE SET posted = EXCLUDED.posted
        ''', (config_name, uri, posted))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"auto_mark_seen: {e}")


def _get_bluesky_client_for_auto(cfg):
    for sid, s in sessions.items():
        if s.get('client'):
            return s['client'], sid

    login_handle = cfg.get('bluesky_handle')
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            row = None
            if login_handle:
                cur.execute('''
                    SELECT session_id, session_string, handle FROM sessions
                    WHERE handle = %s AND expires_at > CURRENT_TIMESTAMP
                    ORDER BY last_used_at DESC LIMIT 1
                ''', (login_handle,))
                row = cur.fetchone()
            if not row:
                cur.execute('''
                    SELECT session_id, session_string, handle FROM sessions
                    WHERE expires_at > CURRENT_TIMESTAMP
                    ORDER BY last_used_at DESC LIMIT 1
                ''')
                row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                client = Client()
                client.login(session_string=row[1])
                sid = row[0]
                sessions[sid] = {
                    'client': client,
                    'handle': row[2],
                    'session_string': row[1]
                }
                print(f"✅ Auto restored Bluesky session for @{row[2]}")
                return client, sid
    except Exception as e:
        print(f"auto restore session: {e}")

    bsky_user = cfg.get('bluesky_handle')
    bsky_pass = cfg.get('bluesky_app_password')
    if bsky_user and bsky_pass:
        result = tool_login(bsky_user, bsky_pass)
        if result.get('success'):
            sid = result['session_id']
            return sessions[sid]['client'], sid

    return None, None


def run_auto_once(name='default'):
    """One cycle: fetch Bluesky → vault → post to Facebook."""
    key_status = ensure_zernio_keys_loaded(for_auto=True)
    if not key_status.get('success'):
        return {
            "success": False,
            "error": key_status.get('message') or "No Zernio API keys in .env",
            "keys_checked": True,
        }

    cfg = _load_auto_config(name)
    if not cfg:
        return {"success": False, "error": "No auto config. Set it up first."}
    if not cfg.get('enabled'):
        return {"success": False, "error": "Auto pilot is disabled", "skipped": True}

    source = cfg.get('source_handle')
    if not source:
        return {"success": False, "error": "source_handle not set"}

    client, session_id = _get_bluesky_client_for_auto(cfg)
    if not client or not session_id:
        msg = "No Bluesky session. Login once in chat, or set bluesky_handle + app password in auto config."
        _save_auto_config({**cfg, 'last_error': msg, 'last_run_at': datetime.now()})
        return {"success": False, "error": msg}

    try:
        fetch = tool_fetch_posts(
            session_id=session_id,
            actor=source,
            limit=max(5, int(cfg.get('max_posts_per_run') or 2) * 3),
            include_reposts=bool(cfg.get('include_reposts')),
            media_only=bool(cfg.get('media_only', True))
        )
        if not fetch.get('success'):
            _save_auto_config({**cfg, 'last_error': fetch.get('error'), 'last_run_at': datetime.now()})
            return fetch

        posts = fetch.get('posts') or []
        new_posts = []
        for p in posts:
            uri = p.get('uri')
            if not uri or _auto_seen(uri, name):
                continue
            new_posts.append(p)
            if len(new_posts) >= int(cfg.get('max_posts_per_run') or 2):
                break

        if not new_posts:
            result_msg = f"No new posts from @{source}"
            _save_auto_config({**cfg, 'last_error': None, 'last_result': result_msg, 'last_run_at': datetime.now()})
            return {"success": True, "posted_count": 0, "message": result_msg}

        tool_add_to_vault(new_posts, handler_handle=source)

        account_id = resolve_facebook_account_id(cfg.get('account_id'), cfg.get('account_username'))
        if not account_id:
            msg = f"Bad Facebook account on pipeline {name}: {cfg.get('account_id')} / {cfg.get('account_username')}"
            _save_auto_config({**cfg, 'last_error': msg, 'last_run_at': datetime.now()})
            return {"success": False, "error": msg}

        posted = 0
        errors = []
        for p in new_posts:
            r = tool_post_now(
                uri=p.get('uri'),
                account_id=account_id,
                account_username=cfg.get('account_username')
            )
            _auto_mark_seen(p.get('uri'), posted=bool(r.get('success')), config_name=name)
            if r.get('success'):
                posted += 1
            else:
                errors.append(r.get('error') or r.get('message') or 'failed')
            time.sleep(2)

        result_msg = f"Posted {posted}/{len(new_posts)} to Facebook from @{source}"
        if errors:
            result_msg += f" · errors: {'; '.join(errors[:3])}"
        _save_auto_config({
            **cfg,
            'last_error': None if posted else (errors[0] if errors else None),
            'last_result': result_msg,
            'last_run_at': datetime.now()
        })
        return {"success": True, "posted_count": posted, "message": result_msg, "errors": errors}

    except Exception as e:
        traceback.print_exc()
        _save_auto_config({**cfg, 'last_error': str(e), 'last_run_at': datetime.now()})
        return {"success": False, "error": str(e)}


def _auto_loop():
    print("🤖 Auto pilot loop started (Facebook destination)")
    while not _auto_stop.is_set():
        try:
            configs = [c for c in _list_auto_configs() if c.get('enabled')]
            for cfg in configs:
                name = cfg.get('name') or 'default'
                interval = int(cfg.get('poll_interval_sec') or 300)
                last = cfg.get('last_run_at')
                should_run = True
                if last:
                    try:
                        if isinstance(last, str):
                            last_dt = datetime.fromisoformat(last)
                        else:
                            last_dt = last
                        if last_dt.tzinfo is None:
                            delta = (datetime.now() - last_dt).total_seconds()
                        else:
                            delta = (datetime.now(last_dt.tzinfo) - last_dt).total_seconds()
                        should_run = delta >= interval
                    except Exception:
                        should_run = True
                if should_run:
                    print(f"🤖 Auto run: {name}")
                    run_auto_once(name)
        except Exception as e:
            print(f"auto loop: {e}")
        _auto_stop.wait(30)


def start_auto_pilot():
    global _auto_thread
    if _auto_thread and _auto_thread.is_alive():
        return {"success": True, "message": "Auto pilot already running", "running": True}
    _auto_stop.clear()
    _auto_thread = threading.Thread(target=_auto_loop, daemon=True)
    _auto_thread.start()
    return {"success": True, "message": "Auto pilot started (→ Facebook)", "running": True}


def stop_auto_pilot():
    _auto_stop.set()
    return {"success": True, "message": "Auto pilot stop signal sent", "running": False}


def tool_auto_status():
    configs = _list_auto_configs()
    running = _auto_thread is not None and _auto_thread.is_alive() and not _auto_stop.is_set()
    pipelines = []
    for c in configs:
        pipelines.append({
            "name": c.get('name'),
            "enabled": c.get('enabled'),
            "source_handle": c.get('source_handle'),
            "account_username": c.get('account_username'),
            "account_id": c.get('account_id'),
            "poll_interval_sec": c.get('poll_interval_sec'),
            "max_posts_per_run": c.get('max_posts_per_run'),
            "last_run_at": str(c.get('last_run_at')) if c.get('last_run_at') else None,
            "last_error": c.get('last_error'),
            "last_result": c.get('last_result'),
        })
    return {
        "success": True,
        "running": running,
        "pipelines": pipelines,
        "destination": "facebook",
        "message": f"Auto {'ON' if running else 'OFF'} · {len([p for p in pipelines if p['enabled']])} enabled · destination=Facebook"
    }


def tool_auto_start():
    # enable all configs that have source+account, or just start the loop
    configs = _list_auto_configs()
    for c in configs:
        if c.get('source_handle') and (c.get('account_id') or c.get('account_username')):
            _save_auto_config({**c, 'enabled': True})
    return start_auto_pilot()


def tool_auto_stop():
    configs = _list_auto_configs()
    for c in configs:
        _save_auto_config({**c, 'enabled': False})
    return stop_auto_pilot()


def tool_auto_run_now(name='default'):
    return run_auto_once(name)


def tool_auto_setup(name, source_handle, account_username=None, account_id=None,
                    poll_interval_sec=300, max_posts_per_run=2, media_only=True,
                    bluesky_handle=None, bluesky_app_password=None):
    cfg = {
        'name': name or 'default',
        'enabled': True,
        'source_handle': source_handle.lstrip('@') if source_handle else None,
        'account_username': account_username,
        'account_id': account_id or resolve_facebook_account_id(None, account_username),
        'poll_interval_sec': int(poll_interval_sec or 300),
        'max_posts_per_run': int(max_posts_per_run or 2),
        'media_only': bool(media_only),
        'content_type': 'feed',
        'bluesky_handle': bluesky_handle,
        'bluesky_app_password': bluesky_app_password,
    }
    ok = _save_auto_config(cfg)
    if ok:
        start_auto_pilot()
        return {
            "success": True,
            "message": f"Pipeline '{cfg['name']}' set: @{cfg['source_handle']} → Facebook @{account_username or account_id} every {cfg['poll_interval_sec']}s"
        }
    return {"success": False, "error": "Failed to save config"}


def tool_auto_remove(name):
    try:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "DB unavailable"}
        cur = conn.cursor()
        cur.execute("DELETE FROM auto_config WHERE name = %s", (name,))
        cur.execute("DELETE FROM auto_seen WHERE config_name = %s", (name,))
        conn.commit()
        cur.close()
        conn.close()
        return {"success": True, "message": f"Removed pipeline {name}"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============================================================
# GEMINI + CHAT
# ============================================================

def call_gemini(messages, tools=None):
    key = next_gemini_key()
    if not key:
        return None, "No Gemini API keys"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": GEMINI_MODEL,
        "messages": messages,
    }
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"
    try:
        r = requests.post(
            f"{GEMINI_BASE_URL}/chat/completions",
            headers=headers,
            json=payload,
            timeout=60
        )
        if r.status_code != 200:
            return None, f"Gemini {r.status_code}: {r.text[:300]}"
        return r.json(), None
    except Exception as e:
        return None, str(e)


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
                    "password": {"type": "string"}
                },
                "required": ["username", "password"]
            }
        }
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
                    "limit": {"type": "integer"},
                    "media_only": {"type": "boolean"},
                    "include_reposts": {"type": "boolean"}
                },
                "required": ["actor"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "add_to_vault",
            "description": "Save recently fetched posts to vault",
            "parameters": {"type": "object", "properties": {}}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_vault",
            "description": "List items in the vault",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer"}}
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "post_now",
            "description": "Post a vault item to Facebook now by vault_id or uri",
            "parameters": {
                "type": "object",
                "properties": {
                    "vault_id": {"type": "integer"},
                    "uri": {"type": "string"},
                    "caption": {"type": "string"},
                    "account_username": {"type": "string"}
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_accounts",
            "description": "List connected Facebook accounts",
            "parameters": {"type": "object", "properties": {}}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_status",
            "description": "Get vault / posted / accounts status",
            "parameters": {"type": "object", "properties": {}}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_scheduled",
            "description": "List scheduled Facebook posts",
            "parameters": {"type": "object", "properties": {}}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "auto_setup",
            "description": "Configure auto pipeline: watch Bluesky handle → post to Facebook account",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "source_handle": {"type": "string"},
                    "account_username": {"type": "string"},
                    "poll_interval_sec": {"type": "integer"},
                    "max_posts_per_run": {"type": "integer"}
                },
                "required": ["source_handle"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "auto_start",
            "description": "Start auto pilot",
            "parameters": {"type": "object", "properties": {}}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "auto_stop",
            "description": "Stop auto pilot",
            "parameters": {"type": "object", "properties": {}}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "auto_status",
            "description": "Auto pilot status",
            "parameters": {"type": "object", "properties": {}}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "auto_run_now",
            "description": "Run one auto cycle immediately",
            "parameters": {
                "type": "object",
                "properties": {"name": {"type": "string"}}
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "check_zernio_key",
            "description": "Validate a Zernio API key and list Facebook accounts",
            "parameters": {
                "type": "object",
                "properties": {"api_key": {"type": "string"}},
                "required": ["api_key"]
            }
        }
    },
]


def execute_tool(name, args, session_id=None):
    args = args or {}
    try:
        if name == 'login':
            return tool_login(args.get('username'), args.get('password'))
        if name == 'fetch_posts':
            if not session_id:
                return {"success": False, "error": "Login first"}
            return tool_fetch_posts(
                session_id,
                args.get('actor'),
                limit=int(args.get('limit') or 15),
                media_only=bool(args.get('media_only', True)),
                include_reposts=bool(args.get('include_reposts', False))
            )
        if name == 'add_to_vault':
            posts = []
            if session_id and session_id in sessions:
                posts = sessions[session_id].get('_last_fetched') or []
            return tool_add_to_vault(posts, handler_handle=sessions.get(session_id, {}).get('_last_actor'))
        if name == 'list_vault':
            return tool_list_vault(limit=int(args.get('limit') or 15))
        if name == 'post_now':
            return tool_post_now(
                vault_id=args.get('vault_id'),
                uri=args.get('uri'),
                caption=args.get('caption'),
                account_username=args.get('account_username'),
                account_id=args.get('account_id')
            )
        if name == 'list_accounts':
            return tool_list_accounts('facebook')
        if name == 'get_status':
            return tool_get_status()
        if name == 'list_scheduled':
            return tool_list_scheduled()
        if name == 'auto_setup':
            return tool_auto_setup(
                name=args.get('name') or 'default',
                source_handle=args.get('source_handle'),
                account_username=args.get('account_username'),
                account_id=args.get('account_id'),
                poll_interval_sec=args.get('poll_interval_sec') or 300,
                max_posts_per_run=args.get('max_posts_per_run') or 2,
            )
        if name == 'auto_start':
            return tool_auto_start()
        if name == 'auto_stop':
            return tool_auto_stop()
        if name == 'auto_status':
            return tool_auto_status()
        if name == 'auto_run_now':
            return tool_auto_run_now(args.get('name') or 'default')
        if name == 'check_zernio_key':
            return tool_check_zernio_key(args.get('api_key'))
        if name == 'list_api_keys':
            return tool_list_api_keys()
        return {"success": False, "error": f"Unknown tool {name}"}
    except Exception as e:
        traceback.print_exc()
        return {"success": False, "error": str(e)}


SYSTEM_PROMPT = """You are the AI for Bluesky AI Vault → Facebook.

RULES:
- Source platform: Bluesky (login, fetch posts).
- Destination platform: Facebook only (post, schedule, auto-pilot via Zernio).
- Only talk about Bluesky and Facebook. Do not name any other social networks.
- Connected publishing accounts are Facebook accounts only.

You help the user:
- Login to Bluesky
- Fetch posts from Bluesky handles
- Save them to a vault
- Post vault items to Facebook
- Auto-pilot: watch a Bluesky account and cross-post new media to Facebook

Be concise. Timezone for schedules is Africa/Nairobi (GMT+3).
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
            continue
        parts.append(f"{name}: OK")
    return "\n".join(parts) if parts else "Done."


def simple_fallback(msg, session_id):
    lower = msg.lower().strip()

    if lower.startswith('login ') or 'login with' in lower:
        m = re.search(r'login(?:\s+with)?\s+([^\s]+)\s+(?:and\s+)?(.+)', msg.strip(), re.IGNORECASE)
        if m:
            username = m.group(1).strip().rstrip(',')
            password = m.group(2).strip().rstrip('.,!')
            result = tool_login(username, password)
            if result.get('success'):
                return f"✅ {result.get('message')}\nSession ID: {result.get('session_id')}"
            return f"❌ Login failed: {result.get('error')}"
        return "Format: Login with <handle> and <app-password>"

    if 'restore' in lower and ('session' in lower or '@' in lower or '.bsky' in lower):
        m = re.search(r'@?([a-zA-Z0-9._-]+\.bsky\.social|[a-zA-Z0-9._-]+)', msg)
        if m:
            result = tool_restore_session(m.group(1))
            return result.get('message') or result.get('error') or str(result)

    sk = re.search(r'(sk_[A-Za-z0-9]{20,})', msg)
    if sk:
        return tool_check_zernio_key(api_key=sk.group(1)).get('message') or str(
            tool_check_zernio_key(api_key=sk.group(1))
        )

    if any(w in lower for w in ('api key', 'api keys', 'zernio key', 'zernio keys', 'how many key')):
        r = tool_list_api_keys()
        return r.get('message') or str(r)

    if any(w in lower for w in ('status', 'how many', "what's in", 'counts')):
        r = tool_get_status()
        return r.get('message', str(r)) if r.get('success') else str(r)

    if 'vault' in lower and any(w in lower for w in ('list', 'show', 'what')):
        r = tool_list_vault(limit=10)
        if not r.get('success'):
            return r.get('error', str(r))
        items = r.get('vault') or []
        if not items:
            return "Vault is empty."
        lines = [f"Vault ({r.get('count')} items):"]
        for i, it in enumerate(items, 1):
            lines.append(f"{i}. id={it.get('id')} @{it.get('author')}: {(it.get('text') or '')[:80]}")
        return "\n".join(lines)

    if 'scheduled' in lower:
        r = tool_list_scheduled()
        if not r.get('success'):
            return r.get('error', str(r))
        items = r.get('scheduled') or []
        if not items:
            return "No scheduled Facebook posts."
        lines = [f"Scheduled ({r.get('count')}):"]
        for it in items:
            lines.append(f"• {it.get('scheduled_for')} — {(it.get('text') or '')[:60]}")
        return "\n".join(lines)

    if 'account' in lower and 'api' not in lower:
        r = tool_list_accounts('facebook')
        if not r.get('success'):
            return r.get('error', str(r))
        accs = r.get('accounts') or []
        if not accs:
            return "No connected Facebook accounts. Connect one in Zernio and set ZERNIO_API_KEY."
        return "Facebook accounts:\n" + "\n".join(
            f"• @{a.get('label')} ({a.get('account_id')})" for a in accs
        )

    if 'fetch' in lower:
        m = re.search(r'@?([a-zA-Z0-9._-]+\.bsky\.social|[a-zA-Z0-9._-]+)', msg)
        limit_m = re.search(r'(\d+)\s*posts?', lower)
        limit = int(limit_m.group(1)) if limit_m else 15
        if not session_id:
            return "Not logged in. Say: Login with <handle> and <app-password>"
        if not m:
            return "Say: Fetch 15 posts from @handle"
        actor = m.group(1)
        if '.' not in actor:
            actor = actor + '.bsky.social'
        r = tool_fetch_posts(session_id, actor, limit=limit)
        if not r.get('success'):
            return f"❌ {r.get('error')}"
        posts = r.get('posts') or []
        lines = [f"Fetched {len(posts)} posts from @{actor}:"]
        for i, p in enumerate(posts[:8], 1):
            media = f" [{len(p.get('images') or [])} img]" if p.get('images') else ""
            lines.append(f"{i}. {(p.get('text') or '')[:70]}{media}")
        if len(posts) > 8:
            lines.append(f"...and {len(posts)-8} more")
        lines.append("\nSay “save them to vault” to store them.")
        if session_id in sessions:
            sessions[session_id]['_last_fetched'] = posts
            sessions[session_id]['_last_actor'] = actor
        return "\n".join(lines)

    if any(w in lower for w in ('save', 'add to vault', 'vault them')):
        if not session_id or session_id not in sessions:
            return "Not logged in / no recent fetch. Fetch posts first."
        posts = sessions[session_id].get('_last_fetched') or []
        if not posts:
            return "No recent fetch to save. Fetch posts first."
        actor = sessions[session_id].get('_last_actor')
        r = tool_add_to_vault(posts, handler_handle=actor)
        return r.get('message') or r.get('error') or str(r)

    id_m = re.search(r'post\s+(?:id\s+)?(\d+)', lower)
    if id_m or re.search(r'\bid\s*(\d+)\b', lower):
        vid = int(id_m.group(1) if id_m else re.search(r'\bid\s*(\d+)\b', lower).group(1))
        result = tool_post_now(vault_id=vid)
        return result.get('message') or result.get('error') or str(result)

    if re.search(r'post\s+(this\s+)?(image|photo|pic)?\s*(to\s+)?(facebook)?', lower) or \
       ('facebook' in lower and 'post' in lower) or \
       ('post' in lower and 'vault' in lower) or 'post now' in lower or 'post them' in lower:
        count_m = re.search(r'(\d+)\s*posts?', lower)
        count = int(count_m.group(1)) if count_m else 1
        if count > 1:
            result = tool_post_vault_batch(count=count)
        else:
            r = tool_list_vault(limit=5)
            items = r.get('vault') or []
            if not items:
                return "No vault items to post. Fetch Bluesky posts and save them first."
            chosen = None
            for it in items:
                if it.get('images'):
                    chosen = it
                    break
            chosen = chosen or items[0]
            result = tool_post_now(vault_id=chosen.get('id'))
        return result.get('message') or result.get('error') or str(result)

    if any(w in lower for w in ('remove pipeline', 'delete pipeline', 'remove auto', 'delete auto')):
        m = re.search(r'(?:remove|delete)\s+(?:pipeline|auto)\s+([a-zA-Z0-9._-]+)', msg, re.I)
        if m:
            return tool_auto_remove(m.group(1)).get('message') or str(tool_auto_remove(m.group(1)))
        return "Say: Remove pipeline <name>"

    if any(w in lower for w in ('auto status', 'autopilot', 'auto pilot')):
        return tool_auto_status().get('message', str(tool_auto_status()))
    if any(w in lower for w in ('stop auto', 'auto stop', 'disable auto')):
        return tool_auto_stop().get('message')
    if any(w in lower for w in ('start auto', 'auto start', 'go autonomous')):
        return str(tool_auto_start())
    if 'auto run' in lower or 'run auto' in lower:
        return str(tool_auto_run_now())

    # auto setup pattern
    m = re.search(
        r'auto\s+setup.*?watch\s+@?([a-zA-Z0-9._-]+).*?(?:post\s+to|to)\s+@?([a-zA-Z0-9._-]+)',
        msg, re.I
    )
    if m or ('auto setup' in lower and 'watch' in lower):
        if m:
            return tool_auto_setup(
                name='default',
                source_handle=m.group(1),
                account_username=m.group(2)
            ).get('message')
        return "Say: Auto setup watch @blueskyhandle post to @facebookusername every 5 minutes"

    return (
        "Bluesky → Facebook vault.\n"
        "I can: login, fetch, save to vault, post now (by id), auto pilot, status.\n"
        "Examples:\n"
        "  Login with handle and app-password\n"
        "  Fetch 10 posts from @someone.bsky.social\n"
        "  Save them to vault\n"
        "  Post id 2\n"
        "  Auto setup watch zorrito post to myfacebook every 5 minutes\n"
        "  Start auto / Stop auto / Auto status"
    )


def _sanitize_reply_facebook_only(reply: str) -> str:
    """Rewrite any non-Bluesky/Facebook social names out of model replies."""
    if not reply or not isinstance(reply, str):
        return reply or ""
    text = reply
    replacements = [
        (r'(?i)\binstagram\b', 'Facebook'),
        (r'(?i)\bthreads\b', 'Facebook'),
        (r'(?i)\bI post to\s+\*?\*?Facebook\*?\*?', 'I post to **Facebook**'),
    ]
    for pat, repl in replacements:
        text = re.sub(pat, repl, text)
    return text


@app.route('/api/chat', methods=['POST'])
def api_chat():
    data = request.json or {}
    message = (data.get('message') or '').strip()
    history = data.get('history') or []
    session_id = data.get('session_id')
    chat_key = data.get('chat_key') or str(uuid.uuid4())

    if not message:
        return jsonify({"success": False, "error": "Empty message"}), 400

    # Keyword fallback first for reliability
    if not GEMINI_API_KEYS:
        reply = simple_fallback(message, session_id)
        return jsonify({
            "success": True,
            "reply": _sanitize_reply_facebook_only(reply),
            "tool_results": [],
            "chat_key": chat_key,
            "session_id": session_id
        })

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history[-10:]:
        if h.get('role') in ('user', 'assistant') and h.get('content'):
            # Drop old assistant turns that named other networks
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
            "session_id": session_id
        })

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
            # Capture session from login
            if result.get('session_id'):
                session_id = result['session_id']
            # Stash fetches
            if name == 'fetch_posts' and result.get('success') and session_id in sessions:
                sessions[session_id]['_last_fetched'] = result.get('posts') or []
                sessions[session_id]['_last_actor'] = result.get('actor')
            tool_results.append({"name": name, "result": result})
            messages.append({
                "role": "tool",
                "tool_call_id": tc.get('id'),
                "content": json.dumps(result)
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
        "session_id": session_id
    })


# ============================================================
# IMAGE UPLOAD → POST / SCHEDULE / VAULT (UI)
# ============================================================

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
            account_username=account_username
        )
        return jsonify(result), (200 if result.get('success') else 500)

    if not image_data:
        return jsonify({"success": False, "error": "Provide vault_id or image_data"}), 400

    resolved = resolve_facebook_account_id(account_id, account_username)
    if not resolved:
        return jsonify({"success": False, "error": "Could not resolve Facebook account. Connect one in Zernio."}), 400

    jpeg, err = data_url_to_jpeg_bytes(image_data)
    if not jpeg:
        return jsonify({"success": False, "error": err or "Invalid image"}), 400

    jpeg = fix_image_for_feed(jpeg)
    public_url = upload_media_to_zernio(jpeg)
    if not public_url:
        return jsonify({"success": False, "error": "Upload to Zernio failed"}), 500

    result = post_to_facebook(
        image_url=public_url,
        caption=caption or 'Posted via AI Vault',
        account_id=resolved,
        scheduled_time=None
    )
    if result.get('success'):
        result['message'] = "Posted to Facebook"
        result['caption'] = caption or 'Posted via AI Vault'
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
        return jsonify({"success": False, "error": "image_data is required"}), 400

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
        scheduled_time=scheduled_for
    )
    if result.get('success'):
        when = scheduled_for.strftime('%Y-%m-%d %H:%M') if scheduled_for else 'ASAP'
        result['message'] = f"Scheduled to Facebook for {when}"
        result['caption'] = caption
        result['account_id'] = resolved
        result['scheduled_for'] = when
    return jsonify(result), (200 if result.get('success') else 500)


@app.route('/api/vault/add-image', methods=['POST'])
def api_vault_add_image():
    data = request.json or {}
    image_data = data.get('image_data') or data.get('image')
    caption = (data.get('caption') or '').strip() or 'Saved from AI Vault'

    if not image_data:
        return jsonify({"success": False, "error": "image_data is required"}), 400

    public_url = None
    jpeg, err = data_url_to_jpeg_bytes(image_data)
    if jpeg:
        jpeg = fix_image_for_feed(jpeg)
        public_url = upload_media_to_zernio(jpeg)

    image_entry = {
        "url": public_url or image_data,
        "thumb": public_url or image_data,
        "alt": caption[:120]
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
            uri,
            'upload',
            'Manual upload',
            caption,
            Json([image_entry]),
            datetime.now().isoformat(),
            'manual',
            "Uploaded via UI · platform=facebook"
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
            "caption": caption
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


# ============================================================
# BASIC REST
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
    return jsonify(tool_auto_start())


@app.route('/api/auto/stop', methods=['POST'])
def api_auto_stop():
    return jsonify(tool_auto_stop())


@app.route('/')
def index():
    return send_from_directory('static', 'index.html')










if __name__ == '__main__':
    print("🚀 Bluesky AI Vault → Facebook starting...")
    if GEMINI_API_KEYS:
        print(f"✅ Gemini keys loaded: {len(GEMINI_API_KEYS)} (round-robin)")
    else:
        print("⚠️  No GEMINI_API_KEYS — using keyword fallback only")

    # Only run these on full server, not on Vercel
    if not IS_VERCEL:
        # ===== AUTO-RESTORE BLUESKY SESSION ON STARTUP =====
        try:
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT session_id, session_string, handle, display_name 
                    FROM sessions 
                    WHERE expires_at > CURRENT_TIMESTAMP 
                    ORDER BY last_used_at DESC 
                    LIMIT 1
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
                        print(f"✅ Auto-restored Bluesky session for @{handle}")
                        print(f"   Session ID: {session_id}")
                    except Exception as e:
                        print(f"⚠️ Could not restore session: {e}")
                else:
                    print("ℹ️ No saved Bluesky session found. Login required.")
        except Exception as e:
            print(f"⚠️ Session restore error: {e}")

        # ===== ZERNIO API KEYS =====
        zernio_status = ensure_zernio_keys_loaded()
        if not zernio_status.get('success'):
            print("⚠️  Facebook posting will fail until ZERNIO_API_KEY is set in .env")
        else:
            try:
                synced = refresh_all_zernio_accounts()
                print(f"✅ Synced {len(synced) if synced else 0} Facebook account(s)")
            except Exception as e:
                print(f"⚠️ Account sync: {e}")

        # ===== AUTO PILOT (disabled on Vercel) =====
        try:
            enabled = [c for c in _list_auto_configs() if c.get('enabled')]
            if enabled:
                start_result = start_auto_pilot()
                if start_result.get('success'):
                    print(f"🤖 Auto pilot resumed ({len(enabled)} pipeline(s) → Facebook)")
                else:
                    print(f"🤖 Auto pilot NOT started: {start_result.get('message')}")
            else:
                print("🤖 Auto pilot idle (enable via chat)")
        except Exception as e:
            print(f"Auto pilot init: {e}")

    port = int(os.environ.get('PORT', 10000))
    app.run(debug=False, host='0.0.0.0', port=port)