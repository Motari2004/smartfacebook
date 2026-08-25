# ============================================================
# DEBUG SCRIPT - Check Health Pipeline Configuration
# ============================================================

import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json

# Database connection
DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://neondb_owner:npg_3FJeskp5EoVg@ep-polished-sky-ayuedb1p-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
)

def debug_health_pipeline():
    """Debug what the Health pipeline is using"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        print("\n" + "="*70)
        print("🔍 HEALTH PIPELINE DEBUG")
        print("="*70)
        
        # 1. Check auto_config for Health
        print("\n📋 1. AUTO_CONFIG (Health pipeline settings):")
        cur.execute("""
            SELECT * FROM auto_config 
            WHERE name = 'Health'
        """)
        health_config = cur.fetchone()
        
        if health_config:
            print(f"   Name: {health_config.get('name')}")
            print(f"   Enabled: {health_config.get('enabled')}")
            print(f"   Account Username: '{health_config.get('account_username')}'")
            print(f"   Account ID: '{health_config.get('account_id')}'")
            print(f"   Source Handles: {health_config.get('source_handles')}")
            print(f"   Max Posts: {health_config.get('max_posts_per_run')}")
            print(f"   Last Result: {health_config.get('last_result')}")
            print(f"   Last Error: {health_config.get('last_error')}")
        else:
            print("   ❌ Health pipeline not found in auto_config!")
        
        # 2. Check zernio_accounts for Global Health Hub
        print("\n📋 2. ZERNIO_ACCOUNTS (Global Health Hub):")
        cur.execute("""
            SELECT account_id, username, display_name, api_key, is_active 
            FROM zernio_accounts 
            WHERE LOWER(TRIM(display_name)) LIKE '%global health hub%' 
               OR LOWER(TRIM(username)) LIKE '%global health hub%'
            ORDER BY is_active DESC
        """)
        health_accounts = cur.fetchall()
        
        if health_accounts:
            for acc in health_accounts:
                print(f"   Account ID: {acc.get('account_id')}")
                print(f"   Username: '{acc.get('username')}'")
                print(f"   Display Name: '{acc.get('display_name')}'")
                print(f"   Is Active: {acc.get('is_active')}")
                print(f"   API Key: {acc.get('api_key')[:20]}..." if acc.get('api_key') else "   API Key: None")
                print("   ---")
        else:
            print("   ❌ Global Health Hub not found in zernio_accounts!")
        
        # 3. Check all Facebook accounts
        print("\n📋 3. ALL FACEBOOK ACCOUNTS:")
        cur.execute("""
            SELECT account_id, username, display_name, is_active 
            FROM zernio_accounts 
            WHERE platform = 'facebook' 
            ORDER BY is_active DESC, display_name
        """)
        all_accounts = cur.fetchall()
        
        if all_accounts:
            for i, acc in enumerate(all_accounts, 1):
                status = "🟢" if acc.get('is_active') else "🔴"
                print(f"   {i}. {status} {acc.get('display_name')} (@{acc.get('username')})")
                print(f"      ID: {acc.get('account_id')}")
        else:
            print("   ❌ No Facebook accounts found!")
        
        # 4. Check if account_id in Health matches any zernio_account
        print("\n📋 4. ACCOUNT ID MATCH CHECK:")
        health_account_id = health_config.get('account_id') if health_config else None
        
        if health_account_id:
            cur.execute("""
                SELECT account_id, username, display_name, is_active 
                FROM zernio_accounts 
                WHERE account_id = %s AND platform = 'facebook'
            """, (health_account_id,))
            match = cur.fetchone()
            
            if match:
                print(f"   ✅ Health's account_id matches:")
                print(f"      Display Name: {match.get('display_name')}")
                print(f"      Username: {match.get('username')}")
                print(f"      Is Active: {match.get('is_active')}")
            else:
                print(f"   ❌ Health's account_id '{health_account_id}' does NOT match any active Facebook account!")
                print(f"   ⚠️ This is likely the problem - Health is trying to post to an invalid account!")
        else:
            print("   ⚠️ Health has no account_id set!")
        
        # 5. Check what's in posted_posts for Health
        print("\n📋 5. RECENT POSTED POSTS (Health):")
        cur.execute("""
            SELECT p.id, p.vault_id, p.status, p.platform_post_id, p.posted_at, 
                   v.text, v.uri
            FROM posted_posts p
            LEFT JOIN vault v ON v.id = p.vault_id
            WHERE p.platform = 'facebook'
            ORDER BY p.posted_at DESC
            LIMIT 10
        """)
        recent_posts = cur.fetchall()
        
        if recent_posts:
            for p in recent_posts:
                status = p.get('status')
                status_icon = "✅" if status == 'posted' else "⚠️" if status == 'duplicate' else "❌"
                text = (p.get('text') or '')[:50]
                print(f"   {status_icon} #{p.get('vault_id')} {status}: {text}...")
        else:
            print("   No recent posts found")
        
        # 6. Count Health's unposted posts
        print("\n📋 6. HEALTH VAULT STATS:")
        cur.execute("""
            SELECT COUNT(*) as total FROM vault WHERE handler_handle = 'Health'
        """)
        total = cur.fetchone()['total']
        
        cur.execute("""
            SELECT COUNT(*) as unposted FROM vault v
            WHERE v.handler_handle = 'Health'
            AND NOT EXISTS (
                SELECT 1 FROM posted_posts p
                WHERE p.uri = v.uri AND p.platform = 'facebook'
                  AND p.status IN ('completed', 'posted', 'duplicate')
            )
        """)
        unposted = cur.fetchone()['unposted']
        
        print(f"   Total posts in Health vault: {total}")
        print(f"   Unposted posts: {unposted}")
        
        # 7. Show a sample of Health's vault posts
        print("\n📋 7. SAMPLE HEALTH VAULT POSTS:")
        cur.execute("""
            SELECT id, text, images, created_at, saved_at
            FROM vault
            WHERE handler_handle = 'Health'
            ORDER BY saved_at DESC
            LIMIT 5
        """)
        samples = cur.fetchall()
        
        if samples:
            for s in samples:
                text = (s.get('text') or '')[:60]
                images = s.get('images')
                if isinstance(images, str):
                    try:
                        images = json.loads(images)
                    except:
                        pass
                img_count = len(images) if isinstance(images, list) else 0
                img_preview = ""
                if images and isinstance(images, list) and len(images) > 0:
                    first = images[0]
                    if isinstance(first, dict):
                        img_preview = first.get('url', '')[:60]
                    else:
                        img_preview = str(first)[:60]
                print(f"   #{s.get('id')}: {text[:40]}...")
                print(f"      Images: {img_count} | First: {img_preview}")
        else:
            print("   No posts in Health vault")
        
        cur.close()
        conn.close()
        
        print("\n" + "="*70)
        print("🔍 DEBUG COMPLETE")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

# Run the debug
if __name__ == "__main__":
    debug_health_pipeline()