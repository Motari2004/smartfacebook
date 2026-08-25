# ============================================================
# DEBUG SCRIPT - Check ALL Pipelines Configuration
# ============================================================

import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json
from datetime import datetime

# Database connection
DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://neondb_owner:npg_3FJeskp5EoVg@ep-polished-sky-ayuedb1p-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
)

def debug_all_pipelines():
    """Debug ALL pipelines configuration"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        print("\n" + "="*70)
        print("🔍 ALL PIPELINES DEBUG")
        print("="*70)
        
        # 1. Get ALL auto_configs
        print("\n📋 1. ALL AUTO_CONFIGS:")
        cur.execute("""
            SELECT * FROM auto_config 
            ORDER BY name
        """)
        all_configs = cur.fetchall()
        
        if not all_configs:
            print("   ❌ No pipelines found in auto_config!")
        else:
            print(f"   Found {len(all_configs)} pipeline(s)\n")
            
            for cfg in all_configs:
                print(f"   {'='*50}")
                print(f"   📌 PIPELINE: {cfg.get('name')}")
                print(f"      Enabled: {'🟢 YES' if cfg.get('enabled') else '🔴 NO'}")
                print(f"      Account Username: '{cfg.get('account_username')}'")
                print(f"      Account ID: '{cfg.get('account_id')}'")
                print(f"      Source Handles: {cfg.get('source_handles')}")
                print(f"      Niche: {cfg.get('niche')}")
                print(f"      Max Posts: {cfg.get('max_posts_per_run')}")
                print(f"      Content Type: {cfg.get('content_type')}")
                print(f"      Media Only: {cfg.get('media_only')}")
                print(f"      Include Reposts: {cfg.get('include_reposts')}")
                print(f"      Last Run: {cfg.get('last_run_at')}")
                print(f"      Last Result: {cfg.get('last_result')}")
                print(f"      Last Error: {cfg.get('last_error')}")
                print(f"      Updated: {cfg.get('updated_at')}")
        
        # 2. Get ALL Zernio accounts
        print("\n" + "="*70)
        print("📋 2. ALL ZERNIO ACCOUNTS:")
        cur.execute("""
            SELECT account_id, platform, username, display_name, is_active, api_key
            FROM zernio_accounts 
            ORDER BY platform, display_name
        """)
        all_accounts = cur.fetchall()
        
        if not all_accounts:
            print("   ❌ No Zernio accounts found!")
        else:
            print(f"   Found {len(all_accounts)} Zernio account(s)\n")
            for acc in all_accounts:
                status = "🟢 ACTIVE" if acc.get('is_active') else "🔴 INACTIVE"
                platform = acc.get('platform', 'unknown')
                print(f"   📱 {platform.upper()}: {acc.get('display_name')} (@{acc.get('username')})")
                print(f"      Account ID: {acc.get('account_id')}")
                print(f"      Status: {status}")
                if acc.get('api_key'):
                    print(f"      API Key: {acc.get('api_key')[:20]}...")
                print("")
        
        # 3. Match pipelines to accounts
        print("\n" + "="*70)
        print("📋 3. PIPELINE TO ACCOUNT MAPPING:")
        
        for cfg in all_configs:
            name = cfg.get('name')
            account_id = cfg.get('account_id')
            account_username = cfg.get('account_username')
            
            print(f"\n   📌 {name}:")
            
            if account_id:
                # Check if account exists
                cur.execute("""
                    SELECT platform, username, display_name, is_active 
                    FROM zernio_accounts 
                    WHERE account_id = %s
                """, (account_id,))
                match = cur.fetchone()
                
                if match:
                    print(f"      ✅ Account found: {match.get('display_name')} (@{match.get('username')}) - {match.get('platform').upper()}")
                    print(f"      Status: {'🟢 ACTIVE' if match.get('is_active') else '🔴 INACTIVE'}")
                else:
                    print(f"      ❌ Account ID '{account_id}' NOT FOUND in zernio_accounts!")
            elif account_username:
                # Try to find by username
                cur.execute("""
                    SELECT account_id, platform, username, display_name, is_active 
                    FROM zernio_accounts 
                    WHERE LOWER(username) = LOWER(%s) OR LOWER(display_name) = LOWER(%s)
                """, (account_username, account_username))
                match = cur.fetchone()
                
                if match:
                    print(f"      ✅ Found by username: {match.get('display_name')} (@{match.get('username')}) - {match.get('platform').upper()}")
                    print(f"      Account ID: {match.get('account_id')}")
                    print(f"      Status: {'🟢 ACTIVE' if match.get('is_active') else '🔴 INACTIVE'}")
                    print(f"      ⚠️ Pipeline has username but not account_id - consider updating with account_id")
                else:
                    print(f"      ⚠️ Username '{account_username}' not found in zernio_accounts!")
            else:
                print(f"      ⚠️ No account configured!")
        
        # 4. Count posts per pipeline
        print("\n" + "="*70)
        print("📋 4. VAULT STATS PER PIPELINE:")
        
        for cfg in all_configs:
            name = cfg.get('name')
            
            # Total posts
            cur.execute("""
                SELECT COUNT(*) as total FROM vault WHERE handler_handle = %s
            """, (name,))
            total = cur.fetchone()['total']
            
            # Unposted posts
            cur.execute("""
                SELECT COUNT(*) as unposted FROM vault v
                WHERE v.handler_handle = %s
                AND NOT EXISTS (
                    SELECT 1 FROM posted_posts p
                    WHERE p.uri = v.uri AND p.status IN ('completed', 'posted', 'duplicate')
                )
            """, (name,))
            unposted = cur.fetchone()['unposted']
            
            # Posted posts
            cur.execute("""
                SELECT COUNT(*) as posted FROM vault v
                WHERE v.handler_handle = %s
                AND EXISTS (
                    SELECT 1 FROM posted_posts p
                    WHERE p.uri = v.uri AND p.status IN ('completed', 'posted')
                )
            """, (name,))
            posted = cur.fetchone()['posted']
            
            print(f"\n   📌 {name}:")
            print(f"      Total: {total} posts")
            print(f"      Unposted: {unposted} posts")
            print(f"      Posted: {posted} posts")
            
            if unposted > 0 and cfg.get('enabled') and cfg.get('account_id'):
                print(f"      ✅ Ready to post {unposted} posts to {cfg.get('account_username')}")
            elif unposted > 0 and not cfg.get('enabled'):
                print(f"      ⚠️ {unposted} posts available but pipeline is DISABLED")
            elif unposted > 0 and not cfg.get('account_id'):
                print(f"      ⚠️ {unposted} posts available but NO ACCOUNT CONFIGURED")
            else:
                print(f"      ℹ️ No posts available")
        
        # 5. Check for orphaned accounts
        print("\n" + "="*70)
        print("📋 5. ORPHANED ACCOUNTS (in zernio_accounts but not used by any pipeline):")
        
        # Get all account IDs used by pipelines
        used_account_ids = set()
        for cfg in all_configs:
            if cfg.get('account_id'):
                used_account_ids.add(cfg.get('account_id'))
        
        cur.execute("""
            SELECT account_id, platform, username, display_name, is_active
            FROM zernio_accounts
            WHERE is_active = TRUE
        """)
        all_active = cur.fetchall()
        
        orphans = [acc for acc in all_active if acc.get('account_id') not in used_account_ids]
        
        if orphans:
            print(f"   Found {len(orphans)} orphaned account(s):")
            for acc in orphans:
                print(f"      📱 {acc.get('display_name')} (@{acc.get('username')}) - {acc.get('platform').upper()}")
                print(f"         Account ID: {acc.get('account_id')}")
        else:
            print("   ✅ All active accounts are being used by pipelines!")
        
        # 6. Check cron status
        print("\n" + "="*70)
        print("📋 6. CRON STATUS:")
        cur.execute("SELECT value FROM app_settings WHERE key = 'cron_enabled'")
        row = cur.fetchone()
        
        if row:
            cron_enabled = row['value'].lower() == 'true'
            print(f"   Cron State: {'🟢 ENABLED' if cron_enabled else '🔴 DISABLED'}")
        else:
            print("   ⚠️ Cron state not set (default: ENABLED)")
        
        # 7. Check for errors
        print("\n" + "="*70)
        print("📋 7. ERROR SUMMARY:")
        
        has_errors = False
        for cfg in all_configs:
            if cfg.get('last_error'):
                has_errors = True
                print(f"   ❌ {cfg.get('name')}: {cfg.get('last_error')}")
        
        if not has_errors:
            print("   ✅ No errors found in any pipeline")
        
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
    debug_all_pipelines()