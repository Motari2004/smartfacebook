# ============================================================
# MIGRATE SPORT VAULT TO PHOTOGRAPHER
# ============================================================

import psycopg2
from psycopg2.extras import RealDictCursor, Json
import os
import json
from datetime import datetime

DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://neondb_owner:npg_3FJeskp5EoVg@ep-polished-sky-ayuedb1p-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
)

def migrate_sport_to_photographer():
    """Migrate all Sport vault posts to a new Photographer vault"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        print("\n" + "="*70)
        print("📦 MIGRATE SPORT → PHOTOGRAPHER")
        print("="*70)
        
        # 1. Count posts in Sport
        cur.execute("""
            SELECT COUNT(*) as total FROM vault 
            WHERE handler_handle = 'Sport'
        """)
        total = cur.fetchone()['total']
        print(f"\n📊 Found {total} posts in Sport vault")
        
        if total == 0:
            print("❌ No posts to migrate")
            cur.close()
            conn.close()
            return
        
        # 2. Check if Photographer pipeline exists
        cur.execute("""
            SELECT name FROM auto_config WHERE name = 'Photographer'
        """)
        exists = cur.fetchone()
        
        if not exists:
            print("\n⚠️ Photographer pipeline doesn't exist yet.")
            print("   Creating it now...")
            
            # Create the pipeline
            cur.execute("""
                INSERT INTO auto_config (
                    name, enabled, source_handle, source_handles, niche,
                    account_username, account_id, content_type, media_only,
                    include_reposts, max_posts_per_run, last_result, updated_at
                ) VALUES (
                    'Photographer', TRUE, 'theathleticzone.bsky.social', 
                    '["theathleticzone.bsky.social"]', 'Photographer',
                    'The Athletic Zone', '6a8c747877555aae01fb001b', 'feed',
                    TRUE, FALSE, 2, 'Created for Sport migration', CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            print("   ✅ Photographer pipeline created!")
        else:
            print(f"\n✅ Photographer pipeline already exists")
        
        # 3. Get the new Photographer pipeline config
        cur.execute("""
            SELECT * FROM auto_config WHERE name = 'Photographer'
        """)
        photographer_config = cur.fetchone()
        print(f"\n📋 Photographer config:")
        print(f"   Name: {photographer_config.get('name')}")
        print(f"   Enabled: {photographer_config.get('enabled')}")
        print(f"   Account: {photographer_config.get('account_username')}")
        print(f"   Account ID: {photographer_config.get('account_id')}")
        
        # 4. Show sample of posts to migrate
        print(f"\n📋 Sample posts to migrate:")
        cur.execute("""
            SELECT id, text, author, created_at, images
            FROM vault 
            WHERE handler_handle = 'Sport'
            ORDER BY saved_at DESC
            LIMIT 5
        """)
        samples = cur.fetchall()
        
        for s in samples:
            text = (s.get('text') or '')[:60]
            print(f"   #{s.get('id')}: {text}...")
        
        # 5. Ask for confirmation
        print("\n" + "="*70)
        print(f"⚠️ This will migrate {total} posts from Sport to Photographer")
        print("   Original Sport posts will be REMOVED from Sport vault")
        print("="*70)
        
        confirm = input("\nType 'YES' to confirm migration: ").strip()
        
        if confirm != 'YES':
            print("❌ Migration cancelled.")
            cur.close()
            conn.close()
            return
        
        # 6. Perform the migration
        print(f"\n🔄 Migrating {total} posts...")
        
        # UPDATE: Change handler_handle from 'Sport' to 'Photographer'
        cur.execute("""
            UPDATE vault 
            SET handler_handle = 'Photographer',
                notes = COALESCE(notes, '') || '\n[Migrated from Sport on ' || CURRENT_TIMESTAMP || ']'
            WHERE handler_handle = 'Sport'
        """)
        migrated = cur.rowcount
        print(f"   ✅ Migrated {migrated} posts from Sport to Photographer")
        
        # 7. Update any auto_seen entries
        cur.execute("""
            UPDATE auto_seen 
            SET config_name = 'Photographer'
            WHERE config_name = 'Sport'
        """)
        seen_updated = cur.rowcount
        print(f"   ✅ Updated {seen_updated} auto_seen entries")
        
        # 8. Update any posted_posts (if any)
        cur.execute("""
            UPDATE posted_posts p
            SET metadata = jsonb_set(
                COALESCE(metadata, '{}'::jsonb),
                '{migrated_from}',
                '"Sport"'
            )
            FROM vault v
            WHERE p.vault_id = v.id 
            AND v.handler_handle = 'Photographer'
        """)
        
        conn.commit()
        
        # 9. Verify the migration
        print("\n📊 Verification:")
        
        cur.execute("""
            SELECT COUNT(*) as sport_count FROM vault 
            WHERE handler_handle = 'Sport'
        """)
        sport_count = cur.fetchone()['sport_count']
        print(f"   Posts remaining in Sport: {sport_count}")
        
        cur.execute("""
            SELECT COUNT(*) as photo_count FROM vault 
            WHERE handler_handle = 'Photographer'
        """)
        photo_count = cur.fetchone()['photo_count']
        print(f"   Posts in Photographer: {photo_count}")
        
        # 10. Show sample of migrated posts
        print(f"\n📋 Sample posts in Photographer:")
        cur.execute("""
            SELECT id, text, author, saved_at
            FROM vault 
            WHERE handler_handle = 'Photographer'
            ORDER BY saved_at DESC
            LIMIT 5
        """)
        samples = cur.fetchall()
        
        for s in samples:
            text = (s.get('text') or '')[:60]
            print(f"   #{s.get('id')}: {text}...")
        
        cur.close()
        conn.close()
        
        print("\n" + "="*70)
        print("✅ MIGRATION COMPLETE!")
        print("="*70)
        print(f"\n📦 {photo_count} posts now in Photographer vault")
        print(f"🔴 {sport_count} posts remain in Sport (should be 0)")
        print("\n💡 Next steps:")
        print("   1. Update Sport pipeline source to something new, or disable it")
        print("   2. Enable Photographer pipeline: start pipeline Photographer")
        print("   3. Test: run Photographer pipeline now")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback() if conn else None

if __name__ == "__main__":
    migrate_sport_to_photographer()