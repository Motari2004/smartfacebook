# ============================================================
# FILTER SPORT VAULT - KEEP ONLY theathletic.com POSTS
# ============================================================

import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json

DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://neondb_owner:npg_3FJeskp5EoVg@ep-polished-sky-ayuedb1p-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
)

def filter_sport_vault_keep_theathletic():
    """Keep only theathletic.com posts in Sport vault, delete everything else"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        print("\n" + "="*70)
        print("🔍 FILTER SPORT VAULT - KEEP ONLY theathletic.com")
        print("="*70)
        
        # 1. Current counts
        cur.execute("""
            SELECT COUNT(*) as total FROM vault 
            WHERE handler_handle = 'Sport'
        """)
        total = cur.fetchone()['total']
        print(f"\n📦 Total posts in Sport vault: {total}")
        
        cur.execute("""
            SELECT COUNT(*) as count FROM vault 
            WHERE handler_handle = 'Sport' 
            AND author = 'theathletic.com'
        """)
        athletic_count = cur.fetchone()['count']
        print(f"📰 Posts from theathletic.com: {athletic_count}")
        
        cur.execute("""
            SELECT COUNT(*) as count FROM vault 
            WHERE handler_handle = 'Sport' 
            AND author != 'theathletic.com'
        """)
        other_count = cur.fetchone()['count']
        print(f"🗑️ Posts from other sources: {other_count}")
        
        if other_count == 0:
            print("\n✅ Sport vault already only has theathletic.com posts!")
            cur.close()
            conn.close()
            return
        
        # 2. Show what will be deleted
        print("\n📋 Posts to DELETE (other sources):")
        cur.execute("""
            SELECT author, COUNT(*) as count
            FROM vault 
            WHERE handler_handle = 'Sport' 
            AND author != 'theathletic.com'
            GROUP BY author
            ORDER BY count DESC
        """)
        to_delete = cur.fetchall()
        
        for d in to_delete:
            print(f"   • {d.get('author')}: {d.get('count')} posts")
        
        # 3. Show what will be kept
        print("\n📋 Posts to KEEP (theathletic.com):")
        cur.execute("""
            SELECT id, text, created_at
            FROM vault 
            WHERE handler_handle = 'Sport' 
            AND author = 'theathletic.com'
            ORDER BY saved_at DESC
            LIMIT 5
        """)
        samples = cur.fetchall()
        
        for s in samples:
            text = (s.get('text') or '')[:60]
            print(f"   #{s.get('id')}: {text}...")
        
        # 4. Confirm
        print("\n" + "="*70)
        print(f"⚠️ This will DELETE {other_count} posts from Sport vault")
        print(f"   Keep: {athletic_count} posts (theathletic.com)")
        print("="*70)
        
        confirm = input("\nType 'YES' to confirm deletion: ").strip()
        
        if confirm != 'YES':
            print("❌ Operation cancelled.")
            cur.close()
            conn.close()
            return
        
        # 5. Delete non-theathletic.com posts
        print(f"\n🔄 Deleting {other_count} posts...")
        
        # Delete from posted_posts first (to maintain referential integrity)
        cur.execute("""
            DELETE FROM posted_posts p
            USING vault v
            WHERE p.vault_id = v.id
            AND v.handler_handle = 'Sport'
            AND v.author != 'theathletic.com'
        """)
        posted_deleted = cur.rowcount
        print(f"   ✅ Deleted {posted_deleted} from posted_posts")
        
        # Delete from vault
        cur.execute("""
            DELETE FROM vault 
            WHERE handler_handle = 'Sport' 
            AND author != 'theathletic.com'
        """)
        vault_deleted = cur.rowcount
        
        # 6. Update auto_seen
        cur.execute("""
            DELETE FROM auto_seen s
            WHERE s.config_name = 'Sport'
            AND NOT EXISTS (
                SELECT 1 FROM vault v
                WHERE v.uri = s.uri
                AND v.handler_handle = 'Sport'
            )
        """)
        seen_deleted = cur.rowcount
        
        conn.commit()
        
        # 7. Verify
        print("\n📊 Verification:")
        cur.execute("""
            SELECT COUNT(*) as total FROM vault 
            WHERE handler_handle = 'Sport'
        """)
        new_total = cur.fetchone()['total']
        print(f"   Posts remaining in Sport: {new_total}")
        
        cur.execute("""
            SELECT COUNT(*) as count FROM vault 
            WHERE handler_handle = 'Sport' 
            AND author = 'theathletic.com'
        """)
        new_athletic = cur.fetchone()['count']
        print(f"   Posts from theathletic.com: {new_athletic}")
        
        cur.execute("""
            SELECT COUNT(*) as count FROM vault 
            WHERE handler_handle = 'Sport' 
            AND author != 'theathletic.com'
        """)
        new_other = cur.fetchone()['count']
        print(f"   Posts from other sources: {new_other}")
        
        cur.close()
        conn.close()
        
        print("\n" + "="*70)
        print("✅ FILTER COMPLETE!")
        print("="*70)
        print(f"\n📊 Sport vault now has {new_total} posts")
        print(f"   All from: theathletic.com")
        print("\n💡 Next steps:")
        print("   1. Update Sport pipeline source:")
        print("      auto setup name=Sport source_handles=['theathletic.com'] account_username='The Athletic Zone' enabled=true")
        print("   2. Run: run Sport pipeline now")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback() if conn else None

if __name__ == "__main__":
    filter_sport_vault_keep_theathletic()