# ============================================================
# ANALYZE SPORT VAULT SOURCES
# ============================================================

import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json
from datetime import datetime

DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://neondb_owner:npg_3FJeskp5EoVg@ep-polished-sky-ayuedb1p-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
)

def analyze_sport_vault_sources():
    """Analyze all posts in Sport vault and show their sources"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        print("\n" + "="*70)
        print("📊 ANALYZE SPORT VAULT SOURCES")
        print("="*70)
        
        # 1. Total count
        cur.execute("""
            SELECT COUNT(*) as total FROM vault 
            WHERE handler_handle = 'Sport'
        """)
        total = cur.fetchone()['total']
        print(f"\n📦 Total posts in Sport vault: {total}")
        
        if total == 0:
            print("❌ No posts in Sport vault")
            cur.close()
            conn.close()
            return
        
        # 2. Get all unique authors (sources)
        cur.execute("""
            SELECT 
                author,
                COUNT(*) as post_count,
                MIN(saved_at) as first_post,
                MAX(saved_at) as last_post
            FROM vault 
            WHERE handler_handle = 'Sport'
            GROUP BY author
            ORDER BY post_count DESC
        """)
        sources = cur.fetchall()
        
        print(f"\n📋 Sources found: {len(sources)}")
        print("="*70)
        
        total_by_source = 0
        for s in sources:
            author = s.get('author')
            count = s.get('post_count')
            total_by_source += count
            first = s.get('first_post')
            last = s.get('last_post')
            
            print(f"\n📌 {author}")
            print(f"   Posts: {count}")
            print(f"   First: {first}")
            print(f"   Last: {last}")
        
        print("\n" + "="*70)
        print(f"📊 Total: {total_by_source} posts across {len(sources)} source(s)")
        
        # 3. Check if all posts are from the same source
        if len(sources) == 1:
            print(f"\n✅ ALL {total} posts are from ONE source: {sources[0].get('author')}")
        else:
            print(f"\n⚠️ Posts are from {len(sources)} different sources:")
            for s in sources:
                print(f"   • {s.get('author')}: {s.get('post_count')} posts")
        
        # 4. Show sample posts from each source
        print("\n" + "="*70)
        print("📝 SAMPLE POSTS FROM EACH SOURCE:")
        
        for s in sources:
            author = s.get('author')
            print(f"\n📌 {author}:")
            
            cur.execute("""
                SELECT id, text, images, created_at
                FROM vault 
                WHERE handler_handle = 'Sport' AND author = %s
                ORDER BY saved_at DESC
                LIMIT 3
            """, (author,))
            samples = cur.fetchall()
            
            for sample in samples:
                text = (sample.get('text') or '')[:60]
                img_count = len(sample.get('images') or []) if sample.get('images') else 0
                print(f"   #{sample.get('id')}: {text}... (📸{img_count})")
        
        # 5. Check if there are any posts from theathleticzone.bsky.social
        print("\n" + "="*70)
        print("🔍 SPECIFIC SOURCE CHECK:")
        
        cur.execute("""
            SELECT COUNT(*) as count FROM vault 
            WHERE handler_handle = 'Sport' 
            AND LOWER(author) LIKE '%theathleticzone%'
        """)
        athletic_count = cur.fetchone()['count']
        print(f"   Posts from theathleticzone.bsky.social: {athletic_count}")
        
        cur.execute("""
            SELECT COUNT(*) as count FROM vault 
            WHERE handler_handle = 'Sport' 
            AND LOWER(author) NOT LIKE '%theathleticzone%'
        """)
        other_count = cur.fetchone()['count']
        print(f"   Posts from OTHER sources: {other_count}")
        
        # 6. Show the actual sources list
        print("\n" + "="*70)
        print("📋 COMPLETE SOURCE LIST:")
        source_list = [s.get('author') for s in sources]
        for i, src in enumerate(source_list, 1):
            print(f"   {i}. {src}")
        
        cur.close()
        conn.close()
        
        print("\n" + "="*70)
        print("✅ ANALYSIS COMPLETE")
        print("="*70)
        
        return {
            "total": total,
            "unique_sources": len(sources),
            "sources": source_list,
            "all_same_source": len(sources) == 1,
            "source_counts": {s.get('author'): s.get('post_count') for s in sources}
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_sport_vault_sources()