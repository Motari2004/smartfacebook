# ============================================================
# RESET HEALTH POSTS TO UNPOSTED - WITH DETAILED STATISTICS
# ============================================================

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime

DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://neondb_owner:npg_3FJeskp5EoVg@ep-polished-sky-ayuedb1p-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
)

def get_pipeline_stats(pipeline_name):
    """Get detailed statistics for a pipeline"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        stats = {}
        
        # 1. Total posts in vault
        cur.execute("""
            SELECT COUNT(*) as total FROM vault 
            WHERE handler_handle = %s
        """, (pipeline_name,))
        stats['total_vault'] = cur.fetchone()['total']
        
        # 2. Posts by status
        cur.execute("""
            SELECT 
                COUNT(CASE WHEN p.status = 'posted' THEN 1 END) as posted,
                COUNT(CASE WHEN p.status = 'completed' THEN 1 END) as completed,
                COUNT(CASE WHEN p.status = 'scheduled' THEN 1 END) as scheduled,
                COUNT(CASE WHEN p.status = 'duplicate' THEN 1 END) as duplicate,
                COUNT(CASE WHEN p.status = 'failed' THEN 1 END) as failed,
                COUNT(CASE WHEN p.status IS NULL THEN 1 END) as unposted
            FROM vault v
            LEFT JOIN posted_posts p ON p.uri = v.uri AND p.platform = 'facebook'
            WHERE v.handler_handle = %s
        """, (pipeline_name,))
        row = cur.fetchone()
        stats['posted'] = row['posted'] or 0
        stats['completed'] = row['completed'] or 0
        stats['scheduled'] = row['scheduled'] or 0
        stats['duplicate'] = row['duplicate'] or 0
        stats['failed'] = row['failed'] or 0
        stats['unposted'] = row['unposted'] or 0
        
        # 3. Total posted (posted + completed)
        stats['total_posted'] = stats['posted'] + stats['completed']
        stats['total_to_reset'] = stats['total_posted'] + stats['duplicate']
        
        # 4. Get sample of posts to reset
        cur.execute("""
            SELECT v.id, v.text, v.images, p.status, p.posted_at
            FROM vault v
            JOIN posted_posts p ON p.uri = v.uri AND p.platform = 'facebook'
            WHERE v.handler_handle = %s
            AND p.status IN ('posted', 'completed', 'duplicate')
            ORDER BY p.posted_at DESC
            LIMIT 5
        """, (pipeline_name,))
        stats['samples'] = cur.fetchall()
        
        # 5. Check auto_seen entries
        cur.execute("""
            SELECT COUNT(*) as count FROM auto_seen
            WHERE config_name = %s
        """, (pipeline_name,))
        stats['auto_seen_count'] = cur.fetchone()['count']
        
        cur.close()
        conn.close()
        return stats
        
    except Exception as e:
        print(f"❌ Error getting stats: {e}")
        return None

def format_stats(stats, pipeline_name):
    """Format statistics for display"""
    lines = []
    lines.append("="*70)
    lines.append(f"📊 PIPELINE STATUS: {pipeline_name}")
    lines.append("="*70)
    lines.append("")
    lines.append(f"📦 Total posts in vault: {stats['total_vault']}")
    lines.append("")
    lines.append("📋 POST STATUS BREAKDOWN:")
    lines.append(f"   ✅ Posted (completed): {stats['completed']}")
    lines.append(f"   ✅ Posted (pending):   {stats['posted']}")
    lines.append(f"   📅 Scheduled:          {stats['scheduled']}")
    lines.append(f"   ⚠️ Duplicate:          {stats['duplicate']}")
    lines.append(f"   ❌ Failed:             {stats['failed']}")
    lines.append(f"   📭 Unposted:           {stats['unposted']}")
    lines.append("")
    lines.append("="*70)
    lines.append(f"🔄 POSTS TO BE RESET: {stats['total_to_reset']}")
    lines.append(f"   • Posted posts: {stats['total_posted']}")
    lines.append(f"   • Duplicate posts: {stats['duplicate']}")
    lines.append("="*70)
    
    if stats['samples']:
        lines.append("")
        lines.append("📝 SAMPLE POSTS TO RESET (most recent):")
        for i, sample in enumerate(stats['samples'], 1):
            text = (sample['text'] or '')[:60]
            status = sample['status']
            posted_at = sample['posted_at'].strftime('%Y-%m-%d %H:%M') if sample['posted_at'] else 'Unknown'
            status_icon = "⚠️" if status == 'duplicate' else "📤"
            lines.append(f"   {i}. {status_icon} #{sample['id']} [{status}] {text}...")
            lines.append(f"      Posted: {posted_at}")
    
    if stats['auto_seen_count'] > 0:
        lines.append("")
        lines.append(f"👁️ auto_seen entries: {stats['auto_seen_count']} (will be cleared)")
    
    lines.append("")
    lines.append("="*70)
    
    return "\n".join(lines)

def reset_pipeline(pipeline_name, confirm=False):
    """
    Reset a pipeline's posted posts to unposted so they can be reposted.
    Shows detailed statistics before and after.
    """
    if not pipeline_name:
        return {"success": False, "error": "Pipeline name required"}
    
    # Verify pipeline exists
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT name FROM auto_config WHERE name = %s", (pipeline_name,))
        if not cur.fetchone():
            cur.close()
            conn.close()
            return {"success": False, "error": f"Pipeline '{pipeline_name}' not found"}
        cur.close()
        conn.close()
    except Exception as e:
        return {"success": False, "error": f"Error checking pipeline: {e}"}
    
    # Get statistics before reset
    print("\n📊 Gathering statistics...")
    stats = get_pipeline_stats(pipeline_name)
    if not stats:
        return {"success": False, "error": "Could not get pipeline statistics"}
    
    # Display statistics
    print(format_stats(stats, pipeline_name))
    
    # Check if there's anything to reset
    if stats['total_to_reset'] == 0:
        return {
            "success": True,
            "message": f"✅ Pipeline '{pipeline_name}' has no posts to reset. All posts are already unposted.",
            "stats": stats,
            "reset_count": 0
        }
    
    # Ask for confirmation
    if not confirm:
        print(f"\n⚠️ This will reset {stats['total_to_reset']} posts for '{pipeline_name}'")
        print("   They will be reposted to the CORRECT account")
        print("\n💡 To proceed, run: reset_pipeline('" + pipeline_name + "', confirm=True)")
        return {
            "success": False,
            "error": "Confirmation required",
            "message": f"⚠️ This will reset {stats['total_to_reset']} posts for '{pipeline_name}'. Run again with confirm=True to proceed.",
            "stats": stats,
            "count": stats['total_to_reset']
        }
    
    # Perform the reset
    print("\n🔄 Resetting posts...")
    
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    # 1. Delete from posted_posts
    cur.execute("""
        DELETE FROM posted_posts p
        USING vault v
        WHERE p.vault_id = v.id
        AND v.handler_handle = %s
        AND p.platform = 'facebook'
    """, (pipeline_name,))
    posted_deleted = cur.rowcount
    print(f"   ✅ Deleted {posted_deleted} entries from posted_posts")
    
    # 2. Delete from auto_seen
    cur.execute("""
        DELETE FROM auto_seen s
        WHERE s.config_name = %s
    """, (pipeline_name,))
    seen_deleted = cur.rowcount
    print(f"   ✅ Deleted {seen_deleted} entries from auto_seen")
    
    # 3. Also clear any failed entries
    cur.execute("""
        DELETE FROM posted_posts p
        USING vault v
        WHERE p.vault_id = v.id
        AND v.handler_handle = %s
        AND p.status = 'failed'
    """, (pipeline_name,))
    failed_deleted = cur.rowcount
    if failed_deleted > 0:
        print(f"   ✅ Also cleared {failed_deleted} failed entries")
    
    conn.commit()
    cur.close()
    conn.close()
    
    # Get statistics after reset
    print("\n📊 Gathering post-reset statistics...")
    new_stats = get_pipeline_stats(pipeline_name)
    
    # Display final result
    print("\n" + "="*70)
    print(f"✅ RESET COMPLETE: {pipeline_name}")
    print("="*70)
    print(f"\n🔄 Posts reset to unposted: {stats['total_to_reset']}")
    print(f"   • Posted posts cleared: {stats['total_posted']}")
    print(f"   • Duplicate posts cleared: {stats['duplicate']}")
    print(f"\n📊 NEW STATUS:")
    print(f"   📭 Unposted posts now: {new_stats['unposted']}")
    print(f"   📦 Total posts in vault: {new_stats['total_vault']}")
    print(f"   🔄 auto_seen cleared: {seen_deleted}")
    print("\n" + "="*70)
    print("\n💡 Next steps:")
    print(f"   1. Verify pipeline destination: list pipelines")
    print(f"   2. Run: run {pipeline_name} pipeline now")
    print(f"   3. Check results: show {pipeline_name} posted today")
    print("="*70 + "\n")
    
    return {
        "success": True,
        "message": f"✅ Reset {stats['total_to_reset']} posts for '{pipeline_name}' to unposted.",
        "stats": stats,
        "new_stats": new_stats,
        "reset_count": stats['total_to_reset'],
        "posted_cleared": stats['total_posted'],
        "duplicate_cleared": stats['duplicate'],
        "seen_cleared": seen_deleted,
        "now_unposted": new_stats['unposted']
    }

def main():
    """Main function to run the reset"""
    print("\n" + "="*70)
    print("🔄 HEALTH PIPELINE RESET TOOL")
    print("="*70)
    print("\nThis tool will reset Health's posted posts to unposted")
    print("so they can be reposted to the CORRECT account (Global Health Hub)")
    print("\n" + "="*70 + "\n")
    
    # Show current stats first
    stats = get_pipeline_stats('Health')
    if stats:
        print(format_stats(stats, 'Health'))
    
    # Ask for confirmation
    print("\n" + "="*70)
    print(f"⚠️ This will reset {stats['total_to_reset']} posts for Health")
    print("   They will be reposted to Global Health Hub")
    print("="*70)
    
    confirm = input("\nType 'YES' to confirm reset: ").strip()
    
    if confirm != 'YES':
        print("❌ Reset cancelled.")
        return
    
    # Perform reset
    result = reset_pipeline('Health', confirm=True)
    
    if result['success']:
        print("\n✅ Reset completed successfully!")
    else:
        print(f"\n❌ Error: {result.get('error')}")

if __name__ == "__main__":
    main()