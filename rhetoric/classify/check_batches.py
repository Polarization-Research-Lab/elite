#!/usr/bin/env python3
'''
Quick batch status checker - shows current status of all tracked batches
'''
import json, os
from batch_monitor import check_batch_status, list_all_batches

def quick_status_check():
    """Quick status check of tracked batches"""
    
    # Check if we have tracked batches
    if os.path.exists('batch_ids.json'):
        print("📋 TRACKED BATCHES:")
        with open('batch_ids.json', 'r') as f:
            data = json.load(f)
        
        for i, batch_group in enumerate(data.get('batches', []), 1):
            print(f"\n  Batch Group #{i} (submitted: {batch_group['created_at'][:19]}):")
            for batch_id in batch_group['batch_ids']:
                try:
                    status = check_batch_status(batch_id)
                    status_emoji = {
                        'validating': '🔍',
                        'in_progress': '⚙️',
                        'finalizing': '🏁', 
                        'completed': '✅',
                        'failed': '❌',
                        'expired': '⏰',
                        'cancelled': '🚫'
                    }.get(status['status'], '❓')
                    
                    print(f"    {status_emoji} {batch_id}: {status['status']}")
                    
                    if status['status'] == 'completed' and status['completed_at']:
                        print(f"        Completed: {status['completed_at']}")
                    
                except Exception as e:
                    print(f"    ❓ {batch_id}: Error checking status ({e})")
    else:
        print("📋 No tracked batches found (batch_ids.json doesn't exist)")
    
    # Also show all batches in your account
    print("\n🌐 ALL BATCHES IN YOUR ACCOUNT:")
    try:
        all_batches = list_all_batches()
        if not all_batches:
            print("  No batches found")
        else:
            for batch in all_batches[-10:]:  # Show last 10
                status_emoji = {
                    'validating': '🔍',
                    'in_progress': '⚙️', 
                    'finalizing': '🏁',
                    'completed': '✅',
                    'failed': '❌',
                    'expired': '⏰',
                    'cancelled': '🚫'
                }.get(batch['status'], '❓')
                
                print(f"  {status_emoji} {batch['id']}: {batch['status']}")
                if batch['metadata'] and 'description' in batch['metadata']:
                    print(f"      {batch['metadata']['description']}")
                print(f"      Created: {batch['created_at']}")
                
    except Exception as e:
        print(f"  Error fetching account batches: {e}")

def main():
    print("🔍 BATCH STATUS CHECKER\n")
    quick_status_check()
    
    print("\n💡 TIPS:")
    print("  • To auto-monitor: python batch_monitor.py --action monitor") 
    print("  • To submit new batches: python classify_batch.py")
    print("  • For detailed status: python batch_monitor.py --action list")

if __name__ == "__main__":
    main() 