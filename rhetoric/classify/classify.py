'''
Actually classify the data - OPTIMIZED BATCH API VERSION
Uses system prompts for better efficiency and lower costs
'''
# Python Standard Library
import sys, json, urllib, datetime, os, time
import argparse
# External Dependencies
import dotenv
import numpy as np 
import pandas as pd

import sqlalchemy as sql
import dataset
import ibis
from ibis import _

import hjson

# Internal Dependencies
import llms
import prompt
from batch_monitor import save_batch_ids

dotenv.load_dotenv('../../../env')
if 'PATH_TO_SECRETS' in os.environ:
    print(os.environ['PATH_TO_SECRETS'])
    dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
else:
    print("Warning: PATH_TO_SECRETS environment variable not found")

## DB Credentials - lazy initialization
def get_db_connection():
    """Get database connection when needed"""
    params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
    conn = ibis.mysql.connect(
        host = os.environ['DB_HOST'],
        user = os.environ['DB_USER'],
        password = os.environ['DB_PASSWORD'],
        database = 'elite',
    )
    return conn, params

def prepare_batch_data_optimized(data):
    """Prepare data for optimized batch processing with system prompts"""
    # Filter out empty or null text before processing
    initial_count = len(data)
    
    # Remove rows with empty, null, or whitespace-only text
    data = data[data['text'].notna()]  # Remove null/NaN
    data = data[data['text'].str.strip() != '']  # Remove empty or whitespace-only
    
    filtered_count = len(data)
    if filtered_count < initial_count:
        removed_count = initial_count - filtered_count
        print(f"⚠️  Filtered out {removed_count} items with empty/null text")
    
    # Create user messages (much shorter than full prompt)
    def safe_get_user_prompt(text):
        try:
            return prompt.get_user_prompt(text)
        except ValueError as e:
            print(f"⚠️  Skipping invalid text: {e}")
            return None
    
    data['user_message'] = data['text'].apply(safe_get_user_prompt)
    
    # Remove any rows where user_message creation failed
    initial_with_messages = len(data)
    data = data[data['user_message'].notna()]
    final_with_messages = len(data)
    
    if final_with_messages < initial_with_messages:
        failed_prompts = initial_with_messages - final_with_messages
        print(f"⚠️  Removed {failed_prompts} additional items due to prompt creation failures")
    
    return data

def main():

    
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        description='Classify political text using OpenAI batch processing',
        epilog='''
Examples:
  python classify.py                                          # Process last 7 days (default)
  python classify.py --begin-date 2024-12-31 --end-date 2024-01-01  # Process entire year 2024
  python classify.py --end-date 2024-06-01 --batch-size 10000       # Process from June 1, 2024 to today
  python classify.py --begin-date 2024-01-15 --end-date 2024-01-01  # Process first 15 days of 2024
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--begin-date', type=str, 
                       default=datetime.date.today().strftime('%Y-%m-%d'),
                       help='Begin date for classification (YYYY-MM-DD format, default: today)')
    parser.add_argument('--end-date', type=str,
                       default=(datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d'),
                       help='End date for classification (YYYY-MM-DD format, default: 7 days ago)')
    parser.add_argument('--batch-size', type=int, default=20000,
                       help='Number of items per batch (default: 20000, max: 50000)')
    
    args = parser.parse_args()
    
    # Validate and parse dates
    try:
        begin_date = datetime.datetime.strptime(args.begin_date, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d').date()
    except ValueError as e:
        print(f"❌ Invalid date format: {e}")
        print("Please use YYYY-MM-DD format (e.g., 2024-01-15)")
        return
    
    # Validate date range
    if begin_date < end_date:
        print(f"❌ Begin date ({begin_date}) must be after end date ({end_date})")
        return
    
    # Validate batch size
    if args.batch_size <= 0:
        print(f"❌ Batch size must be positive, got: {args.batch_size}")
        return
    if args.batch_size > 50000:
        print(f"❌ Batch size exceeds OpenAI limit of 50,000, got: {args.batch_size}")
        return
    
    conn, params = get_db_connection()
    
    unclassified_items = (
        conn.table('classifications')
        .select([
            'id',
            'text',
            'date',
            'classified',
            'attack_personal',
            'attack_type',
            'attack_target',
            'attack_policy',
            'outcome_bipartisanship',
            'outcome_creditclaiming',
            'policy',
            'policy_area',
            'extreme_label',
            'extreme_target',
        ])
        .filter([
            _.date >= end_date.strftime('%Y-%m-%d'),
            _.date <= begin_date.strftime('%Y-%m-%d'),
            (_.classified != 1) | _.classified.isnull()
        ])
    )

    count = unclassified_items.count().execute()
    batch_size = args.batch_size  # Items per database batch (within 50,000 API limit)
    
    print(f'''
RUNNING OPTIMIZED BATCH CLASSIFICATION
📅 Date range: {end_date} to {begin_date} ({(begin_date - end_date).days + 1} days)
📊 Unclassified items found: {count:,}
🔢 Batch size: {batch_size:,}
🚀 Using SYSTEM PROMPTS for better efficiency!
''')

    # Check if there are items to process
    if count == 0:
        print("ℹ️  No unclassified items found in the specified date range.")
        return

    # Process in batches
    offset = 0
    total_processed = 0
    
    while offset < count:
        print(f'Processing batch starting at offset {offset}')
        
        # Get batch data
        batch_data = (
            unclassified_items
            .limit(batch_size, offset=offset)
            .execute()
        )
        
        if batch_data.shape[0] == 0:
            break
        
        # Prepare for optimized batch processing
        batch_data = prepare_batch_data_optimized(batch_data)
        
        # Submit batch job with system prompt
        print(f'Submitting optimized batch of {batch_data.shape[0]} items...')
        batch_ids = llms.send_batch_with_system(
            batch_data, 
            "classification", 
            prompt.system_prompt,
            "o4-mini"
        )
        
        print(f'Batch submitted with IDs: {batch_ids}')
        
        # Save batch IDs for monitoring
        save_batch_ids(batch_ids)
        
        offset += batch_size
        total_processed += batch_data.shape[0]
        
        print(f'Submitted {total_processed} items for batch processing')

    print(f'==== OPTIMIZED BATCH SUBMISSION COMPLETE ====')
    print(f'📅 Date range processed: {end_date} to {begin_date}')
    print(f'📊 Total items submitted: {total_processed:,}')
    print('\nTo monitor batch progress, use:')
    print('  python batch_monitor.py --action list       # List all batches')
    print('  python batch_monitor.py --action monitor    # Auto-monitor and process results')
    print('\nBatch IDs have been saved to logs/batch_ids.json for tracking.')

if __name__ == "__main__":
    main() 