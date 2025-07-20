'''
Monitor and retrieve OpenAI batch processing results
'''
import os, json, time, datetime
import pandas as pd
import openai
import dotenv
import dataset
import urllib.parse
import hjson
from ibis import _

dotenv.load_dotenv('../../../env')
if 'PATH_TO_SECRETS' in os.environ:
    dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
else:
    print("Warning: PATH_TO_SECRETS environment variable not found")

# DB connection - only connect when needed
def get_db_params():
    """Get database connection parameters"""
    try:
        return f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
    except KeyError as e:
        raise KeyError(f"Missing required environment variable: {e}. Please set up your database credentials.")

def check_batch_status(batch_id):
    """Check the status of a specific batch"""
    with openai.OpenAI() as client:
        batch = client.batches.retrieve(batch_id)
        return {
            'id': batch.id,
            'status': batch.status,
            'created_at': datetime.datetime.fromtimestamp(batch.created_at),
            'completed_at': datetime.datetime.fromtimestamp(batch.completed_at) if batch.completed_at else None,
            'request_counts': batch.request_counts.__dict__ if batch.request_counts else None,
            'output_file_id': batch.output_file_id,
            'error_file_id': batch.error_file_id,
            'metadata': batch.metadata
        }

def list_all_batches():
    """List all batches to see their status"""
    with openai.OpenAI() as client:
        batches = client.batches.list(limit=100)
        batch_info = []
        for batch in batches.data:
            info = check_batch_status(batch.id)
            batch_info.append(info)
        return batch_info

def download_batch_results(batch_id):
    """Download and process completed batch results"""
    with openai.OpenAI() as client:
        batch = client.batches.retrieve(batch_id)
        
        if batch.status != 'completed':
            print(f"Batch {batch_id} is not completed yet. Status: {batch.status}")
            return None
        
        if not batch.output_file_id:
            print(f"No output file for batch {batch_id}")
            return None
        
        # Download the output file
        file_response = client.files.content(batch.output_file_id)
        
        # Parse the JSONL results
        results = []
        for line in file_response.text.strip().split('\n'):
            if line.strip():
                results.append(json.loads(line))
        
        return results

def process_batch_results_to_db(batch_results):
    """Process batch results and update database"""
    processed_data = []
    
    for result in batch_results:
        try:
            # Extract custom_id to get original ID
            custom_id = result['custom_id']
            original_id = custom_id.split('-')[-1]  # Assuming format "classification-{id}"
            
            # Parse the response
            response_content = result['response']['body']['choices'][0]['message']['content']
            
            # Clean up JSON formatting
            if response_content.lstrip().startswith("```json"): 
                response_content = response_content.lstrip()[7:]
            if response_content.rstrip().endswith("```"): 
                response_content = response_content.rstrip()[:-3]
            
            try:
                response = hjson.loads(response_content)
            except:
                print(f'Bad JSON detected for ID {original_id}')
                continue
            
            # Extract classification results
            def yesno(x):
                if x:
                    x = x.lower()
                    if x == 'yes':
                        return 1
                    elif x == 'no':
                        return 0
                return None
            
            row_data = {
                'id': int(original_id),
                'attack_personal': yesno(response['attacks']['personal_attack']),
                'attack_type': str(response['attacks']['attack_type']),
                'attack_target': str(response['attacks']['personal_attack_target']),
                'attack_policy': yesno(response['policy_criticism']['policy_attack']),
                'outcome_bipartisanship': yesno(response['bipartisanship']['is_bipartisanship']),
                'outcome_creditclaiming': yesno(response['credit_claiming']['is_creditclaiming']),
                'policy_area': str(response['policy']['policy_area']),
                'extreme_label': str(response['extremism']['extreme_label']),
                'extreme_target': str(response['extremism']['extreme_target']),
                'classified': 1
            }
            
            # Set policy flag
            try:
                if len(hjson.loads(row_data['policy_area'])) > 0:
                    row_data['policy'] = 1
                else:
                    row_data['policy'] = 0
            except:
                row_data['policy'] = 0
                
            processed_data.append(row_data)
            
        except Exception as e:
            print(f'Error processing result: {e}')
            continue
    
    # Update database
    if processed_data:
        params = get_db_params()
        dbx = dataset.connect(params)
        dbx['classifications'].upsert_many(processed_data, 'id')
        dbx.engine.dispose()
        dbx.close()
        print(f"Updated {len(processed_data)} records in database")
    
    return processed_data

def save_batch_ids(batch_ids, filename='batch_ids.json'):
    """Save batch IDs to a file for tracking"""
    data = {
        'batch_ids': batch_ids,
        'created_at': datetime.datetime.now().isoformat(),
        'status': 'submitted'
    }
    
    # Load existing data if file exists
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            existing_data = json.load(f)
        if 'batches' not in existing_data:
            existing_data['batches'] = []
        existing_data['batches'].append(data)
    else:
        existing_data = {'batches': [data]}
    
    with open(filename, 'w') as f:
        json.dump(existing_data, f, indent=2)
    
    print(f"Saved batch IDs to {filename}")

def monitor_batches(filename='batch_ids.json', wait_minutes=30):
    """Monitor all tracked batches and process completed ones"""
    if not os.path.exists(filename):
        print(f"No batch tracking file found: {filename}")
        return
    
    with open(filename, 'r') as f:
        data = json.load(f)
    
    print(f"Monitoring {len(data.get('batches', []))} batch groups...")
    
    completed_batches = []
    
    for batch_group in data.get('batches', []):
        if batch_group.get('status') == 'completed':
            continue
            
        print(f"\nChecking batch group from {batch_group['created_at']}:")
        all_completed = True
        
        for batch_id in batch_group['batch_ids']:
            status_info = check_batch_status(batch_id)
            print(f"  Batch {batch_id}: {status_info['status']}")
            
            if status_info['status'] == 'completed':
                print(f"    ✅ Downloading and processing results...")
                results = download_batch_results(batch_id)
                if results:
                    processed = process_batch_results_to_db(results)
                    print(f"    📝 Processed {len(processed)} items")
            elif status_info['status'] in ['failed', 'expired', 'cancelled']:
                print(f"    ❌ Batch failed with status: {status_info['status']}")
            else:
                all_completed = False
        
        if all_completed:
            batch_group['status'] = 'completed'
            completed_batches.append(batch_group)
    
    # Save updated status
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    
    if completed_batches:
        print(f"\n🎉 Completed {len(completed_batches)} batch groups!")
    else:
        print(f"\n⏳ No batches completed yet. Will check again in {wait_minutes} minutes.")

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Monitor OpenAI batch processing')
    parser.add_argument('--action', choices=['list', 'status', 'download', 'monitor'], 
                       default='list', help='Action to perform')
    parser.add_argument('--batch-id', help='Specific batch ID to check/download')
    parser.add_argument('--monitor-file', default='batch_ids.json', 
                       help='File to track batch IDs')
    parser.add_argument('--wait', type=int, default=30, 
                       help='Minutes to wait between checks when monitoring')
    
    args = parser.parse_args()
    
    if args.action == 'list':
        print("All batches:")
        batches = list_all_batches()
        for batch in batches:
            print(f"ID: {batch['id']}")
            print(f"  Status: {batch['status']}")
            print(f"  Created: {batch['created_at']}")
            print(f"  Completed: {batch['completed_at']}")
            if batch['request_counts']:
                print(f"  Requests: {batch['request_counts']}")
            print()
    
    elif args.action == 'status':
        if not args.batch_id:
            print("Please provide --batch-id")
            return
        status = check_batch_status(args.batch_id)
        print(json.dumps(status, indent=2, default=str))
    
    elif args.action == 'download':
        if not args.batch_id:
            print("Please provide --batch-id")
            return
        results = download_batch_results(args.batch_id)
        if results:
            processed = process_batch_results_to_db(results)
            print(f"Processed {len(processed)} results")
    
    elif args.action == 'monitor':
        print(f"Starting batch monitoring (checking every {args.wait} minutes)...")
        print("Press Ctrl+C to stop")
        try:
            while True:
                monitor_batches(args.monitor_file, args.wait)
                time.sleep(args.wait * 60)
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")

if __name__ == "__main__":
    main() 