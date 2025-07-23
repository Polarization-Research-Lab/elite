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

# Ensure logs directory exists
LOGS_DIR = 'logs'
os.makedirs(LOGS_DIR, exist_ok=True)

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

def safe_dict_conversion(obj):
    """Safely convert various object types to dictionary"""
    if obj is None:
        return None
    try:
        # If it's already a dict
        if isinstance(obj, dict):
            return obj
        # If it has __dict__ attribute
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        # If it's dict-like (has items method)
        elif hasattr(obj, 'items'):
            return dict(obj)
        # Try to convert directly
        else:
            return dict(obj)
    except (TypeError, ValueError, AttributeError):
        # Fallback to string representation
        return str(obj)

def check_batch_status(batch_id):
    """Check the status of a specific batch with error handling"""
    try:
        with openai.OpenAI() as client:
            batch = client.batches.retrieve(batch_id)
            
            # Build comprehensive status info
            status_info = {
                'id': batch.id,
                'status': batch.status,
                'created_at': datetime.datetime.fromtimestamp(batch.created_at),
                'completed_at': datetime.datetime.fromtimestamp(batch.completed_at) if batch.completed_at else None,
                'request_counts': safe_dict_conversion(batch.request_counts),
                'output_file_id': batch.output_file_id,
                'error_file_id': batch.error_file_id,
                'metadata': safe_dict_conversion(batch.metadata)
            }
            
            # Add derived information
            if status_info['created_at'] and status_info['completed_at']:
                duration = status_info['completed_at'] - status_info['created_at']
                status_info['duration_minutes'] = duration.total_seconds() / 60
            
            # Add progress information if available
            if status_info['request_counts']:
                counts = status_info['request_counts']
                total = counts.get('total', 0)
                completed = counts.get('completed', 0)
                failed = counts.get('failed', 0)
                
                if total > 0:
                    status_info['completion_percentage'] = (completed / total) * 100
                    status_info['failure_percentage'] = (failed / total) * 100
            
            return status_info
            
    except Exception as e:
        error_msg = f"Failed to check status for batch {batch_id}: {str(e)}"
        print(f"❌ {error_msg}")
        
        # Return error status
        return {
            'id': batch_id,
            'status': 'error',
            'error': error_msg,
            'error_type': type(e).__name__,
            'checked_at': datetime.datetime.now().isoformat()
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
    """Download and process completed batch results with detailed error handling"""
    try:
        with openai.OpenAI() as client:
            print(f"📥 Downloading results for batch {batch_id}...")
            
            # Get batch status
            try:
                batch = client.batches.retrieve(batch_id)
            except Exception as api_error:
                error_msg = f"Failed to retrieve batch {batch_id}: {api_error}"
                print(f"❌ {error_msg}")
                return None, None
            
            # Check batch status
            if batch.status != 'completed':
                print(f"⏳ Batch {batch_id} is not completed yet. Status: {batch.status}")
                if batch.status in ['failed', 'expired', 'cancelled']:
                    print(f"❌ Batch failed with status: {batch.status}")
                    # Try to get error details
                    if batch.error_file_id:
                        try:
                            error_content = client.files.content(batch.error_file_id)
                            error_file = os.path.join(LOGS_DIR, f"batch_api_errors_{batch_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl")
                            with open(error_file, 'w') as f:
                                f.write(error_content.read().decode('utf-8'))
                            print(f"🔍 Saved API errors to {error_file}")
                        except Exception as e:
                            print(f"⚠️  Could not download error file: {e}")
                return None, None
            
            # Check for output file
            if not batch.output_file_id:
                print(f"❌ No output file for batch {batch_id}")
                print(f"   Request counts: {safe_dict_conversion(batch.request_counts)}")
                return None, None
            
            # Download the output file
            try:
                file_response = client.files.content(batch.output_file_id)
                file_content = file_response.read().decode('utf-8')
            except Exception as download_error:
                error_msg = f"Failed to download output file for batch {batch_id}: {download_error}"
                print(f"❌ {error_msg}")
                return None, None
            
            # Save raw output file for backup
            backup_file = os.path.join(LOGS_DIR, f"batch_output_{batch_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl")
            with open(backup_file, 'w') as f:
                f.write(file_content)
            # Only print for debugging or when there might be issues
            if len(file_content.strip().split('\n')) <= 100:
                print(f"💾 Saved raw output to {backup_file}")
            
            # Parse the JSONL results
            results = []
            parse_errors = []
            
            for line_num, line in enumerate(file_content.strip().split('\n'), 1):
                if line.strip():
                    try:
                        result = json.loads(line)
                        results.append(result)
                    except json.JSONDecodeError as parse_error:
                        error_msg = f"JSON parse error on line {line_num}: {parse_error}"
                        print(f"⚠️  {error_msg}")
                        parse_errors.append({
                            'line_number': line_num,
                            'error': str(parse_error),
                            'raw_line': line
                        })
            
            # Save parse errors if any
            if parse_errors:
                parse_error_file = os.path.join(LOGS_DIR, f"parse_errors_{batch_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                with open(parse_error_file, 'w') as f:
                    json.dump(parse_errors, f, indent=2)
                print(f"🔍 Saved {len(parse_errors)} parse errors to {parse_error_file}")
            
            if len(results) > 0:
                print(f"📥 Downloaded {len(results)} results from batch {batch_id}")
            return results, backup_file
            
    except Exception as e:
        error_msg = f"Unexpected error downloading batch {batch_id}: {str(e)}"
        print(f"❌ {error_msg}")
        return None, None

def process_batch_results_to_db(batch_results, backup_file=None):
    """Process batch results and update database with detailed error handling"""
    processed_data = []
    failed_responses = []
    error_log = []
    
    if len(batch_results) > 100:
        print(f"Processing {len(batch_results)} batch results...")
    
    # Store created file names for cleanup
    created_files = []
    
    for i, result in enumerate(batch_results):
        try:
            # Extract custom_id to get original ID
            custom_id = result.get('custom_id', f'unknown_{i}')
            original_id = custom_id.split('-')[-1] if '-' in custom_id else custom_id
            
            # Check if request failed at API level
            if 'error' in result and result['error'] is not None:
                error_msg = f"API Error for ID {original_id}: {result['error']}"
                print(f"❌ {error_msg}")
                error_log.append({'id': original_id, 'error': error_msg, 'full_result': result})
                continue
            
            # Check response structure
            if 'response' not in result or 'body' not in result['response']:
                error_msg = f"Invalid response structure for ID {original_id}: Missing response/body"
                print(f"❌ {error_msg}")
                error_log.append({'id': original_id, 'error': error_msg, 'full_result': result})
                continue
            
            # Check response status code
            response_status = result['response'].get('status_code', 0)
            if response_status != 200:
                error_msg = f"API Response Error for ID {original_id}: HTTP {response_status}"
                print(f"❌ {error_msg}")
                error_log.append({'id': original_id, 'error': error_msg, 'full_result': result})
                continue
            
            # Check for API errors in response
            response_body = result['response']['body']
            if 'error' in response_body and response_body['error'] is not None:
                error_msg = f"API Response Error for ID {original_id}: {response_body['error']}"
                print(f"❌ {error_msg}")
                error_log.append({'id': original_id, 'error': error_msg, 'full_result': result})
                continue
            
            # Extract response content
            if 'choices' not in response_body or len(response_body['choices']) == 0:
                error_msg = f"No choices in response for ID {original_id}"
                print(f"❌ {error_msg}")
                error_log.append({'id': original_id, 'error': error_msg, 'full_result': result})
                continue
            
            response_content = response_body['choices'][0]['message']['content']
            
            # Save raw response for debugging
            raw_response_data = {
                'id': original_id,
                'custom_id': custom_id,
                'raw_content': response_content,
                'full_response': result
            }
            
            # Clean up JSON formatting
            cleaned_content = response_content
            if cleaned_content.lstrip().startswith("```json"): 
                cleaned_content = cleaned_content.lstrip()[7:]
            if cleaned_content.rstrip().endswith("```"): 
                cleaned_content = cleaned_content.rstrip()[:-3]
            
            # Check for specific empty text error before trying to parse JSON
            if "statement to analyze wasn't included" in response_content or "statement to analyze wasn\u2019t included" in response_content:
                error_msg = f"Empty text error for ID {original_id}: Model indicates no text was provided"
                print(f"❌ {error_msg}")
                error_log.append({
                    'id': original_id, 
                    'error': error_msg,
                    'error_type': 'empty_text',
                    'raw_content': response_content,
                    'full_result': result
                })
                failed_responses.append(raw_response_data)
                continue
            
            # Parse JSON response
            try:
                response = hjson.loads(cleaned_content)
            except Exception as json_error:
                error_msg = f"JSON Parse Error for ID {original_id}: {json_error}"
                print(f"❌ {error_msg}")
                print(f"   Raw content: {response_content[:200]}...")
                error_log.append({
                    'id': original_id, 
                    'error': error_msg, 
                    'raw_content': response_content,
                    'full_result': result
                })
                failed_responses.append(raw_response_data)
                continue
            
            # Extract classification results with detailed error handling
            def yesno(x):
                if x:
                    x = str(x).lower()
                    if x == 'yes':
                        return 1
                    elif x == 'no':
                        return 0
                return None
            
            try:
                row_data = {
                    'id': int(original_id),
                    'attack_personal': yesno(response.get('attacks', {}).get('personal_attack')),
                    'attack_type': str(response.get('attacks', {}).get('attack_type', '')),
                    'attack_target': str(response.get('attacks', {}).get('personal_attack_target', '')),
                    'attack_policy': yesno(response.get('policy_criticism', {}).get('policy_attack')),
                    'outcome_bipartisanship': yesno(response.get('bipartisanship', {}).get('is_bipartisanship')),
                    'outcome_creditclaiming': yesno(response.get('credit_claiming', {}).get('is_creditclaiming')),
                    'policy_area': str(response.get('policy', {}).get('policy_area', '[]')),
                    'extreme_label': str(response.get('extremism', {}).get('extreme_label', '')),
                    'extreme_target': str(response.get('extremism', {}).get('extreme_target', '')),
                    'classified': 1
                }
                
                # Set policy flag with error handling
                try:
                    policy_area_list = hjson.loads(row_data['policy_area'])
                    row_data['policy'] = 1 if len(policy_area_list) > 0 else 0
                except Exception as policy_error:
                    print(f"⚠️  Policy area parse warning for ID {original_id}: {policy_error}")
                    row_data['policy'] = 0
                
                # Check for null/missing critical fields and save to error file if found
                critical_fields = ['attack_personal', 'attack_policy', 'outcome_bipartisanship', 'outcome_creditclaiming']
                null_fields = [field for field in critical_fields if row_data[field] is None]
                
                if null_fields:
                    error_msg = f"Null fields detected for ID {original_id}: {null_fields}"
                    print(f"⚠️  {error_msg}")
                    
                    # Save the problematic response
                    null_field_error = {
                        'id': original_id,
                        'error': error_msg,
                        'null_fields': null_fields,
                        'row_data': row_data,
                        'raw_response': response,
                        'full_result': result
                    }
                    error_log.append(null_field_error)
                    failed_responses.append({
                        'id': original_id,
                        'custom_id': custom_id,
                        'raw_content': response_content,
                        'full_response': result,
                        'reason': 'null_fields'
                    })
                else:
                    processed_data.append(row_data)
                
            except KeyError as key_error:
                error_msg = f"Missing required field for ID {original_id}: {key_error}"
                print(f"❌ {error_msg}")
                print(f"   Available fields: {list(response.keys())}")
                error_log.append({
                    'id': original_id, 
                    'error': error_msg, 
                    'response_structure': response,
                    'full_result': result
                })
                failed_responses.append(raw_response_data)
                continue
                
        except Exception as e:
            error_msg = f"Unexpected error processing result {i}: {str(e)}"
            print(f"❌ {error_msg}")
            error_log.append({
                'id': original_id if 'original_id' in locals() else f'unknown_{i}', 
                'error': error_msg, 
                'full_result': result
            })
            continue
    
    # Save error log if there were errors
    if error_log:
        error_file = os.path.join(LOGS_DIR, f"batch_errors_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(error_file, 'w') as f:
            json.dump(error_log, f, indent=2, default=str)
        print(f"🔍 Saved {len(error_log)} errors to {error_file}")
        created_files.append(error_file)
    
    # Save failed responses for debugging
    if failed_responses:
        failed_file = os.path.join(LOGS_DIR, f"failed_responses_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(failed_file, 'w') as f:
            json.dump(failed_responses, f, indent=2, default=str)
        print(f"🔍 Saved {len(failed_responses)} failed responses to {failed_file}")
        created_files.append(failed_file)
    
    # Update database
    if processed_data:
        try:
            params = get_db_params()
            dbx = dataset.connect(params)
            dbx['classifications'].upsert_many(processed_data, 'id')
            dbx.engine.dispose()
            dbx.close()
            
            # Only print for large batches or when there were errors
            if len(processed_data) > 100 or error_log:
                print(f"✅ Updated {len(processed_data)} records in database")
            
            # Clean up files if completely successful (no errors)
            if not error_log and not failed_responses:
                files_to_cleanup = created_files[:]  # Copy the list
                if backup_file:
                    files_to_cleanup.append(backup_file)
                
                for file_path in files_to_cleanup:
                    try:
                        if os.path.exists(file_path):
                            os.remove(file_path)
                    except Exception as cleanup_error:
                        print(f"⚠️  Could not clean up {file_path}: {cleanup_error}")
                
                if files_to_cleanup and len(processed_data) > 100:
                    print(f"🧹 Cleaned up {len(files_to_cleanup)} temporary files")
                    
        except Exception as db_error:
            error_msg = f"Database update failed: {db_error}"
            print(f"❌ {error_msg}")
            # Save processed data to file as backup
            backup_file_path = os.path.join(LOGS_DIR, f"db_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(backup_file_path, 'w') as f:
                json.dump(processed_data, f, indent=2, default=str)
            print(f"💾 Saved processed data to backup file: {backup_file_path}")
            created_files.append(backup_file_path)
            raise
    
    # Summary - only show if there are errors or large batches
    total_results = len(batch_results)
    successful = len(processed_data)
    failed = len(error_log)
    
    if failed > 0 or total_results > 100:
        print(f"\n📊 PROCESSING SUMMARY:")
        print(f"   Total results: {total_results}")
        print(f"   ✅ Successful: {successful}")
        if failed > 0:
            print(f"   ❌ Failed: {failed}")
            print(f"   🔍 Error logs saved for debugging")
    
    return processed_data

def save_batch_ids(batch_ids, filename='batch_ids.json'):
    """Save batch IDs to a file for tracking"""
    # Ensure filename uses logs directory
    if not filename.startswith(LOGS_DIR):
        filename = os.path.join(LOGS_DIR, filename)
    
    data = {
        'batch_ids': batch_ids,
        'created_at': datetime.datetime.now().isoformat()
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
    # Ensure filename uses logs directory
    if not filename.startswith(LOGS_DIR):
        filename = os.path.join(LOGS_DIR, filename)
        
    if not os.path.exists(filename):
        print(f"No batch tracking file found: {filename}")
        return False  # No batches to monitor
    
    with open(filename, 'r') as f:
        data = json.load(f)
    
    incomplete_batches = []
    completed_count = 0
    
    print(f"Monitoring {len(data.get('batches', []))} batch groups...")
    
    for batch_group in data.get('batches', []):
        print(f"\nChecking batch group from {batch_group['created_at']}:")
        all_completed = True
        
        for batch_id in batch_group['batch_ids']:
            try:
                status_info = check_batch_status(batch_id)
                print(f"  Batch {batch_id}: {status_info['status']}")
                
                if status_info['status'] == 'completed':
                    print(f"    ✅ Downloading and processing results...")
                    results, backup_file = download_batch_results(batch_id)
                    if results:
                        processed = process_batch_results_to_db(results, backup_file)
                        if len(processed) > 10:
                            print(f"    📝 Processed {len(processed)} items")
                        
                        # Log successful completion
                        completion_log = {
                            'batch_id': batch_id,
                            'completed_at': datetime.datetime.now().isoformat(),
                            'total_results': len(results),
                            'processed_items': len(processed),
                            'request_counts': status_info.get('request_counts', {})
                        }
                        
                        log_file = os.path.join(LOGS_DIR, f"completion_log_{datetime.datetime.now().strftime('%Y%m%d')}.json")
                        if os.path.exists(log_file):
                            with open(log_file, 'r') as f:
                                log_data = json.load(f)
                        else:
                            log_data = {'completions': []}
                        
                        log_data['completions'].append(completion_log)
                        with open(log_file, 'w') as f:
                            json.dump(log_data, f, indent=2)
                    else:
                        print(f"    ⚠️  No results downloaded for batch {batch_id}")
                        all_completed = False
                        
                elif status_info['status'] in ['failed', 'expired', 'cancelled']:
                    print(f"    ❌ Batch failed with status: {status_info['status']}")
                    
                    # Log failure details
                    failure_log = {
                        'batch_id': batch_id,
                        'failed_at': datetime.datetime.now().isoformat(),
                        'status': status_info['status'],
                        'request_counts': status_info.get('request_counts', {}),
                        'error_file_id': status_info.get('error_file_id'),
                        'metadata': status_info.get('metadata', {})
                    }
                    
                    failure_file = os.path.join(LOGS_DIR, f"batch_failures_{datetime.datetime.now().strftime('%Y%m%d')}.json")
                    if os.path.exists(failure_file):
                        with open(failure_file, 'r') as f:
                            failure_data = json.load(f)
                    else:
                        failure_data = {'failures': []}
                    
                    failure_data['failures'].append(failure_log)
                    with open(failure_file, 'w') as f:
                        json.dump(failure_data, f, indent=2)
                    
                    print(f"    🔍 Logged failure details to {failure_file}")
                    
                else:
                    print(f"    ⏳ Status: {status_info['status']}")
                    if status_info.get('request_counts'):
                        counts = status_info['request_counts']
                        print(f"       Progress: {counts.get('completed', 0)}/{counts.get('total', 0)} completed")
                    all_completed = False
                    
            except Exception as e:
                error_msg = f"Error checking batch {batch_id}: {str(e)}"
                print(f"    ❌ {error_msg}")
                
                # Log monitoring errors
                monitor_error = {
                    'batch_id': batch_id,
                    'error_at': datetime.datetime.now().isoformat(),
                    'error': error_msg,
                    'error_type': type(e).__name__
                }
                
                error_file = os.path.join(LOGS_DIR, f"monitor_errors_{datetime.datetime.now().strftime('%Y%m%d')}.json")
                if os.path.exists(error_file):
                    with open(error_file, 'r') as f:
                        error_data = json.load(f)
                else:
                    error_data = {'errors': []}
                
                error_data['errors'].append(monitor_error)
                with open(error_file, 'w') as f:
                    json.dump(error_data, f, indent=2)
                
                # Assume not completed if we can't check
                all_completed = False
        
        if all_completed:
            completed_count += 1
            print(f"    🗑️  Removing completed batch group from tracking")
        else:
            # Keep tracking this batch group
            incomplete_batches.append(batch_group)
    
    # Update file with only incomplete batches
    if incomplete_batches:
        data['batches'] = incomplete_batches
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"\n📊 Status: {len(incomplete_batches)} batch groups still in progress")
        if completed_count > 0:
            print(f"🎉 Removed {completed_count} completed batch groups from tracking")
        return True  # Still have batches to monitor
    else:
        # No incomplete batches left - remove the file
        os.remove(filename)
        print(f"\n🎉 ALL BATCHES COMPLETED! Removed tracking file.")
        if completed_count > 0:
            print(f"✅ Processed and removed {completed_count} completed batch groups")
        return False  # No more batches to monitor

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Monitor OpenAI batch processing')
    parser.add_argument('--action', choices=['list', 'status', 'download', 'monitor'], 
                       default='list', help='Action to perform')
    parser.add_argument('--batch-id', help='Specific batch ID to check/download')
    parser.add_argument('--monitor-file', default=os.path.join(LOGS_DIR, 'batch_ids.json'), 
                       help='File to track batch IDs')
    parser.add_argument('--wait', type=int, default=15, 
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
        results, backup_file = download_batch_results(args.batch_id)
        if results:
            processed = process_batch_results_to_db(results, backup_file)
            print(f"Processed {len(processed)} results")
    
    elif args.action == 'monitor':
        print(f"Starting batch monitoring (checking every {args.wait} minutes)...")
        print("Press Ctrl+C to stop")
        try:
            while True:
                still_monitoring = monitor_batches(args.monitor_file, args.wait)
                if not still_monitoring:
                    print("\n🎉 All batches completed! Monitoring finished.")
                    break
                print(f"\n⏳ Waiting {args.wait} minutes before next check...")
                time.sleep(args.wait * 60)
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")

if __name__ == "__main__":
    main() 