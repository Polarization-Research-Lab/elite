import sys, time, json, os, copy, tempfile, subprocess
import pandas as pd
import openai



# Exponential Backoff Decorator
def cautious_fetch(max_retries=5, wait_time=7):
    def decorator_retry(func):
        def wrapper_retry(*args, **kwargs):
            retries, current_wait_time = 0, wait_time
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"An error occurred: {e}")
                    print(f"Retrying in {current_wait_time} seconds...")
                    time.sleep(current_wait_time)
                    retries += 1
                    current_wait_time *= 3
            print("Exceeded maximum number of retries. Aborting.")
            return None
        return wrapper_retry
    return decorator_retry


# OpenAI
@cautious_fetch(max_retries=5, wait_time=7)
def chatgpt(message, model = 'o4-mini'):
    messages = [{
        'role': 'user',
        'content': message,
    }]

    with openai.OpenAI() as client:
        response = client.chat.completions.create(
            # model = "gpt-3.5-turbo-1106",
            # model = "gpt-4-1106-preview",
            # model = "gpt-4-turbo-2024-04-09",
            # model = "o4-mini",
            model = model,
            messages = messages,
            # max_tokens = 1,
        )
        response = response.choices[0].message.content
    return response

# OpenAI with system prompt (more efficient)
@cautious_fetch(max_retries=5, wait_time=7)
def chatgpt_with_system(user_message, system_message, model = 'o4-mini'):
    messages = [
        {
            'role': 'system',
            'content': system_message,
        },
        {
            'role': 'user',
            'content': user_message,
        }
    ]

    with openai.OpenAI() as client:
        response = client.chat.completions.create(
            model = model,
            messages = messages,
        )
        response = response.choices[0].message.content
    return response


def send_batch_with_system(data, prompt_name, system_prompt, model):
    """Send batch with system prompt for better efficiency"""
    
    records = data.apply(
        lambda entry: {
            "custom_id": f"{prompt_name}-{entry['id']}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body":
            {
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": system_prompt
                    },
                    {
                        "role": "user",
                        "content": entry['user_message']
                    }
                ],
                "max_completion_tokens": 5000
            }
        }, 
        axis = 1
    )

    # OpenAI Batch API limits: 50,000 requests and 200MB per batch
    max_requests_per_batch = 50000
    max_file_size = 209715200  # 200MB in bytes
    
    batches = []
    
    # Split data if it exceeds request limit
    if len(records) > max_requests_per_batch:
        print(f'SPLITTING BY REQUEST COUNT: {len(records)} requests > {max_requests_per_batch} limit')
        for i in range(0, len(records), max_requests_per_batch):
            chunk = records.iloc[i:i + max_requests_per_batch]
            
            with tempfile.NamedTemporaryFile() as file:
                chunk.to_json(file.name, orient = 'records', lines = True)
                batch_id = api_call(file.name, prompt = prompt_name)
                batches.append(batch_id)
                print(f'  Submitted batch chunk {i//max_requests_per_batch + 1} with {len(chunk)} requests')
    else:
        # Check file size and split if needed
        with tempfile.NamedTemporaryFile() as file:
            records.to_json(file.name, orient = 'records', lines = True)
            if os.path.getsize(file.name) > max_file_size:
                print(f'SPLITTING BY FILE SIZE: {os.path.getsize(file.name)} bytes > {max_file_size} limit')
                objs = []
                os.makedirs('./tmpsplitdir/', exist_ok = True)
                subprocess.run(["split", "-C", f"{max_file_size}", file.name, './tmpsplitdir/'])
                print(f'>>> files created in split: {len([file for file in os.listdir("./tmpsplitdir/")])}')
                for subfile in os.listdir('./tmpsplitdir/'):
                    batch_id = api_call(os.path.join('./tmpsplitdir/', subfile), prompt = prompt_name)
                    batches.append(batch_id)
                    os.remove(os.path.join('./tmpsplitdir/', subfile))
                os.rmdir('./tmpsplitdir/')
            else:
                print('NO SPLIT NEEDED')
                batch_id = api_call(file.name, prompt = prompt_name)
                batches.append(batch_id)

    return batches

def api_call(file_name, prompt = ''):
    # create batch
    with openai.OpenAI() as client:

        batch_input_file = client.files.create(
            file = open(file_name, "rb"),
            purpose = "batch",
        )

        batch = client.batches.create(
            input_file_id = batch_input_file.id,
            endpoint = "/v1/chat/completions",
            completion_window = "24h",
            metadata = {
              "description": "classification job: " + prompt
            },
        )

        return batch.id