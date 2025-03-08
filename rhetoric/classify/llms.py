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
def chatgpt(message, model = 'gpt-4o'):
    messages = [{
        'role': 'user',
        'content': message,
    }]

    with openai.OpenAI() as client:
        response = client.chat.completions.create(
            # model = "gpt-3.5-turbo-1106",
            # model = "gpt-4-1106-preview",
            # model = "gpt-4-turbo-2024-04-09",
            # model = "gpt-4o",
            model = model,
            messages = messages,
            temperature = 0.8,
            # max_tokens = 1,
        )
        response = response.choices[0].message.content
    return response


def send_batch(data, prompt, model):

    records = data.apply(
        lambda entry: {
            "custom_id": f"{prompt}-{entry['id']}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body":
            {
                "model": model,
                "messages": [
                {
                    "role": "user",
                    "content": entry['message']
                }],
                "max_tokens": 1000
            }
        }, 
        axis = 1
    )

    max_file_size = 50000000
    
    batches = []
        
    # with tempfile.NamedTemporaryFile(dir = 'check', delete = False) as file: # <-- for testing
    with tempfile.NamedTemporaryFile() as file:
        records.to_json(file.name, orient = 'records', lines = True)
        if os.path.getsize(file.name) > max_file_size:
            print('SPLITTING')
            objs = []
            os.makedirs('./tmpsplitdir/', exist_ok = True)
            subprocess.run(["split", "-C", f"{max_file_size}", file.name, './tmpsplitdir/'])
            print(f'>>> files created in split: {len([file for file in os.listdir("./tmpsplitdir/")])}')
            for subfile in os.listdir('./tmpsplitdir/'):
                batch_id = api_call(os.path.join('./tmpsplitdir/', subfile), prompt = prompt)
                batches.append(batch_id)
                os.remove(os.path.join('./tmpsplitdir/', subfile))
            os.rmdir('./tmpsplitdir/')
        else:
            print('NO SPLIT NEEDED')
            batch_id = api_call(file.name, prompt = prompt)
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