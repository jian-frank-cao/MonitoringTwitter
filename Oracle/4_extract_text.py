import os
os.chdir('/home/ubuntu/twitter_monitor/coronavirus')
import oci
import json
import time

# Parameters
download_path = "/home/ubuntu/twitter_monitor/coronavirus/downloaded/"
bucket_name = "COVID-19-twitter-text-A"
wait_time = 600
retry_time = 1

# Load the default configuration
config_oci = oci.config.from_file("~/.oci/config")

# Function to extract text from json
def preprocess_text(obj):
    out = dict()
    out["id_str"] = obj["id_str"]
    out["created_at"] = obj["created_at"]
    if 'retweeted_status' in obj:
            if 'extended_tweet' in obj['retweeted_status']:
                out["retweet_full_text"] = obj['retweeted_status']['extended_tweet']['full_text']
            else:
                if 'full_text' in obj['retweeted_status']:
                    out["retweet_full_text"] = obj['retweeted_status']['full_text']
                else:
                    out["retweet_text"] = obj['retweeted_status']['text']
    else:
        if 'extended_tweet' in obj:
            out["full_text"] = obj['extended_tweet']['full_text']
        else:
            if 'full_text' in obj:
                out["full_text"] = obj['full_text']
            else:
                out["text"] = obj['text']
    if 'quoted_status' in obj:
            if 'extended_tweet' in obj['quoted_status']:
                out["quoted_full_text"] = obj['quoted_status']['extended_tweet']['full_text']
            else:
                if 'full_text' in obj['quoted_status']:
                    out["quoted_full_text"] = obj['quoted_status']['full_text']
                else:
                    out["quoted_text"] = obj['quoted_status']['text']
    return out

# Setup retry strategy
retry_strategy_covid19 = oci.retry.RetryStrategyBuilder(
    # Make up to 10 service calls
    max_attempts_check=True,
    max_attempts=10,
    # Don't exceed a total of 600 seconds for all service calls
    total_elapsed_time_check=True,
    total_elapsed_time_seconds=600,
    # Wait 45 seconds between attempts
    retry_max_wait_between_calls_seconds=45,
    # Use 2 seconds as the base number for doing sleep time calculations
    retry_base_sleep_time_seconds=2,
    # Retry on certain service errors:
    #
    #   - 5xx code received for the request
    #   - Any 429 (this is signified by the empty array in the retry config)
    #   - 400s where the code is QuotaExceeded or LimitExceeded
    service_error_check=True,
    service_error_retry_on_any_5xx=True,
    service_error_retry_config={
        400: ['QuotaExceeded', 'LimitExceeded'],
        429: []
    },
    # Use exponential backoff and retry with full jitter, but on throttles use
    # exponential backoff and retry with equal jitter
    backoff_type=oci.retry.BACKOFF_FULL_JITTER_EQUAL_ON_THROTTLE_VALUE
).get_retry_strategy()


# Connect to Oracle Object Storage bucket
object_storage_client = oci.object_storage.ObjectStorageClient(config_oci)
namespace = object_storage_client.get_namespace().data


while True:
    retry = False
    # Dectect files
    files_list = sorted([x for x in os.listdir(download_path) if ".7z" not in x])
    for filename in files_list:
        with open(download_path + filename, "r") as file:
            file_content = file.read()
        data = json.loads('['+file_content.replace('}{','},{')+']')
        text = "".join([json.dumps(preprocess_text(x)) for x in data])
        object_name = "text" + filename[7:]
        try:
            put_object_response = object_storage_client.put_object(namespace, bucket_name,
                                                                   object_name, text,
                                                                   retry_strategy = retry_strategy_covid19)
            os.remove(download_path + filename)
            print(filename + " is processed")
        except IOError as ex:
            print('I just caught an IOError-----' + str(ex))
            retry = True
            continue # temporary interruption, re-try request
    if retry:
        print('Retrying...')
        time.sleep(retry_time)
    else:
        print('Waiting for next update...')
        time.sleep(wait_time)
    
    
    
    
    
    
    
    
    
    
    

