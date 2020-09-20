import os
os.chdir('/home/ubuntu/twitter_monitor/coronavirus')
import config_jian
import oci
import json
import time
from base64 import b64encode
from TwitterAPI import TwitterAPI, TwitterConnectionError, TwitterRequestError

## Setup ------------------------------------------------------------------------------------------------------------------
# Keywords
keywords = ['#coronavirus','coronavirus']

# Oracle IDs
stream_name = "COVID-19-A"
partitions = 1
compartment_id = "ocid1.compartment.oc1..aaaaaaaadltovre7x2qlxrdjehvhcsgqlbnzsvrhdkp6soqnhgw35tlmv7ja"

# Load the default configuration
config_oci = oci.config.from_file("~/.oci/config")

# Create a StreamAdminClientCompositeOperations for composite operations.
stream_admin_client = oci.streaming.StreamAdminClient(config_oci)
stream_admin_client_composite = oci.streaming.StreamAdminClientCompositeOperations(stream_admin_client)

# Functions to get the stream
def get_or_create_stream(client, compartment_id, stream_name, partition, sac_composite):
    list_streams = client.list_streams(compartment_id=compartment_id, name=stream_name,
                                       lifecycle_state=oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE)
    if list_streams.data:
        # If we find an active stream with the correct name, we'll use it.
        print("An active stream {} has been found".format(stream_name))
        sid = list_streams.data[0].id
        return get_stream(sac_composite.client, sid)
    print(" No Active stream  {} has been found; Creating it now. ".format(stream_name))
    print(" Creating stream {} with {} partitions.".format(stream_name, partition))
    # Create stream_details object that need to be passed while creating stream.
    stream_details = oci.streaming.models.CreateStreamDetails(name=stream_name, partitions=partition,
                                                              compartment_id=compartment_id, retention_in_hours=24)
    # Since stream creation is asynchronous; we need to wait for the stream to become active.
    response = sac_composite.create_stream_and_wait_for_state(
        stream_details, wait_for_states=[oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE])
    return response

def get_stream(admin_client, stream_id):
    return admin_client.get_stream(stream_id)

# Connecting to Twitter API
api = TwitterAPI(config_jian.nick_consumer_key,
                 config_jian.nick_consumer_secret,
                 config_jian.nick_access_token_key,
                 config_jian.nick_access_token_secret)

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

## Put tweets in Oracle stream -----------------------------------------------------------------------------------------------
# Endless stream
message_list = []
while True:
    # Get or create a stream with stream_name
    stream = get_or_create_stream(stream_admin_client, compartment_id, stream_name,
                              partitions, stream_admin_client_composite).data
    stream_id = stream.id
    # Connect to the stream using the provided message endpoint.
    stream_client = oci.streaming.StreamClient(config_oci,
                                               service_endpoint=stream.messages_endpoint)
    try:
        r = api.request('statuses/filter', {'track': keywords})
        for item in r:
            if 'text' in item:
                message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=b64encode(str(time.time()).encode()).decode(),
                                                                                 value=b64encode(json.dumps(item).encode()).decode())) 
            elif 'disconnect' in item:
                event = item['disconnect']
                if event['code'] in [2,5,6,7]:
                    raise Exception(event['reason']) # something needs to be fixed before re-connecting
                else:
                    break # temporary interruption, re-try request
            if len(message_list) == 50:
                messages = oci.streaming.models.PutMessagesDetails(messages=message_list)
                put_message_result = stream_client.put_messages(stream_id, messages,
                                                                retry_strategy = retry_strategy_covid19)
                message_list = []
    except IOError as ex:
        print('I just caught an IOError-----' + str(ex))
        time.sleep(1)
        pass # temporary interruption, re-try request
    except TwitterRequestError as e:
        print('I just caught a TwitterRequestError-----' + str(e))
        if e.status_code == 420:
            time.sleep(300)
            pass # Hit the rate limit, wait 5 minutes
        else:
            time.sleep(5)
            pass # There is a HTTP error, re-try request
    except TwitterConnectionError as tce:
        print('I just caught a TwitterConnectionError-----' + str(tce))
        time.sleep(0.25)
        pass # temporary interruption, re-try request
    except ConnectionError as ce:
        print('I just caught a ConnectionError-----' + str(ce))
        time.sleep(1)
        pass # temporary interruption, re-try request


