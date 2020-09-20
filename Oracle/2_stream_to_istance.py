import os
os.chdir('/home/ubuntu/twitter_monitor/coronavirus')
import oci
import uuid
import time
import json
import datetime
import subprocess
from base64 import b64decode
from oci.exceptions import ServiceError

# Parameters
compartment_id = "ocid1.compartment.oc1..aaaaaaaadltovre7x2qlxrdjehvhcsgqlbnzsvrhdkp6soqnhgw35tlmv7ja"
download_path = "/home/ubuntu/twitter_monitor/coronavirus/downloaded/"
stream_name = "COVID-19-A"
max_tweets_per_second = 200
offset = 39072281
partitions = 1

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

# Function to compress file
def compress_file(path, filename):
    subprocess.call(['7z', 'a', path+filename+'.7z', path+filename])
    return({"filename":filename+'.7z', "path":path})

# Function to set the cursor by partition
def get_cursor_by_partition(client, stream_id, partition):
    print("Creating a cursor for partition {}".format(partition))
    cursor_details = oci.streaming.models.CreateCursorDetails(
        partition=partition,
        #offset = offset,
        type=oci.streaming.models.CreateCursorDetails.TYPE_TRIM_HORIZON)
    response = client.create_cursor(stream_id, cursor_details)
    cursor = response.data.value
    return cursor

# Get or create a stream with stream_name
stream = get_or_create_stream(stream_admin_client, compartment_id, stream_name,
                          partitions, stream_admin_client_composite).data
stream_id = stream.id
# Connect to the stream using the provided message endpoint.
stream_client = oci.streaming.StreamClient(config_oci,
                                           service_endpoint=stream.messages_endpoint)

# Save tweets in stream into files
cursor = get_cursor_by_partition(stream_client, stream_id, partition="0")
data = []
while True:
    try:
        get_response = stream_client.get_messages(stream_id,
                                                  cursor,
                                                  limit=max_tweets_per_second)
        if get_response.data:
            for item in get_response.data:
                data.append(b64decode(item.value.encode()).decode())
            cursor = get_response.headers["opc-next-cursor"]
            offset = item.offset
        # save tweets into files
        if len(data) > 15000:
            ts = datetime.datetime.strptime(json.loads(data[-1])["created_at"], '%a %b %d %H:%M:%S %z %Y')
            filename = ("twitter-coronavirus-" + stream_name[-1] + "-" +
                        ts.strftime("%Y-%m-%d-%H-%M-%S") +
                        "-" + str(uuid.uuid4()))
            with open(download_path + filename, "w") as file:
                file.write("".join(data))
            compress_response = compress_file(download_path, filename)
            # os.remove(download_path + filename)
            data = []
            print("=========================== {} is saved".format(compress_response["filename"]))
    except IOError as ex:
        print('I just caught an IOError-----' + str(ex))
        time.sleep(1)
        pass # temporary interruption, re-try request
    except ServiceError as se:
        print('I just caught an ServiceError-----' + str(se))
        time.sleep(5)
        pass # temporary interruption, re-try request
    # batches, as to avoid too many http requests.
    time.sleep(1)



