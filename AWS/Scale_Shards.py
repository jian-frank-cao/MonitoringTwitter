import json
import time
from time import gmtime, strftime
import datetime
import boto3

## Monitoring the Kinesis stream and scaling shards ----------------------------------------------------------------------------------
# Setting the thresholds
byte_upper       = 800     # KB/second & shard
record_upper     = 800     # records/second & shard
byte_lower       = 500     # KB/second & shard
record_lower     = 500     # records/second & shard
mins_before_deletion = 180 # number of consecutive minutes before deleting a shard
deletion_triger  = 0       # number of consecutive minutes the incoming data have been below the lower thresholds
wait             = 60.0    # Wait for next scaling

# Connecting Kinesis stream
kinesis = boto3.client("kinesis", region_name='us-east-2')
stream_name = 'primary-election'

# Connecting to CloudWatch
cloudwatch = boto3.client("cloudwatch", region_name='us-east-2')

# Connecting to firehose
firehose = boto3.client('firehose', region_name='us-east-2')
firehose_name = '2020-primary-election-monitor'

# Scaling shards
while True:
    # Counting the number of open shards
    list_shards = kinesis.list_shards(StreamName=stream_name)
    n_open_shards = 0
    for item in list_shards['Shards']:
        if 'EndingSequenceNumber' not in item['SequenceNumberRange']:
            n_open_shards = n_open_shards + 1
    
    # Obtaining incoming bytes
    IncomingBytes = cloudwatch.get_metric_statistics(
        Namespace='AWS/Kinesis',
        Dimensions=[
            {
                'Name': 'StreamName',
                'Value': stream_name
            }
        ],
        MetricName='IncomingBytes',
        StartTime=time.time() - 60,
        EndTime=time.time(),
        Period=60,
        Statistics=[
            'Sum'
        ],
        Unit='Bytes'
    )
        
    # Obtaining incoming records
    IncomingRecords = cloudwatch.get_metric_statistics(
        Namespace='AWS/Kinesis',
        Dimensions=[
            {
                'Name': 'StreamName',
                'Value': stream_name
            }
        ],
        MetricName='IncomingRecords',
        StartTime=time.time() - 60,
        EndTime=time.time(),
        Period=60,
        Statistics=[
            'Sum'
        ],
        Unit='Count'
    )
    
    if len(IncomingBytes['Datapoints']) > 0 and len(IncomingRecords['Datapoints']) > 0:
        bytes_per_s = IncomingBytes['Datapoints'][0]['Sum']
        records_per_s = IncomingRecords['Datapoints'][0]['Sum']
        print(str(datetime.datetime.now()) + '. ' + str(n_open_shards) + ' open shard(s), ' + str(round(bytes_per_s/(60*1024),2)) + ' KB/s, ' + str(round(records_per_s/60,2)) + ' Records/s')
        
        # Putting metrics in an S3 bucket
        response = firehose.put_record(DeliveryStreamName=firehose_name, Record={'Data': json.dumps({u'Timestamp': strftime("%a, %d %b %Y %H:%M:%S", gmtime()), u'OpenShards': n_open_shards, u'IncomingBytes': bytes_per_s, u'IncomingRecords': records_per_s})})
    
        # Adding shards
        if bytes_per_s/(60*1024) > byte_upper * n_open_shards or records_per_s/60 > record_upper * n_open_shards:
            kinesis.update_shard_count(
                StreamName=stream_name,
                TargetShardCount=n_open_shards + 1,
                ScalingType='UNIFORM_SCALING'
            )
        
        # Deleting shards
        if bytes_per_s/(60*1024) < byte_lower * (n_open_shards-1) and records_per_s/60 < record_upper * (n_open_shards-1):
            deletion_triger = deletion_triger + 1
            
        if deletion_triger == mins_before_deletion:
            kinesis.update_shard_count(
                StreamName=stream_name,
                TargetShardCount=n_open_shards - 1,
                ScalingType='UNIFORM_SCALING'
            )
            deletion_triger = 0
            
    else:
        print('Monitor stand by...')
    
    time.sleep(wait)
