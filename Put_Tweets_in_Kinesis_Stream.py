from TwitterAPI import TwitterAPI, TwitterConnectionError, TwitterRequestError
import json
import time
import boto3
import config

## Creating Kinesis stream (only need to do this once) -----------------------------------------------------------------------
"""
client = boto3.client('kinesis', region_name='us-east-2')
response = client.create_stream(
   StreamName='coronavirus',
   ShardCount=1     # number of shards
)
"""


## Putting tweets in Kinesis stream ------------------------------------------------------------------------------------------
# Connecting to Twitter API
api = TwitterAPI(config.consumer_key, config.consumer_secret, config.access_token_key, config.access_token_secret)

# Connecting to Kinesis stream
kinesis = boto3.client("kinesis", region_name='us-east-2')
stream_name = "coronavirus"

# Endless stream
while True:
    try:
        r = api.request('statuses/filter', {'track': '#coronavirus' or '#Coronavirus' or 'coronavirus' or '#Wuhan' or 'wuhan' or
                                            '#coronavirusoutbreak' or '#coronaoutbreak' or '#facemask' or
                                            'pandemic' or '#pandemic' or '#WHO' or '#2020ncov' or '#Ncov2019' or '#2019Ncov' or
                                            '#wuhanvirus' or '#wuhanlockdown' or '#WuhanSARS' or '#SARS2' or
                                            '#CoronavirusWho' or '#Coronavirusoutbreak' or '#ChinaVirus' or '#China' or
                                            '#Wuhancoronavirus' or '#Wuhanpneumonia' or '#Health' or 'Coronaoutbreak' or
                                            '2019-nCoV' or 'Virus' or 'SARS' or 'Corona Virus Outbreak'})
        for item in r:
            if 'text' in item:
                # Randomly assigning partition keys so that tweets can be evenly directed to multiple shards
                kinesis.put_record(StreamName=stream_name, Data=json.dumps(item), PartitionKey=str(int((time.time()*10000000)%128)))
            elif 'disconnect' in item:
                event = item['disconnect']
                if event['code'] in [2,5,6,7]:
                    raise Exception(event['reason']) # something needs to be fixed before re-connecting
                else:
                    break # temporary interruption, re-try request
    except IOError as ex:
        print('I just caught the exception' + str(ex))
        pass # temporary interruption, re-try request
    except TwitterRequestError as e:
        if e.status_code < 500:
            raise # something needs to be fixed before re-connecting
        else:
            pass # temporary interruption, re-try request
    except TwitterConnectionError:
        pass # temporary interruption, re-try request
