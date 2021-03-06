# MonitoringTwitter
Monitoring Twitter Project - The Division of Humanities and Social Sciences, California Institute of Technology

A technical report for the Twitter monitors: [Reliable and Efficient Long-Term Social Media Monitoring](https://arxiv.org/abs/2005.02442)

- [The AWS Twitter Monitors](https://github.com/jian-frank-cao/MonitoringTwitter#the-aws-twitter-monitors)
- [The GCP Twitter Monitors](https://github.com/jian-frank-cao/MonitoringTwitter#the-gcp-twitter-monitors)
- [The Oracle Twitter Monitors](https://github.com/jian-frank-cao/MonitoringTwitter#the-oracle-monitors)

## The AWS Twitter Monitors
![flow chart](./monitoringtwitter.png)

### 1.	Requesting Twitter stream from Twitter API
Use twitter developer’s credentials to request real time twitter stream and filter the stream using keywords of interest.

[Put_Tweets_in_Kinesis_Stream.py](./AWS/Put_Tweets_in_Kinesis_Stream.py)

### 2.	Putting filtered tweets in shards of Kinesis stream
Put tweets that are relevant to target keywords in Kinesis stream. Specifically, the tweets are labeled by partition keys and directed to available shards. Each shard is a data timeline, it gives us access to data that were put in the stream as early as 24 hours ago. The writing limit of a shard is 1000 records/s and 1 MB/s.

[Put_Tweets_in_Kinesis_Stream.py](./AWS/Put_Tweets_in_Kinesis_Stream.py)

### 3.	Automatically scaling the number of shards
Monitor usage of each shard and automatically close a redundant shard or open a new shard if incoming data are exceeding the limit.

[Scale_Shards.py](./AWS/Scale_Shards.py)

### 4.	Storing the tweets in Kinesis stream to an S3 bucket
Put all tweets got from the past 10 minutes in a JSON file and store it in an S3 bucket. The reading limit of a shard is 2 MB/s.

### 5.	Pushing the JSON files in S3 bucket to a Box folder
Every hour, the new JSON files in the S3 bucket are pushed to Box FTP, so that the target Box folder will be updated once an hour and is available to all team members.

[Send_S3_Files_to_Box_FTP.py](./AWS/Send_S3_Files_to_Box_FTP.py)

### Tools
[Search_Tweets_from_REST_API.py](./AWS/Search_Tweets_from_REST_API.py)

[Parse_Kinesis_Stream_to_MariaDB.py](./AWS/Parse_Kinesis_Stream_to_MariaDB.py)

[Read_JSON_Files.py](./AWS/Read_JSON_Files.py)

## The GCP Twitter Monitors

### A. Requesting Tweets from Twitter API and Publish them in Pub/Sub Topics
Use twitter developer’s credentials to request real time twitter stream, filter the stream using keywords of interest, then publish the collected tweets in a Pub/Sub Topic.

[tweets_to_pubsub.py](./GCP/tweets_to_pubsub.py)

### B. Requesting Tweets from Twitter API and Save them on a Compute Instance
Use twitter developer’s credentials to request real time twitter stream, filter the stream using keywords of interest, then save the tweets temporarily on a compute instance.

[tweets_to_instance.py](./GCP/tweets_to_instance.py)

### C. Moving Collected Tweets from the Compute Instance to Cloud Storage
Moving the tweets from the compute instance's hard drive to a Cloud Storage folder.

[instance_to_cloud_storage.py](./GCP/instance_to_cloud_storage.py)

### D. Moving Collected Tweets from the Compute Instance to Google Drive
Moving the tweets from the compute instance's hard drive to a Google Drive folder.

[instance_to_google_drive.py](./GCP/instance_to_google_drive.py)

### E. Moving Files from Box FTP to Google Drive
Moving all files in a Box folder to a Google Drive folder.

[box_to_google_drive.py](./GCP/box_to_google_drive.py)


## The Oracle Monitors

### 1. Requesting Tweets from Twitter API and Publish them in an Oracle Stream

[1_twitter_to_stream.py](./Oracle/1_twitter_to_stream.py)

### 2. Read the Tweets from the Oracle Stream and Save them on a Compute Instance

[2_stream_to_istance.py](./Oracle/2_stream_to_istance.py)

### 3. Moving Collected Tweets from the Compute Instance to Box FTP

[3_instance_to_box.py](./Oracle/3_instance_to_box.py)

### Tools

[extract_text.py](./Oralce/4_extract_text.py)
