import mysql.connector
from mysql.connector import Error
from dateutil import parser
import json
import time
import boto3
import config


## Storing all records from Kinesis stream in a local/EC2 MariaDB ------------------------------------------------
dbName = ''      # Name of the SQL database
stream_name = '' # Name of Kinesis stream     

# Function used to connect to MariaDB and insert data
def connect(tweetFields, userFields, rtFields, qtFields, textFields):
    try:
        con = mysql.connector.connect(host=config.dbHost_ec2,
                                      database=dbName,
                                      user=config.dbUser_ec2,
                                      password=config.mysqlPassword)
        if con.is_connected():
            cursor = con.cursor()
            query_user = "INSERT INTO user_data (user_id, user_screenname, user_name, location, description, followers, friends, status_count, user_tz, user_created_at, verified) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE user_id=user_id, user_screenname = %s, user_name=%s, location = %s, description = %s, followers = %s, friends= %s, status_count = %s, user_tz = %s, verified = %s"
            query_tweet = "INSERT INTO twitter_data (status_id, user_id, created_at, location, followers, friends, status_count, RT_status, QT_status, text_status_id ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            query_text = 'INSERT INTO text_data(text_status_id, full_text, RT_count, QT_count, favorite_count, reply_count) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE text_status_id=text_status_id, full_text=full_text, RT_count = %s, QT_count = %s, favorite_count = %s, reply_count = %s'
            cursor.execute(query_user, (userFields[0],  userFields[1],  userFields[2],  userFields[3],  userFields[4],  userFields[5],  userFields[6],  userFields[7],  userFields[8],  userFields[9],  userFields[10], userFields[1],  userFields[2],  userFields[3],  userFields[4],  userFields[5],  userFields[6],  userFields[7],  userFields[8],  userFields[10]))
            cursor.execute(query_text, (textFields[0], textFields[1],textFields[2],textFields[3],textFields[4],textFields[5],textFields[2],textFields[3],textFields[4],textFields[5]))
            cursor.execute(query_tweet, (tweetFields[0], userFields[0], tweetFields[1], userFields[3], userFields[5], userFields[6], userFields[7], tweetFields[2], tweetFields[3], textFields[0]))
            if len(rtFields)!=0:
                query_rt = "INSERT INTO RT_Network_Info (status_id, from_user_id, from_status_id) VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE status_id=status_id"
                cursor.execute(query_rt, (tweetFields[0], rtFields[0], rtFields[1]))
            if len(qtFields)!=0:
                query_qt = "INSERT INTO QT_Network_Info (status_id, from_user_id, from_status_id) VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE status_id=status_id"
                cursor.execute(query_qt, (tweetFields[0], qtFields[0], qtFields[1]))
                query_qt_text = 'INSERT INTO text_data(text_status_id, full_text, RT_count, QT_count, favorite_count, reply_count) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE text_status_id=text_status_id, full_text=full_text, RT_count = %s, QT_count = %s, favorite_count = %s, reply_count = %s'
                cursor.execute(query_qt_text, (qtFields[1], qtFields[2], qtFields[3], qtFields[4], qtFields[5], qtFields[6], qtFields[3], qtFields[4], qtFields[5], qtFields[6] ))
            con.commit()
    except Error as e:
        print(e)
    cursor.close()
    con.close()
    return

# Connecting to Kinesis stream
kinesis = boto3.client("kinesis", region_name='us-east-2')
shard_id = 'shardId-000000000000' #names of shards

# Storing tweets in local/EC2 MariaDB
# Setting the initial "last sequence number" as the starting point
pre_shard_it = kinesis.get_shard_iterator(StreamName="twitter",ShardId=shard_id,ShardIteratorType="LATEST")["ShardIterator"]
out = kinesis.get_records(ShardIterator=pre_shard_it)
last_sequence_number = out['Records'][len(out['Records'])-1]['SequenceNumber']

while True:
    # Setting up shard iterator
    shard_it = kinesis.get_shard_iterator(StreamName="twitter",ShardId=shard_id,ShardIteratorType="AFTER_SEQUENCE_NUMBER",StartingSequenceNumber=last_sequence_number)["ShardIterator"]
    
    # Reading records from Kinesis stream
    out = kinesis.get_records(ShardIterator=shard_it)
    
    # Monitoring the process
    n = len(out['Records'])
    start = time.time()
    
    # Storing tweets
    if n > 0:
        last_sequence_number = out['Records'][n-1]['SequenceNumber']
        for item in out['Records']:
            
            raw_data = json.loads(item['Data'])
            status_id = raw_data['id']
            created_at = parser.parse(raw_data['created_at'])
            RT_status = 0
            QT_status = 0
            rtData = []
            qtData=[]
            
            if 'retweeted_status' in raw_data:
                if raw_data['retweeted_status']['truncated'] == True:
                    tweet = raw_data['retweeted_status']['extended_tweet']['full_text']
                else:
                    tweet =  raw_data['retweeted_status']['text']
                RT_status = 1
                from_user_id = raw_data['retweeted_status']['user']['id']
                from_tweet_id = raw_data['retweeted_status']['id']
                retweet_count = raw_data['retweeted_status']['retweet_count']
                quote_count = raw_data['retweeted_status']['quote_count']
                favorite_count = raw_data['retweeted_status']['favorite_count']
                reply_count = raw_data['retweeted_status']['reply_count']
                rtData = [from_user_id, from_tweet_id]
                
            elif 'quoted_status' in raw_data:
                if raw_data['truncated'] == True:
                    tweet = raw_data['extended_tweet']['full_text']
                else:
                    tweet = raw_data['text']
                QT_status = 1
                from_user_id = raw_data['quoted_status']['user']['id']
                from_tweet_id = raw_data['quoted_status']['id']
                qt_text = raw_data['quoted_status']['text']
                qt_retweet_count = raw_data['quoted_status']['retweet_count']
                qt_quote_count = raw_data['quoted_status']['quote_count']
                qt_favorite_count = raw_data['quoted_status']['favorite_count']
                qt_reply_count = raw_data['quoted_status']['reply_count']
                retweet_count = raw_data['retweet_count']
                quote_count = raw_data['quote_count']
                favorite_count = raw_data['favorite_count']
                reply_count = raw_data['reply_count']
                qtData = [from_user_id, from_tweet_id, qt_text, qt_retweet_count, qt_quote_count, qt_favorite_count, qt_reply_count]
                
            else:
                if raw_data['truncated'] == True:
                    tweet = raw_data['extended_tweet']['full_text']
                else:
                    tweet = raw_data['text']
                from_tweet_id = raw_data['id']
                retweet_count = raw_data['retweet_count']
                quote_count = raw_data['quote_count']
                favorite_count = raw_data['favorite_count']
                reply_count = raw_data['reply_count']
                
            userid = raw_data['user']['id']
            screenname = raw_data['user']['screen_name']
            name = raw_data['user']['screen_name']
            location = raw_data['user']['location'] if raw_data['user']['location'] else ''
            description = raw_data['user']['description'] if raw_data['user']['description'] else ''
            verified = raw_data['user']['verified']
            followers_count = raw_data['user']['followers_count']
            friends_count = raw_data['user']['friends_count']
            status_count = raw_data['user']['statuses_count']
            user_created_at = parser.parse(raw_data['user']['created_at'])
            user_tz = raw_data['user']['time_zone'] if  raw_data['user']['time_zone'] else ''
            tweetData = [status_id,  created_at, RT_status, QT_status ]
            userData = [userid, screenname, name, location, description, followers_count, friends_count, status_count, user_tz, user_created_at, verified]
            textData = [from_tweet_id, tweet, retweet_count, quote_count, favorite_count, reply_count]
            
            connect(tweetData, userData, rtData, qtData, textData)
    print(n)
    print(time.time()-start)
    
    # Adjust the waiting time to avoid the 2 MB/s reading limit
    time.sleep(1.0)