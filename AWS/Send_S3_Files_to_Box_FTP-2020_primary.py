from ftplib import FTP_TLS, error_perm
import boto3
import config
import time


## Sending S3 files to Box ---------------------------------------------------------------------------------------------------
box_path = ['/2020-Primary-Election/Election-Day-Voting/',
            '/2020-Primary-Election/Voter-Fraud/',
            '/2020-Primary-Election/Remote-Voting/',
            '/2020-Primary-Election/Voter-ID/',
            '/2020-Primary-Election/Polling-Places/',
            '/2020-Primary-Election/OC-LA-SoS/',
            '/2020-Primary-Election/Super-Tuesday/']
bucketname = ['2020-primary-election-group1',
              '2020-primary-election-group2',
              '2020-primary-election-group3',
              '2020-primary-election-group4',
              '2020-primary-election-group5',
              '2020-primary-election-group6',
              '2020-primary-election-group7']
wait_retry = 60.0  # Seconds before each retry
wait_next = 3300.0 # Seconds before each update

# Using marker as a starting point (not include) for list_objects request
marker = ['','','','','','','']
marker_old = marker

# Function used to connect to Box FTP
def connect():
    ftp = FTP_TLS(config.box_host)
    ftp.debugging = 2
    ftp.login(config.box_username, config.box_passwd)
    return ftp

# Connecting to S3 buckets
s3 = boto3.client('s3', region_name='us-east-2')

# Pushing S3 files to Box folder
while True:
    ftp = connect()
    for i in range(7):
        ftp.cwd(box_path[i])
        response = s3.list_objects(Bucket=bucketname[i], Marker=marker[i])
        marker_old[i] = marker[i]
        retry = 0
        if 'Contents' in response:
            list_objects = response['Contents']
            marker[i] = list_objects[-1]['Key'] 
            for item in list_objects:
                key = item['Key']
                file = s3.get_object(
                        Bucket=bucketname[i],
                        Key=key
                        )['Body']
                ftp_command = str('STOR ' + key)
                try:
                    ftp.storbinary(ftp_command, file)
                except error_perm as reason:
                    if list_objects.index(item) > 0:
                        marker[i] = list_objects[list_objects.index(item)-1]['Key']
                    else:
                        marker[i] = marker_old[i]
                    if str(reason)[:3] != '551':
                        raise IOError # Need to fix this error before re-try
                    else:
                        retry = 1
                        break # Temporary error, re-try uploading
                file.close()
    ftp.quit()
    if retry == 1:
        print('Retrying...')
        time.sleep(wait_retry)
    else:
        print('Waiting for next update...')
        time.sleep(wait_next)
        
