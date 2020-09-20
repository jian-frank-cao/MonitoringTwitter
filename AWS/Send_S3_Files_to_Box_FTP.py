from ftplib import FTP_TLS, error_perm
import boto3
import config
import time


## Sending S3 files to Box ---------------------------------------------------------------------------------------------------
box_path = '/Twitter-Coronavirus/'
bucketname = 'twitter-coronavirus'
wait_retry = 60.0  # Seconds before each retry
wait_next = 3300.0 # Seconds before each update

# Using marker as a starting point (not include) for list_objects request
marker = u'2020/02/10/21/twitter-coronavirus-2-2020-02-10-21-50-09-78c13889-fe60-4e72-9503-32d545d6c530'

# Function used to connect to Box FTP
def connect():
    ftp = FTP_TLS(config.box_host)
    ftp.debugging = 2
    ftp.login(config.box_username, config.box_passwd)
    ftp.cwd(box_path)
    return ftp

# Connecting to S3 buckets
s3 = boto3.client('s3', region_name='us-east-2')

# Pushing S3 files to Box folder
while True:
    response = s3.list_objects(Bucket=bucketname, Marker=marker)
    marker_old = marker
    retry = 0
    if 'Contents' in response:
        list_objects = response['Contents']
        marker = list_objects[-1]['Key'] 
        ftp = connect()
        for item in list_objects:
            key = item['Key']
            file = s3.get_object(
                    Bucket=bucketname,
                    Key=key
                    )['Body']
            ftp_command = str('STOR ' + key[14:])
            try:
                ftp.storbinary(ftp_command, file)
            except error_perm as reason:
                if list_objects.index(item) > 0:
                    marker = list_objects[list_objects.index(item)-1]['Key']
                else:
                    marker = marker_old
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
