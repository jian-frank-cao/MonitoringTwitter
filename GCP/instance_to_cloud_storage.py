# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 11:07:17 2020

@author: Jian Cao
"""
## Environment ----------------------------------------------------------------
import os
credential_path = ''
os.system('export GOOGLE_APPLICATION_CREDENTIALS="{}"'.format(credential_path))
os.chdir('/home/jccit_caltech_edu/COVID-19/')
from google.cloud import storage
from google.cloud.storage import Blob
import datetime
import time
import pandas as pd
import subprocess
from itertools import compress

## Functions ------------------------------------------------------------------
# Function that compresses file
def compress_file(path, out_path, filename):
    out_filename = filename.split('.')[0]
    subprocess.call(['7z', 'a', out_path + out_filename + '.7z', path + filename])
    print(out_filename + ' is compressed.')
    # return({"filename":out_filename+'.7z', "path":out_path})


# Function that list the files
def list_files(path, pattern):
    out = pd.DataFrame([x for x in os.listdir(path) if pattern in x],
                        columns = ['files'])
    out['time'] = [datetime.datetime.strptime(x[18:37], '%Y-%m-%d-%H-%M-%S') for x in out['files']]
    out = out.sort_values(by = 'time', ascending = True).reset_index()
    return(out['files'])


# Function that connects Cloud Storage
def connect_storage(bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    return(bucket)


# Function that uploads files
def upload_to_storage(path, filename, bucket, bucket_folder, bucket_name):
    blob = Blob(bucket_folder + filename, bucket)
    try:
        blob.upload_from_filename(path + filename)
        print(filename +
              ' is uploaded to "{}:{}"'.format(bucket_name, bucket_folder))
        retry = False
    except:
        print(filename + ' needs to be re-uploaded.')
        retry = True
    return({'filename':filename, 'retry':retry})


# Function that list the old files
def list_old_files(files, days):
    timestamps = [datetime.datetime.strptime(x[18:37], '%Y-%m-%d-%H-%M-%S') for x in files]
    out = [files[i] for i in list(compress(range(len(files)), [x < datetime.datetime.now()-datetime.timedelta(days=days) for x in timestamps]))]
    return out


# main
def main(bucket_name, bucket_folder, path_7z, path_tweets,
         marker, wait_retry = 5, wait_next = 900):
    while True:
        retry = False
        # Compress files
        files_tweets = list_files(path_tweets, '.txt')
        for file_tweets in files_tweets:
            compress_file(path_tweets, path_7z, file_tweets)
            os.remove(path_tweets + file_tweets)
        # Upload files
        bucket = connect_storage(bucket_name)
        files_7z = list_files(path_7z, '.7z').tolist()
        if '.7z' in marker:
            files_upload = files_7z[files_7z.index(marker)+1:]
        else:
            files_upload = files_7z
        for file_7z in files_upload:
            response = upload_to_storage(path_7z, file_7z, bucket,
                                         bucket_folder, bucket_name)
            if not response['retry']:
                marker = file_7z
            else:
                retry = True
                continue
        # Delete old files
        files_old = list_old_files(files_7z, 7)
        if files_old:
            for file_old in files_old:
                os.remove(path_7z + file_old)
        # Finish
        if retry:
            print('Retrying...')
            time.sleep(wait_retry)
        else:
            print('Waiting for next update...')
            time.sleep(wait_next)


## main -----------------------------------------------------------------------
if __name__ == "__main__":
    main()
