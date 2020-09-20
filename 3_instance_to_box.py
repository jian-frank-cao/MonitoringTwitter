import os
os.chdir('/home/ubuntu/twitter_monitor/coronavirus')
from ftplib import FTP_TLS, error_perm
import time
import datetime
import config_jian
from itertools import compress

## Sending 7z files to Box ---------------------------------------------------------------------------------------------------
box_path = "/Twitter-Coronavirus/"
download_path = "/home/ubuntu/twitter_monitor/coronavirus/downloaded/"
wait_retry = 60.0  # Seconds before each retry
wait_next = 3300.0 # Seconds before each update
marker = "twitter-coronavirus-A-2020-04-15-23-20-59-c3205c70-b2d4-40e6-86e9-9add9b0fcb0d.7z"

# Function used to connect to Box FTP
def connect():
    ftp = FTP_TLS(config_jian.box_host)
    ftp.debugging = 2
    ftp.login(config_jian.box_username,
              config_jian.box_passwd)
    ftp.cwd(box_path)
    return ftp

# Function to check if directory exists
def check_wd(ftp, path):
    try:
        resp = ftp.sendcmd('MLST ' + path)
        if 'Type=dir' in resp:
            return True
        else:
            return False
    except error_perm as e:
        return False

# Function to create a directory
def create_wd(ftp, box_path, year, month, day):
    if check_wd(ftp, box_path + year + "/") == False:
        ftp.mkd(box_path + year + "/")
    if check_wd(ftp, box_path + year + "/" + month + "/") == False:
        ftp.mkd(box_path + year + "/" + month + "/")
    if check_wd(ftp, box_path + year + "/" + month + "/" + day + "/") == False:
        ftp.mkd(box_path + year + "/" + month + "/" + day + "/")

# Function to upload file to Box
def upload_to_box(ftp, path, filename):
    retry = 0
    ftp_command = str('STOR ' + filename)
    try:
        ftp.storbinary(ftp_command, open(path + filename, 'rb'))
    except error_perm as reason:
        if str(reason)[:3] != '551':
            raise IOError # Need to fix this error before re-try
        else:
            retry = 1
    return({"filename":filename, "path":path, "retry":retry})

# Function to move files to subfolders
def move_to_subfolder(ftp, filename, box_path):
    year  = filename[22:26]
    month = filename[27:29]
    day   = filename[30:32]
    path  = box_path + year + "/" + month + "/" + day + "/"
    create_wd(ftp, box_path, year, month, day)
    ftp.rename(box_path + filename, path + filename)

# Function to identify the files more than a week old
def find_old_files(files):
    timestamps = [datetime.datetime.strptime(x[22:41], '%Y-%m-%d-%H-%M-%S') for x in files]
    out = [files[i] for i in list(compress(range(len(files)), [x < datetime.datetime.now()-datetime.timedelta(days=7) for x in timestamps]))]
    return out

# Pushing 7z files to Box folder
while True:
    ftp = connect()
    files = sorted([x for x in os.listdir(download_path) if ".7z" in x])
    files_upload = files[files.index(marker)+1:]
    files_delete = find_old_files(files)
    marker_old = marker
    retry = 0
    if files_upload:
        for file in files_upload:
            upload = upload_to_box(ftp, download_path, file)
            if upload['retry']==0:
                move_to_subfolder(ftp, upload["filename"], box_path)
                marker = file
                print(file+" is done.")
            else:
                if files_upload.index(file) > 0:
                    marker = files_upload[files_upload.index(file)-1]
                else:
                    marker = marker_old
                retry = 1
                break
    if files_delete:
        for file in files_delete:
            os.remove(download_path+file)
    ftp.quit()
    if retry == 1:
        print('Retrying...')
        time.sleep(wait_retry)
    else:
        print('Waiting for next update...')
        time.sleep(wait_next)    
            
