from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build, MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from ftplib import FTP_TLS, error_perm
import ftplib
import time
import os
import datetime
import subprocess

## Environment ----------------------------------------------------------------
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']

## Functions ------------------------------------------------------------------
# Function that connectrs google drive
def connect_drive():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    service = build('drive', 'v3', credentials=creds)
    print("Connected to Google Drive")
    return service
    
# Function that lists the folders
def search_object(name, service):
    page_token = None
    is_folder = False
    if is_folder:
        mimeType = 'application/vnd.google-apps.folder'
        q = "name='{}', mimeType='{}'".format(name,mimeType)
    else:
        q = "name='{}'".format(name)
    while True:     
        response = service.files().list(
                q = q,
                spaces = 'drive',        
                pageSize = 10,
                fields = "nextPageToken, files(id, name)",
                pageToken = page_token
                ).execute()
        items = response.get('files', [])
        if not items:
            print('No items found.')
        else:
            print('Items:')
            for item in items:
                print(u'{0} ({1})'.format(item['name'], item['id']))
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

# Function that creates folder in google drive
def create_folder_drive(name, parents, service):
    file_metadata = {
        'name': name,
        'parents': [parents],
        'mimeType': 'application/vnd.google-apps.folder'
    }
    file = service.files().create(body=file_metadata,
                                    fields='id').execute()
    print("Folder {} has been created.".format(name))
    return file.get("id")

# Function that check if folder exists
def check_object(name, parents, service, is_folder = True):
    mimeType = "application/vnd.google-apps.folder"
    if is_folder:
        q = "name = '{}' and parents = '{}' and mimeType = '{}'".format(name, parents, mimeType)
    else:
        q = "name = '{}' and parents = '{}' and mimeType != '{}'".format(name, parents, mimeType)
    response = service.files().list(
                      q = q,
                      spaces='drive',
                      fields='files(id, name)'
                      ).execute()
    items = response.get('files', [])
    if not items:
        return False
    else:
        return items[0]['id']
            
# Function that uploads file to google drive
def upload_object(file, path, folder_id, service):
    file_metadata = {
        'name': file,
        'parents': [folder_id]
    }
    media = MediaFileUpload(
            path + file
            )
    response = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
            ).execute()
    print(file + " {" + response.get('id') + "} is uploaded.")
    
# Function to compress file
def compress_file(path, filename):
    subprocess.call(['7z', 'a', path+filename+'.7z', path+filename])
    return({"filename":filename+'.7z', "path":path})
    
    
    
# Function that assigns and creates subfolders
def get_subfolder(name, parents, service):
    year = name[26:30]
    month = name[31:33]
    day = name[34:36]
    year_id = check_object(year, parents, service)
    if not year_id:
        year_id = create_folder_drive(year, parents, service)
    month_id = check_object(month, year_id, service)
    if not month_id:
        month_id = create_folder_drive(month, year_id, service)
    day_id = check_object(day, month_id, service)
    if not day_id:
        day_id = create_folder_drive(day, month_id, service)
    return day_id
    
    
# Function to identify the files more than a week old
def find_old_files(files):
    out = [x for x in files if datetime.datetime.strptime(x[26:45], '%Y-%m-%d-%H-%M-%S') < datetime.datetime.now()- datetime.timedelta(days=7)]
    return out
    
    
# main
def main(path, download_path, parents, marker):
    os.chdir(path) 
    while True:
        # connect to google drive
        service = connect_drive()
        files_raw = sorted([x for x in os.listdir(download_path) if ".7z" not in x])
        for item in files_raw:
            compress = compress_file(download_path, item)
            os.remove(download_path + item)
        files = sorted([x for x in os.listdir(download_path) if ".7z" in x])
        files_upload = files[files.index(marker)+1:]
        files_delete = find_old_files(files)
        retry = False
        if files_upload:
            for file in files_upload:
                try:
                    folder_id = get_subfolder(file, parents, service)
                    upload_object(file, download_path, folder_id, service)
                    marker = file
                except IOError as ex:
                    print('I just caught an IOError--------------------------\n' + str(ex))
                    retry = True
                    continue
                except HttpError as he:
                    print('I just caught an HttpError------------------------\n' + str(he))
                    retry = True
                    continue
                except ConnectionError as ce:
                    print('I just caught a ConnectionError-------------------\n' + str(ce))
                    retry = True
                    continue
        if files_delete:
            for file in files_delete:
                os.remove(download_path + file)
        if retry:
            print("Marker: " + marker)
            print('Retrying...')
            time.sleep(30)
        else:
            print('Waiting for next update...')
            time.sleep(1800) 
            
                    

       
if __name__ == "__main__":
    main(
        path = "/home/jccit_caltech_edu/COVID-19-protest/",
        download_path = "/home/jccit_caltech_edu/COVID-19-protest/data/",
        parents = "1kiVOVE3yQ66lFTy4JgLYVDDcuuBm5A1K",
        marker = "twitter-covid19-protest-1-2020-06-02-22-32-37-bfede778-7eb9-4b0e-abe4-081f279ab4cc.7z"
    )
      
      


