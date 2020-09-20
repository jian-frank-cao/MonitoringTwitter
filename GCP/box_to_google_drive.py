from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build, MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from ftplib import FTP_TLS, error_perm
from googleapiclient.errors import HttpError
import subprocess
import ftplib
import time
import os

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


# Function that uploads file to google drive
def upload_object(file, path, folder_id, service):
    file_metadata = {
        'name': file,
        'parents': [folder_id]
    }
    media = MediaFileUpload(
            path + file,
            chunksize=1048576,
            resumable=True
            )
    response = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
            ).execute()
    print(file + " {" + response.get('id') + "} is uploaded.")


# Function that connects to Box FTP
def connect(box_path):
    ftp = FTP_TLS('ftp.box.com')
    # ftp.debugging = 2
    ftp.login('jccit@caltech.edu', 'cJ19870915@#')
    ftp.cwd(box_path[0])
    print("Connected to Box FTP")
    return ftp


# Function that checks if a item is directory
def _is_ftp_dir(ftp_handle, name, guess_by_extension=True):
    """ simply determines if an item listed on the ftp server is a valid directory or not """
    # if the name has a "." in the fourth to last position, its probably a file extension
    # this is MUCH faster than trying to set every file to a working directory, and will work 99% of time.
    if guess_by_extension is True:
        if len(name) >= 4:
            if name[-4] == '.':
                return False
    original_cwd = ftp_handle.pwd()  # remember the current working directory
    try:
        ftp_handle.cwd(name)  # try to set directory to new name
        ftp_handle.cwd(original_cwd)  # set it back to what it was
        return True
    except ftplib.error_perm as e:
        print(e)
        return False
    except Exception as e:
        print(e)
        return False


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


# Function make parent dir
def _make_parent_dir(fpath):
    """ ensures the parent directory of a filepath exists """
    dirname = os.path.dirname(fpath)
    while not os.path.exists(dirname):
        try:
            os.makedirs(dirname)
            print("created {0}".format(dirname))
        except OSError:
            _make_parent_dir(dirname)


# Function that download box object
def _download_ftp_file(ftp_handle, name, dest, overwrite):
    """ downloads a single file from an ftp server """
    #_make_parent_dir(dest.lstrip("/"))
    if not os.path.exists(dest) or overwrite is True:
        try:
            with open(dest, 'wb') as f:
                ftp_handle.retrbinary("RETR {0}".format(name), f.write)
            print("downloaded: {0}".format(dest))
        except FileNotFoundError:
            print("FAILED: {0}".format(dest))
    else:
        print("already exists: {0}".format(dest))


def check_folder(name, parents, service):
    q = "name = '{}' and parents = '{}'".format(name, parents)
    response = service.files().list(
                      q = q,
                      spaces='drive',
                      fields='files(id, name)'
                      ).execute()
    print(response.get('name') + response.get('id'))
    return response.get('id')

    
def get_folder_id(name, service):
    page_token = None
    # mimeType = 'application/vnd.google-apps.folder'
    q = "name='{}'".format(name) 
    response = service.files().list(
            q = q,
            spaces = 'drive',        
            pageSize = 10,
            fields = "nextPageToken, files(id, name)",
            pageToken = page_token
            ).execute()
    items = response.get('files', [])
    for item in items:
        print('Found folder {}'.format(item['name']))
    if len(items) == 1:
        return items[0]['id']
    else:
        return None


# Function that compresses file
def compress_file(path, out_path, filename):
    out_filename = filename.split('.')[0]
    subprocess.call(['7z', 'a', out_path + out_filename + '.7z', path + filename])
    print(out_filename + ' is compressed.')
    return({"filename":out_filename+'.7z', "path":out_path})


# main
def main(path, download_path, drive_folder, box_path, compress):
    os.chdir(path)
    # exp retries
    error_time = time.time()
    error_count = 0
    # connect to google drive
    service = connect_drive()
    # connect to box
    ftp = connect(box_path)
    # start from level 1
    level = 1
    finish = False
    backup = None
    current = [None]
    drive_folder_id = [get_folder_id(drive_folder[0], service)]
    
    
    while not finish:
        retry = False
        ftp.cwd(box_path[level - 1])
        files = ftp.nlst()
        files = [x for x in files if x not in ['.','..']]
        if current[level - 1]:
            ind = files.index(current[level - 1])
            if ind < (len(files) - 1):
                target = files[ind + 1]
                current[level- 1] = target
            elif not level == 1:
                box_path = box_path[0:-1]
                drive_folder = drive_folder[0:-1]
                drive_folder_id = drive_folder_id[0:-1]
                current = current[0:-1]
                level = level - 1
                continue
            else:
                finish = True
                continue
        elif files:
            target = files[0]
            current[level- 1] = target
        else:
            if not level == 1:
                box_path = box_path[0:-1]
                drive_folder = drive_folder[0:-1]
                drive_folder_id = drive_folder_id[0:-1]
                current = current[0:-1]
                level = level - 1
                continue
            else:
                finish = True
                continue
                    
        if _is_ftp_dir(ftp, target, False):
            box_path = box_path + [box_path[level - 1] + target + "/"]
            folder_id = create_folder_drive(
                    target,
                    drive_folder_id[level - 1],
                    service
                    )
            drive_folder = drive_folder + [target]
            drive_folder_id = drive_folder_id + [folder_id]
            current = current + [None]
            level = level + 1
            continue
        else:
            try:
                _download_ftp_file(
                        ftp,
                        target,
                        download_path + target,
                        overwrite = True
                        )
                if compress:
                    compress_output = compress_file(download_path, download_path, target)
                    upload_object(
                        compress_output['filename'],
                        compress_output['path'],
                        drive_folder_id[level - 1],
                        service
                        )
                    os.remove(compress_output['path'] + compress_output['filename'])
                else:
                    upload_object(
                        target,
                        download_path,
                        drive_folder_id[level - 1],
                        service
                        )
                backup = target
                os.remove(download_path + target)
            except HttpError as he:
                print('I just caught an HttpError --------------------------\n' + str(he))
                retry = True
                pass # temporary interruption, re-try request
            except error_perm as reason:
                print('I just caught an error_perm --------------------------\n' + str(reason))
                retry = True
                pass # temporary interruption, re-try request
            except ConnectionResetError as cre:
                print('I just caught an ConnectionResetError --------------------------\n' + str(cre))
                retry = True
                pass # temporary interruption, re-try request
            if retry:
                print(current)
                print(box_path)
                print(drive_folder)
                print(drive_folder_id)
                current[level - 1] = backup
                if time.time() - error_time > 900:
                    error_count = 0
                wait = min(0.25 * 2**error_count, 30)
                error_time = time.time()
                error_count = error_count + 1
                print('Wait {} seconds before retrying...'.format(wait))
                time.sleep(wait)


if __name__ == '__main__':
    path = "/home/jccit_caltech_edu/box_to_gd/"
    download_path = "/home/jccit_caltech_edu/box_to_gd/data/"
    box_path = input('Box Folder: ')
    drive_folder = input('Google Drive Folder: ')
    compress = input('Compress File? (Yes/No): ')
    box_path = ['/{}/'.format(box_path)]
    drive_folder = [drive_folder]
    if compress.lower() == 'yes':
        compress = True
    else:
        compress = False
    
    main(
        path,
        download_path,
        drive_folder,
        box_path,
        compress
    )

    
    
    
