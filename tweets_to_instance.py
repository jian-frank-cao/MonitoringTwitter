# export GOOGLE_APPLICATION_CREDENTIALS="/home/jccit_caltech_edu/COVID-19/twitter-277319-a2f64f6845eb.json"

path = "/home/jccit_caltech_edu/COVID-19/"
import os
os.chdir(path)
import config
import time
import requests
import uuid
from datetime import datetime, timezone
from requests import HTTPError, ConnectionError

## Parameters
consumer_key = config.consumer_key  
consumer_secret = config.consumer_secret
file_path = "./tweets/"
file_time = 900
headers = []
timer = []
data = []

# Function to get the bearer token
def get_bearer_token(key, secret):
    response = requests.post("https://api.twitter.com/oauth2/token",
                             auth=(key, secret),
                             data={'grant_type': 'client_credentials'},
                             headers={"User-Agent": "TwitterDevCovid19StreamQuickStartPython"})
    if response.status_code != 200:
        print(response.status_code)
        print(response.text)
        raise Exception("Bearer token error")
    body = response.json()
    print("Bearer token is ready.")
    return body['access_token']


# Function to save tweets in instance
def save_data(item, partition):
    global data, timer
    data.append(str(item, 'utf-8'))
    if not isinstance(timer, float):
        timer = time.time()
    if (time.time()-timer) > file_time:
        timer = time.time()
        ts = datetime.now(timezone.utc)
        file_name = ("twitter-covid19-" + str(partition) + "-" +
                     ts.strftime("%Y-%m-%d-%H-%M-%S") +
                     "-" + str(uuid.uuid4()) + '.txt')
        with open(file_path+file_name, 'w') as file_object:
            file_object.write("{}".format("".join(data)))
        print(str(ts) + " ----- " + str(len(data)) + " tweets.")
        data = []


# Function to launch the monitor
def monitor_launch(partition):
    global headers
    timeout_start = None
    response = requests.get("https://api.twitter.com/labs/1/tweets/stream/covid19?partition={}".format(partition),
                            headers={"User-Agent": "TwitterDevCovid19StreamQuickStartPython",
                                     "Authorization": "Bearer {}".format(get_bearer_token(consumer_key, consumer_secret))},stream=True)
    headers = response.headers
    print("Connected to Twitter COVID-19 endpoint (partition {})".format(partition))
    print("Collecting tweets...")
    for response_line in response.iter_lines():
        if response_line:
            timeout_start = None
            save_data(response_line, partition)
        elif timeout_start:
            if time.time() - timeout_start > 20:
                raise TypeError("Stream stopped delivering for more than 20 seconds.")
        else:
            timeout_start = time.time()


def main(partition):
    error_time = time.time()
    error_count = 0
    while True:
        try:
            monitor_launch(partition)
        except IOError as ex:
            print('I just caught an IOError --------------------------\n' + str(ex))
            pass # temporary interruption, re-try request
        except HTTPError as he:
            print('I just caught an HTTPError ------------------------\n' + str(he))
            pass # temporary interruption, re-try request
        except ConnectionError as ce:
            print('I just caught a ConnectionError -------------------\n' + str(ce))
            pass # temporary interruption, re-try request
        except TypeError as te:
            print('I just caught a TimeoutError ----------------------\n' + str(te))
            pass # temporary interruption, re-try request
        print(headers)
        if time.time() - error_time > 900:
            error_count = 0
        wait = min(0.25 * 2**error_count, 30)
        error_time = time.time()
        error_count = error_count + 1
        print('Wait {} seconds before retrying...'.format(wait))
        time.sleep(wait)


if __name__ == "__main__":
    main()


