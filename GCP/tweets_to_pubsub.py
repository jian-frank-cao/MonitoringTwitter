# export GOOGLE_APPLICATION_CREDENTIALS="/home/jccit_caltech_edu/COVID-19/twitter-277319-a2f64f6845eb.json"

path = "/home/jccit_caltech_edu/COVID-19/"
import os
os.chdir(path)
import config
import time
import requests
from google.cloud import pubsub_v1
from requests import HTTPError, ConnectionError

## Parameters
consumer_key = config.consumer_key  
consumer_secret = config.consumer_secret
project_id = "twitter-277319"
topic_name = "twitter-covid19"
partition = 1
result = []
headers = []

# Function to connect to pub sub
def connect_pubsub(project_id, topic_name):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    print("Pub/Sub connection is ready.")
    return publisher, topic_path

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

# Function to publish tweets in pub sub       
def publish_data(item, publisher, topic_path):
    global result
    # data = json.dumps(item)
    # data = data.encode("utf-8")
    future = publisher.publish(topic_path,
                               data = item)
    result = future.result()

# Function to launch the monitor
def monitor_launch(partition, publisher, topic_path):
    global headers
    timeout_start = None
    response = requests.get("https://api.twitter.com/labs/1/tweets/stream/covid19?partition={}".format(partition),
                            headers={"User-Agent": "TwitterDevCovid19StreamQuickStartPython",
                                     "Authorization": "Bearer {}".format(get_bearer_token(consumer_key, consumer_secret))},stream=True)
    headers = response.headers
    print("Connected to Twitter COVID-19 endpoint (partition {})".format(partition))
    print("Publishing tweets in Pub/Sub...")
    for response_line in response.iter_lines():
        if response_line:
            timeout_start = None
            publish_data(response_line, publisher, topic_path)
        elif timeout_start:
            if time.time() - timeout_start > 20:
                raise TypeError("Stream stopped delivering for more than 20 seconds.")
        else:
            timeout_start = time.time()


def main(partition):
    error_time = time.time()
    error_count = 0
    while True:
        publisher, topic_path = connect_pubsub(project_id, topic_name)
        try:
            monitor_launch(partition, publisher, topic_path)
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
        print(result)
        if time.time() - error_time > 900:
            error_count = 0
        wait = min(0.25 * 2**error_count, 30)
        error_time = time.time()
        error_count = error_count + 1
        print('Wait {} seconds before retrying...'.format(wait))
        time.sleep(wait)


if __name__ == "__main__":
    main()


