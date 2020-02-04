from TwitterAPI import TwitterAPI
import time
import jsonpickle
import config


## Setting Parameters --------------------------------------------------------------------------------------------------------
maxTweets = 10000000 # Some arbitrary large number
tweetsPerQry = 100  # this is the max the API permits
fName = 'Downloaded_Tweets.txt' # We'll store the tweets in a text file.


## Requesting tweets ---------------------------------------------------------------------------------------------------------
# Connecting to Twitter API
api = TwitterAPI(config.consumer_key, config.consumer_secret, config.access_token_key, config.access_token_secret)

# If results from a specific ID onwards are read, set since_id to that ID.
# else default to no lower limit, go as far back as API allows
since_id = 1224431131207983111

# If results only below a specific ID are, set max_id to that ID.
# else default to no upper limit, start from the most recent tweet matching the search query.
max_id = 1224442064206626817

# Downloading tweets
tweetCount = 0
print("Downloading max {0} tweets".format(maxTweets))
with open(fName, 'w') as f:
    while tweetCount < maxTweets:
        
        # Searching tweets from REST API with tweet_mode = extended
        # The result contains max_id but not since_id. So use max_id-1 to avoid duplication
        new_tweets = api.request('search/tweets', {'q': '#coronavirus' or '#Coronavirus' or 'coronavirus' or '#Wuhan' or 'wuhan' or
                                '#coronavirusoutbreak' or '#coronaoutbreak' or '#facemask' or
                                'pandemic' or '#pandemic' or '#WHO' or '#2020ncov' or '#Ncov2019' or '#2019Ncov' or
                                '#wuhanvirus' or '#wuhanlockdown' or '#WuhanSARS' or '#SARS2' or
                                '#CoronavirusWho' or '#Coronavirusoutbreak' or '#ChinaVirus' or '#China' or
                                '#Wuhancoronavirus' or '#Wuhanpneumonia' or '#Health' or 'Coronaoutbreak' or
                                '2019-nCoV' or 'Virus' or 'SARS' or 'Corona Virus Outbreak',
                                'max_id': max_id-1, 'since_id': since_id, 'count': tweetsPerQry, 'tweet_mode': 'extended'})
        n_tweets = len(new_tweets.json()['statuses'])
        if n_tweets == 0:
            print("No more tweets found")
            break
        
        # Writing tweets in text file
        for tweet in new_tweets:
            f.write(jsonpickle.encode(tweet, unpicklable=False) +
                    '\n')
            
        tweetCount += n_tweets
        print("Downloaded {0} tweets".format(tweetCount))
        max_id = tweet['id']
        
        # REST API has a rate limit 180 request/15 minutes, which is 1 request/5 seconds
        time.sleep(5.1)
    f.close()

print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))
