import tweepy
import json
import time
from kafka import KafkaProducer

# Configure the authentication keys for the Twitter API
bearer_token="AAAAAAAAAAAAAAAAAAAAAMxXkAEAAAAAFC3teGdOH64EKRkhCiWANfWxwvk%3DvPTipAtXE0gr2YHUcBTp0MQ8bYz65LAbRyiiGLFIbCTvw4FZiD"
client = tweepy.Client(bearer_token=bearer_token)


# Parameters
<<<<<<< HEAD
nMaxTweet = 10
=======
nMaxTweet = 100
>>>>>>> a3914292376dd48d20363c9edc88a022866c3a09
keyword = 'ecology' 
query = keyword + ' -is:retweet'
topic_name = 'ecology-tweets'

# Producer
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# Paginator
paginator = tweepy.Paginator(
    client.search_recent_tweets, 
<<<<<<< HEAD
    query = query, 
    tweet_fields = ['lang'], 
    max_results = 100
=======
    query=query, 
    tweet_fields=['lang'], 
    max_results=100
>>>>>>> a3914292376dd48d20363c9edc88a022866c3a09
    )

# add tweet to topic in a json
while True :
    for tweet in paginator.flatten(limit=nMaxTweet):
    
<<<<<<< HEAD
    tweet = json.dumps(tweet_dict).encode('utf-8')
    producer.send(topic_name, tweet)
    # print("Sending message {} to topic: {}".format(tweet, topic_name)) 
=======
        content = str(tweet.text)
        lang = str(tweet.lang)
>>>>>>> a3914292376dd48d20363c9edc88a022866c3a09

        tweet_dict = {
            "content": content,
            "lang": lang,
        }
        if tweet_dict["lang"] == 'en':
            # print(tweet_dict["lang"])
            tweet = json.dumps(tweet_dict).encode('utf-8')
            producer.send(topic_name, tweet)
            # print("Sending message {} to topic: {}".format(tweet, topic_name)) 

    nMaxTweet = 5
    time.sleep(1)
