import tweepy
import json
import time
from kafka import KafkaProducer

# Configure the authentication keys for the Twitter API
bearer_token="AAAAAAAAAAAAAAAAAAAAAMxXkAEAAAAAFC3teGdOH64EKRkhCiWANfWxwvk%3DvPTipAtXE0gr2YHUcBTp0MQ8bYz65LAbRyiiGLFIbCTvw4FZiD"
client = tweepy.Client(bearer_token=bearer_token)


# Parameters
nMaxTweet = 1000000
keyword = 'ecology' 
query = keyword + ' -is:retweet'
topic_name = 'ecology-tweets'

# Producer
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# Paginator
paginator = tweepy.Paginator(
    client.search_recent_tweets, 
    query=query, 
    tweet_fields=['lang', 'created_at'], 
    max_results=100
    )

i = 0
for tweet in paginator.flatten(limit=nMaxTweet):

    i += 1
    content = str(tweet.text)
    lang = str(tweet.lang)
    date = str(tweet.created_at)

    tweet_dict = {
        "content": content,
        "lang": lang,
        "date": date
    }
    
    if tweet_dict["lang"] == 'en':
        # print(tweet_dict["lang"])
        tweet = json.dumps(tweet_dict).encode('utf-8')
        producer.send(topic_name, tweet)
        # print("Sending message {} to topic: {}".format(tweet, topic_name)) 

    if i > 100  == 0:
        time.sleep(1)
    
    if i > 100 == 0 and i%5:
        time.sleep(1)