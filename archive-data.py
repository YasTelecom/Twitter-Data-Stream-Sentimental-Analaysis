from kafka import KafkaConsumer
import json
import time

topics = ["raw-tweets", "en-tweets", "fr-tweets", "positive-tweets", "negative-tweets"]

# Create a Kafka consumer to read tweets from the "raw-tweets", "en-tweets", "fr-tweets", "positive-tweets" and "negative-tweets" topics
consumer = KafkaConsumer(*topics, bootstrap_servers="localhost:9092", group_id =None, auto_offset_reset ='earliest')

# archive the tweets in txt files (we decide to put it in different text files)
for msg in consumer:
    tweet = json.loads(msg.value)
    topic = msg.topic

    # we add the topic name in the dict 
    tweet['topic'] = topic

    # write in the files
    if topic == "raw-tweets" :
        with open('raw_tweets.txt', 'a+', encoding="utf-8") as file:
            file.write(str(tweet) + '\n\n')
    elif topic == "en-tweets" :
        with open('english_tweets.txt', 'a+', encoding="utf-8") as file:
            file.write(str(tweet) + '\n\n')
    elif topic == "fr-tweets" :
        with open('french_tweets.txt', 'a+', encoding="utf-8") as file:
            file.write(str(tweet) + '\n\n')

    elif topic == "positive-tweets" :
        with open('positive_tweets.txt', 'a+', encoding="utf-8") as file:
            file.write(str(tweet) + '\n\n')

    elif topic == "negative-tweets" :
        with open('negative_tweets.txt', 'a+', encoding="utf-8") as file:
            file.write(str(tweet) + '\n\n')