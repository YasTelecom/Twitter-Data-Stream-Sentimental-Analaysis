from kafka import KafkaConsumer
from kafka import KafkaProducer
from transformers import pipeline # for sentiment classification
import json
import time


# Create a Kafka consumer to read tweets from the "en-tweets" and "fr-tweets" topics
consumer = KafkaConsumer("ecology-tweets", bootstrap_servers="localhost:9092", group_id =None, auto_offset_reset ='earliest')

# Create a Kafka producer to send tweets to the "positive-tweets" and "negative-tweets" topics
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# import a pretrained sentiment classificator
sentiment_analysis = pipeline("sentiment-analysis",model="finiteautomata/bertweet-base-sentiment-analysis")

# Read tweets from the "en-tweets" and "fr-tweets" topics
for msg in consumer:
    # Parse the tweet from the message
    tweet = json.loads(msg.value)
    texte = tweet["content"]

    result = sentiment_analysis(texte)[0] # sentiment classification

    # # comments for visualisation
    # print()
    # print(texte)

    # print("Label:", result['label'])
    # print("Confidence Score:", result['score'])
    

    # Check the sentiment of the tweet and write it to the appropriate topic
    if result['label']=='POS':
        tweet = json.dumps(tweet).encode('utf-8')
        producer.send("positive-tweets", tweet)
        # print("A positive tweets have been added to the positive-tweets topic") 

    elif result['label'] == 'NEG' : 
        tweet = json.dumps(tweet).encode('utf-8')
        producer.send("negative-tweets", tweet)
        # print("A negative tweets have been added to the negative-tweets topic") 

    # else :
    #     print("We didn't save the tweet as it was neutral")

    # time.sleep(1)