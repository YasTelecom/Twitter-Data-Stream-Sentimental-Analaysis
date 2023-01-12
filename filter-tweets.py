from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import time

# Create a Kafka consumer to read tweets from the "raw-tweets" topic
consumer = KafkaConsumer("raw-tweets", bootstrap_servers="localhost:9092", group_id =None, auto_offset_reset ='earliest')

# Create a Kafka producer to send tweets to the "en-tweets" and "fr-tweets" topics
producer = KafkaProducer(bootstrap_servers="localhost:9092")


# Read tweets from the "raw-tweets" topic
# comments for visualisation if needed
for msg in consumer:
    tweet = json.loads(msg.value)
    
    # Check the language of the tweet and write it to the appropriate topic
    if tweet["lang"] == "en":
        tweet = json.dumps(tweet).encode('utf-8')
        producer.send('en-tweets', tweet)
        # print("A english tweets have been added to the en-tweets topic")    
        
    elif tweet["lang"] == "fr":
        tweet = json.dumps(tweet).encode('utf-8')
        producer.send('fr-tweets', tweet)
        # print("A french tweets have been added to the fr-tweets topic")

    # time.sleep(1)