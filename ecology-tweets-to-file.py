from kafka import KafkaConsumer
# from kafka import KafkaProducer
import json


# Create a Kafka consumer to read tweets from the "ecology-tweets" topic
consumer = KafkaConsumer("ecology-tweets", bootstrap_servers="localhost:9092", group_id =None, auto_offset_reset ='earliest')

# # Create a Kafka producer to send eventually tweets
# producer = KafkaProducer(bootstrap_servers="localhost:9092")


# Read tweets from the "raw-tweets" topic
# comments for visualisation if needed
for msg in consumer:
    tweet = json.loads(msg.value)
    
    # tweet dict avec 'content', 'date', 'lang'
    # # à remplir ici par ce que vous voulez
    with open('ecology_tweets.txt', 'a+', encoding="utf-8") as file:
            file.write(str(tweet))
