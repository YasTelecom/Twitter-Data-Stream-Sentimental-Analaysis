from kafka import KafkaConsumer
from kafka import KafkaProducer
#from transformers import pipeline # for sentiment classification
import json
import time
import pandas as pd
from textblob import TextBlob
from jinja2 import Template
import re
from wordcloud import WordCloud, STOPWORDS
import numpy as np
from PIL import Image
import matplotlib.pyplot as plt

# Create a Kafka consumer to read tweets from the "en-tweets" and "fr-tweets" topics
consumer = KafkaConsumer("ecology-tweets", bootstrap_servers="localhost:9092", group_id =None, auto_offset_reset ='earliest')

# Create a Kafka producer to send tweets to the "positive-tweets" and "negative-tweets" topics
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# import a pretrained sentiment classificator
#sentiment_analysis = pipeline("sentiment-analysis",model="finiteautomata/bertweet-base-sentiment-analysis")
def sentiment_analysis(text):
    analysis = TextBlob(text)
    #print("\n polarity=",analysis.sentiment.polarity)
    if analysis.sentiment.polarity > 0.1:
    
        return 'Positive', analysis.sentiment.polarity
    elif abs(analysis.sentiment.polarity) < 0.1: 
        return 'Neutral',analysis.sentiment.polarity
    else:
        return 'Negative',analysis.sentiment.polarity



def tweets_to_worldCloud(all_tweets):
    # Clean the text by removing mentions (@username), hashtags (#), and emojis
    all_tweets=all_tweets.lower()
    all_tweets = re.sub(r'@\w+', '', all_tweets)
    all_tweets = re.sub(r'#\w+', '', all_tweets)
    all_tweets = re.sub(r'[^\w\s]', '', all_tweets)
    all_tweets = re.sub(r'\becology\b', '', all_tweets)

    # Load the Twitter logo image as the mask for the word cloud
    twitter_mask = np.array(Image.open("twitter_mask.png"))

    # Generate the word cloud with the Twitter logo shape and light color scheme
    wc = WordCloud(background_color="white", max_words=2000, mask=twitter_mask,
                stopwords=STOPWORDS, contour_color='steelblue',min_word_length=4)

    wordcloud = wc.generate(all_tweets)
    return wordcloud

df = pd.DataFrame(columns=['tweet', 'date', 'language','sentiment','polarity'])

j=0

DF_MAX_LENGTH=100000

for msg in consumer:

    if df.shape[0] >=DF_MAX_LENGTH:
        df = df.drop(df.index[0])
    j+=1
    # Parse the tweet from the message
    tweet = json.loads(msg.value)
    text = tweet["content"]
    date=tweet["date"]
    language=tweet["lang"]
 
    sentiment,polarity = sentiment_analysis(text) # sentiment classification



    

    # Check the sentiment of the tweet and write it to the appropriate topic
    if sentiment=='Positive':
        tweet = json.dumps(tweet).encode('utf-8')
        row = {'tweet': str(text), 
                'date': date, 
                'language': language, 
                'sentiment': sentiment, 
                'polarity': polarity}
        df = pd.concat([df, pd.DataFrame([row], 
                        columns=row.keys())], 
                        ignore_index=True)


        producer.send("positive-tweets", tweet)
        # print("A positive tweets have been added to the positive-tweets topic") 

    elif sentiment == 'Neutral' : 
        tweet = json.dumps(tweet).encode('utf-8')
        row = {'tweet': str(text), 
                'date': date, 
                'language': language, 
                'sentiment': sentiment, 
                'polarity': polarity}
        df = pd.concat([df, pd.DataFrame([row], 
                        columns=row.keys())], 
                        ignore_index=True)


        producer.send("neutral-tweets", tweet)



    elif sentiment == 'Negative' : 
        tweet = json.dumps(tweet).encode('utf-8')
        row = {'tweet': str(text), 
                'date': date, 
                'language': language, 
                'sentiment': sentiment, 
                'polarity': polarity}
        df = pd.concat([df, pd.DataFrame([row], 
                        columns=row.keys())], 
                        ignore_index=True)


        producer.send("negative-tweets", tweet)
        # print("A negative tweets have been added to the negative-tweets topic") 

    if j%1000==0:

        all_tweets = df['tweet'].str.cat(sep=' ')
        all_tweets_positive=df[df['sentiment']=='Positive']['tweet'].str.cat(sep=' ')
        all_tweets_neutral=df[df['sentiment']=='Neutral']['tweet'].str.cat(sep=' ')
        all_tweets_negative=df[df['sentiment']=='Negative']['tweet'].str.cat(sep=' ')
        #print(all_tweets[j-10:j])
        wordcloud = tweets_to_worldCloud(all_tweets)
        wordcloud.to_file("wordcloud.png")
        
        wordcloud = tweets_to_worldCloud(all_tweets_positive)
        wordcloud.to_file("positive.png")
        
        wordcloud = tweets_to_worldCloud(all_tweets_neutral)
        wordcloud.to_file("neutral.png")

        wordcloud = tweets_to_worldCloud(all_tweets_negative)
        wordcloud.to_file("negative.png")

       
        positive_rows = df.loc[df['sentiment'] == 'Positive']
        positive_count = positive_rows.shape[0]
        neutral_rows = df.loc[df['sentiment'] == 'Neutral']
        neutral_count = neutral_rows.shape[0]
        negative_rows = df.loc[df['sentiment'] == 'Negative']
        negative_count = negative_rows.shape[0]

        total_count = positive_count + neutral_count + negative_count
        positive_percent = int((positive_count / total_count) * 100)
        neutral_percent = int((neutral_count / total_count) * 100)
        negative_percent = int((negative_count / total_count) * 100)

        print(positive_count)
        print(positive_percent)
        
        html_table = df.tail(10).to_html()

        template = Template("""

<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">

<script>
    setTimeout(function(){
       location.reload();
    }, 5000);
    </script>



<html>
  <head>
    <title>Green Tweet</title>
    <style>
      /* Add some styles for the overall layout of the page */
      body {

        max-width: 1200px; /* set the max-width to 1200px */
         margin: 0 auto;
        font-family: Arial, sans-serif;

        padding: 0;
      }

      table {
  width: 1000px; /* set the width to 800px */
  max-width: 100%; /* set max-width to 100% */
  margin: 0 auto;
  border-collapse: collapse;
  table-layout: fixed;
    }


      /* Style the h1 and h2 elements */
      h1, h2 {
        text-align: center;
        margin: 20px 0;
      }


      /* Style the table headers */
      th {
        background-color: #4CAF50;
        color: white;
        padding: 12px;
        border: 1px solid #ddd;
      }

      /* Style the table cells */
      td {
        padding: 12px;
        border: 1px solid #ddd;
        text-align: left;
      }

      /* Style the image */
      img {
        width: 600px;
        margin: 20px auto;
        display: block;
      }
    </style>
  </head>
  <body>
    <h1>Green Tweet</h1>
    <p>Let see how people talk about ecology... </p>
    <img src="wordcloud.png" alt="image" >

    <table>
            <tr>
                <th>Sentiment</th>
                <th>Count</th>
                <th>Percentage</th>
            </tr>
            <tr>
                <td>Positive</td>
                <td>{{ positive_count }}</td>
                <td>{{ positive_percent }}%</td>
            </tr>
            <tr>
                <td>Neutral</td>
                <td>{{ neutral_count }}</td>
                <td>{{ neutral_percent }}%</td>
            </tr>
            <tr>
                <td>Negative</td>
                <td>{{ negative_count }}</td>
                <td>{{ negative_percent }}%</td>
            </tr>
        </table>

 <div class="row">
  <div class="col-4">
    <img src="positive.png" alt="image1" style="width:300px; height:300px;">
    <p>    Positive WordCloud </p>
  </div>
  <div class="col-4">
    <img src="neutral.png" alt="image2" style="width:300px; height:300px;">
    <p>   Neutral WordCloud</p>
  </div>
  <div class="col-4">
    <img src="negative.png" alt="image3" style="width:300px; height:300px;">
    <p>  Negative WordCloud</p>
  </div>
</div>

    <h2>Last tweets about ecology</h2>
    <div class="table-responsive">
    <table>
        {{ html_table }}
    </table>
    </div>
  </body>
</html>






""")
        with open("report.html", "w", encoding='utf-8') as f:
            f.write(template.render(html_table=html_table, 
                                    positive_count=positive_count, 
                                    positive_percent=positive_percent, 
                                    neutral_count=neutral_count, 
                                    neutral_percent=neutral_percent, 
                                    negative_count=negative_count, 
                                    negative_percent=negative_percent))
        