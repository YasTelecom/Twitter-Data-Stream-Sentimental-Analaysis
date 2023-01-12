from textblob import TextBlob

def sentiment_analysis(text):
    analysis = TextBlob(text)
    #print("\n polarity=",analysis.sentiment.polarity)
    if analysis.sentiment.polarity > 0.1:
    
        return 'Positive'+" polarity=",analysis.sentiment.polarity
    elif abs(analysis.sentiment.polarity) < 0.1: #and analysis.sentiment.polarity > -0.1:
        return 'Neutral'+" polarity=",analysis.sentiment.polarity
    else:
        return 'Negative'+" polarity=",analysis.sentiment.polarity



print("----------------------------------------")
text = "I love ecology, it gives a meaning to my life."
print(text+ "\n" ,sentiment_analysis(text))
# Output: Positive
text = "I am not interested in ecology, I really have no opinion. "
print(text+ "\n" ,sentiment_analysis(text))

text = "Those who love ecology are really idiots."
print(text+ "\n", sentiment_analysis(text))
# Output: Negative

print("----------------------------------------")