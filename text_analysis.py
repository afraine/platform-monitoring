import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from textblob import TextBlob
import re

def hashtags_from_tweets(text):
    return

def sentiment(text):
    text = re.sub(r"(?:\@|https?\://)\S+", "", text).strip()
    text = text.replace("RT ", "")
    try:
        score = SentimentIntensityAnalyzer().polarity_scores(text)
    except:
        nltk.download('vader_lexicon')
    try:
        score = SentimentIntensityAnalyzer().polarity_scores(text)
    except:
        score = None
    return score.get("compound") if score is not None else 0