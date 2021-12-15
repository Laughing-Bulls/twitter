""" Execution script"""
import pandas as pd
import tweepy
from twitter_connect import Connect
from twitter_credentials import Credentials


if __name__ == '__main__':

    # twitter_credentials file holds private Twitter keys
    api_key = Credentials.api_key()
    api_secret_key = Credentials.api_secret_key()

    # tweepy calls API with user keys
    auth = tweepy.OAuthHandler(api_key, api_secret_key)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    # choose topic and number of tweets to receive
    twitter_topic = "heisman"
    how_many_tweets = 30
    how_many_seconds = 10
    # choose delivery method ("screen", "csv", "database", or "stream")
    method = "csv"

    if method == "screen":
        # gather tweets and print on screen
        public_tweets = tweepy.Cursor(api.search_tweets, q=twitter_topic).items(how_many_tweets)
        for tweet in public_tweets:
            print(tweet.text)

    if method == "csv":
        # gather tweets
        public_tweets = tweepy.Cursor(api.search_tweets, q=twitter_topic).items(how_many_tweets)
        tweets = [tweet.text for tweet in public_tweets]
        # create pandas data frame and send to csv file
        tweet_text_df = pd.DataFrame(data=tweets, columns=['data'])
        tweet_text_df.to_csv('twitter_download.csv')

    print("That's all, Folks!")

