""" Execution script"""
import pandas as pd
import socket
import tweepy
from tweepy import Stream
from twitter_credentials import Credentials
# from twitter_connect import Connect
from twitter_connect import MyStreamListener
from twitter_connect import TweetsListener


if __name__ == '__main__':

    # twitter_credentials file holds private Twitter keys
    api_key = Credentials.api_key()
    api_secret_key = Credentials.api_secret_key()
    access_token = Credentials.oauth_token()
    access_token_secret = Credentials.oath_token_secret()

    # tweepy calls API with user keys
    auth = tweepy.OAuthHandler(api_key, api_secret_key)
    auth.set_access_token(access_token, access_token_secret)

    # choose topic and number of tweets to receive
    twitter_topic = "manchin"
    how_many_tweets = 30
    how_many_seconds = 10
    # choose delivery method ("screen", "csv", "database", or "stream")
    method = "stream"

    if method == "screen":
        # gather tweets and print on screen
        api = tweepy.API(auth, wait_on_rate_limit=True)
        public_tweets = tweepy.Cursor(api.search_tweets, q=twitter_topic).items(how_many_tweets)
        for tweet in public_tweets:
            print(tweet.text)

    if method == "csv":
        # gather tweets
        api = tweepy.API(auth, wait_on_rate_limit=True)
        public_tweets = tweepy.Cursor(api.search_tweets, q=twitter_topic).items(how_many_tweets)
        tweets = [tweet.text for tweet in public_tweets]
        # create pandas data frame and send to csv file
        tweet_text_df = pd.DataFrame(data=tweets, columns=['data'])
        tweet_text_df.to_csv('twitter_download.csv')

    if method == "database":
        # gather tweets and save in database
        print("FUNCTION NOT IMPLEMENTED YET")

    if method == "stream":

        # create API object
        # api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        api = tweepy.API(auth, wait_on_rate_limit=True)
        # twitter_stream = tweepy.Stream(api_key, api_secret_key, access_token, access_token_secret)
        """
        # create new class to
        tweets_listener = MyStreamListener(api)
        twitter_stream = tweepy.Stream(api_key, api_secret_key, access_token, access_token_secret)
        print(3)
        # on_status() will be called when a tweet matches the filter topic word(s)
        twitter_stream.filter(track=twitter_topic, languages=["en"])
        print(4)

       
        # use tweepy to stream tweets for use by Spark streaming
        # create the stream with api object and stream object
        myStreamListener = MyStreamListener()
        myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
        # start stream with filter term
        myStream.filter(track=twitter_topic, is_async=True)
        """

        # server (local machine) creates listening socket
        s = socket.socket()
        host = "0.0.0.0"
        port = 5555
        s.bind((host, port))
        print('socket is ready')
        # server (local machine) listens for connections
        s.listen(4)
        print('socket is listening')
        # return the socket and the address on the other side of the connection (client side)
        c_socket, addr = s.accept()
        print("Received request from: " + str(addr))

        print('start sending data from Twitter to socket')
        # authenticate based on credentials above
        # auth.set_access_token(bearer_token_scott)
        # auth.set_access_token(bearer_token_scott, access_secret)
        # start sending data from the Streaming API
        twitter_stream = Stream(auth, TweetsListener(c_socket))
        twitter_stream.filter(track=twitter_topic, languages=["en"])

    print("That's all, Folks!")
