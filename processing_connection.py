""" Twitter connection functions """
import json
import requests
from bs4 import BeautifulSoup


class TwitterIn:
    """ Get Twitter stream"""

    @staticmethod
    def create_twitter_url():
        # set up twitter request parameters
        url = 'https://stream.twitter.com/1.1/statuses/filter.json'
        # English tweets in the USA
        query_data = [('language', 'en'), ('locations', '-160,17,-65,50'), ('track', 'iphone')]
        query_url = url + '?' + '&'.join([str(element[0]) + '=' + str(element[1]) for element in query_data])
        return query_url

    @staticmethod
    def get_tweets(auth):
        # return response as serialized JSON content
        query_url = TwitterIn.create_twitter_url()
        response = requests.get(query_url, auth=auth, stream=True)
        if response:
            print("THE TWITTER QUERY WAS SUCCESSFUL")
        print(query_url, response)
        return response

    @staticmethod
    def tweet_streamer(http_resp, tcp_connection):
        iterations = 0
        for data in http_resp.iter_lines():
            if iterations < 1000:
                try:
                    # collect tweet information
                    tweet_data = json.loads(data)
                    tweet_text = tweet_data['text']
                    # twitterID = tweet_data['user']['screen_name']
                    tweet_source = tweet_data['source']
                    soup = BeautifulSoup(tweet_source)
                    for anchor in soup.find_all('a'):
                       tweet_location = anchor.text
                    # source_device = tweet_location.replace(" ", "")
                    # device = "TS"+source_device.replace("Twitter", "")

                    # location filter
                    tweet_country_code = "CC"+tweet_data['place']['country_code']
                    if tweet_country_code=="CCUS" and "tter" in tweet_location and "\n" not in tweet_text:
                       iterations += 1
                       print(tweet_text)

                       # delimiter for distinguishing tweet_data
                       fulltext="0|~|0|~|0|~|0|~|0|~|"+tweet_text +'\n'

                       # echo data
                       tcp_connection.send(fulltext.encode())
                except:
                    continue
            else:
                break
