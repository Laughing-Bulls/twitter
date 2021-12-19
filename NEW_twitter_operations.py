""" Twitter connection functions """
import json
import requests
from bs4 import BeautifulSoup


class Twitter_In:
    """ Get Twitter stream"""

    @staticmethod
    def get_tweets(auth):
        url = 'https://stream.twitter.com/1.1/statuses/filter.json'
        query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','iphone')]
        query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
        response = requests.get(query_url, auth=auth, stream=True)
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
                    twitterID = tweet_data['user']['screen_name']
                    tweet_source = tweet_data['source']
                    dataSoup = BeautifulSoup(tweet_source)
                    for anchor in dataSoup.find_all('a'):
                       tweet_location = anchor.text
                    source_device = tweet_location.replace(" ", "")
                    device = "TS"+source_device.replace("Twitter", "")

                    # location filter
                    tweet_country_code = "CC"+tweet_data['place']['country_code']
                    if tweet_country_code=="CCUS" and "tter" in tweet_location and "\n" not in tweet_text:
                       iterations=iterations+1
                       print(tweet_text)

                       # delimiter for distinguishing tweet_data
                       fulltext="0|~|0|~|0|~|0|~|0|~|"+tweet_text +'\n'

                       # echo data
                       tcp_connection.send(fulltext.encode())
                except:
                    continue
            else:
                break
