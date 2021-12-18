""" Execute to connect with Twitter and echo content """
import requests_oauthlib
import socket
from twitter_credentials import Credentials
from NEW_twitter_operations import Twitter_In

# obtain access codes for Twitter account
oauth_token = Credentials.oauth_token()
oauth_token_secret = Credentials.oath_token_secret()
api_key = Credentials.api_key()
api_secret_key = Credentials.api_secret_key()

auth = requests_oauthlib.OAuth1(api_key, api_secret_key, oauth_token, oauth_token_secret)

# set up communications port
tcp_host = '127.0.1.1'
tcp_port = 5555
conn = None

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((tcp_host, tcp_port))
s.listen(1)
conn, addr = s.accept()

"""
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((tcp_host, tcp_port))
    s.listen(1)
    conn, addr = s.accept()
    with conn:
        print("Connected to: ", addr)
        while True:
            data = conn.recv(1024)
            if not data:
                break
            conn.sendall(data)    
"""

# call functions to get tweets and echo
response = Twitter_In.get_tweets(auth)
Twitter_In.tweet_streamer(response, conn)
