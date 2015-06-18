import sys
import tweepy
import datetime
import urllib
import signal
import json
import boto

class TweetSerializer:
    out = None
    first = True
    count = 0
    def start(self):
        self.count += 1
        fname = "tweets-"+str(self.count)+".json"
        self.out = open(fname,"w")
        self.out.write("[\n")
        self.first = True
    
    def end(self):
        if self.out is not None:
            self.out.write("\n]\n")
            self.out.close()
        self.out = None
    
    def write(self,tweet):
        if not self.first:
            self.out.write(",\n")
        self.first = False
        self.out.write(json.dumps(tweet._json).encode('utf8'))


def interrupt(signum, frame):
    print "Interrupted, closing ..."
    # magic goes here
    exit(1)

signal.signal(signal.SIGINT, interrupt)

consumer_key = "vETvId9gNkn2O6vx4E5Qk1t19"
consumer_secret = "yaUDR370u6BlkE93aUouGZfWDlXqmAGSkzLVbVAsgzmAjOFHUy"

access_token = "2482974258-w6KFwPBZyxD4jwsFc3Cnn2O72C0PsytB2CHEM0S"
access_token_secret = "TGXePdRLmgFQwLVSbWmuWDLfutYFt1MDV9aqkMmIqPUrv"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

q = urllib.quote_plus("#NBAFinals2015")  # URL encoded query

ts = TweetSerializer()
ts.start()

for tweet in tweepy.Cursor(api.search,q=q,since='2015-06-08', until='2015-06-14').items(10):
    print tweet.created_at, tweet.text
    ts.write(tweet)

ts.end()

#this works... still to do - chunking and saving to S3 using boto
