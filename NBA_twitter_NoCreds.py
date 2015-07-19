import sys
import tweepy
import datetime
import urllib
import signal
import json
from boto.s3.connection import S3Connection
from boto.s3.key import Key

class TweetSerializer:
    out = None
    first = True
    count = 0
    fname = "empty.json"

    def start(self):
        self.count += 1
        self.fname = "tweets-"+str(self.count)+".json"
        self.out = open(self.fname,"w")
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


#S3 connection credentials
conn = S3Connection('...','...')
myBucket = conn.get_bucket('nba_twitter_mids205') 

#Twitter API credentials
consumer_key = "..."
consumer_secret = "..."

access_token = "..."
access_token_secret = "..."

#setup twitter access
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

ts = TweetSerializer()

#Data is chunked by day and by hashtag of interest
day_range = [['2015-06-20','2015-06-21'],['2015-06-21','2015-06-22'],['2015-06-22','2015-06-23'],['2015-06-23','2015-06-24'],['2015-06-24','2015-06-25'],['2015-06-25','2015-06-26'],['2015-06-26','2015-06-27']]

hashtag_range = ["#NBAFinals2015","#Warriors","#NBAFinals2015 AND #Warriors"]
hashtag = ""
day = ["",""]

#setup interrupt handler
def interrupt(signum, frame):
    print "\nInterrupted, closing ..."
    ts.end()
    out = open("interrupt.log","w")
    out.write("Interrupted At:\n")
    print(hashtag)
    print(day)
    out.write(hashtag+"\n")
    out.write(day[0]+"\n")
    out.write(day[1]+"\n")
    out.close()
    exit(1)

signal.signal(signal.SIGINT, interrupt)

#Loop over hashtags and over days. Create a new json file and add tweets to it using cursor functionality. once all tweets in that day / hashtag are read into the json file move it into the S3 bucket
for hashtag in hashtag_range:
    q = urllib.quote_plus(hashtag)
    for day in day_range:
        ts.start()
        print(day[0])
        print(day[1])
        for tweet in tweepy.Cursor(api.search,q=q,since=day[0], until=day[1]).items():
            ts.write(tweet)
        ts.end()
        k = Key(myBucket)
        keyname = day[0]+'_'+hashtag
        k.key = keyname.replace(" ","_")
        k.set_contents_from_filename(ts.fname)

