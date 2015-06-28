import json
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import nltk
import re
import csv

#S3 connection
conn = S3Connection('...','...')
myBucket = conn.get_bucket('nba_twitter_mids205') 

#create tokenizer to only collect the word characters
tokenizer = nltk.tokenize.RegexpTokenizer(r'\w+')

#again loop over the hashtags
hashtag_list = ['#NBAFinals2015$','#NBAFinals2015_AND_#Warriors','^#Warriors']
for hashtag in hashtag_list:
    #filter the keys by the hashtag
    keys = myBucket.get_all_keys()
    keys = [k for k in keys if re.search(hashtag,k.name)]
    doc_list = []
    #load the keys from S3 then extract all the tweet text
    for k in keys:
        tweet_dict = json.loads(k.get_contents_as_string())
        for tweet in tweet_dict:
            doc_list = doc_list + [tweet['text']]
    #tokenize the text then convert all words to lower case
    tokens = [tokenizer.tokenize(txt) for txt in doc_list]
    words = []
    for t in tokens:
        words = words + t
    words = [w.lower() for w in words]
    #create the frequency distribution, save to CSV file and plot histogram
    fdist = nltk.FreqDist(words)
    afile = open(hashtag+"FDIST.csv",'w')
    wr = csv.writer(afile,quoting = csv.QUOTE_ALL)
    for w in fdist.items():
        wr.writerow([w[0].encode('utf-8'),w[1]])
    fdist.plot(20, cumulative=True)
