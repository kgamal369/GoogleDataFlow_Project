#Kgamal 30/6/2018 
import json
import pandas as pd
import numpy 
import random
import cPickle as pickle
import matplotlib.pyplot as plt
import np



tweets_data_path = '../MindValley_Project/twitter_data.txt'
tweets_data = []
tweets_file = open(tweets_data_path, "r")
for line in tweets_file:
    try:
        tweet = json.loads(line)
        tweets_data.append(tweet)
    except:
        continue
        print len(tweets_data)
tweets = pd.DataFrame()
tweets['text'] = map(lambda tweet: tweet['text'], tweets_data)
tweets['lang'] = map(lambda tweet: tweet['lang'], tweets_data)
tweets['daati'] = map(lambda tweet: tweet['created_at'], tweets_data)
tweets['country'] = map(lambda tweet: tweet['place']['country'] if tweet['place'] != None else None, tweets_data)


np.savetxt('../MindValley_Project/twitter_OP.txt', tweets,delimiter = '|', fmt='%s')



