import utils

import pandas as pd
from datetime import datetime, timedelta
import snscrape.modules.twitter as sntwitter

dates = utils.time_interval("2020-07-31", "2020-11-01", interval = 1)   
    
query      = 'swarmapp.com/'
max_tweets = 50000

for date1, date2 in dates:
    
    print('From {} to {}'.format(date1, date2))
    
    search    =  query + ' ' + 'since:' + date1 + ' ' + 'until:' + date2

    df_tweets = pd.DataFrame(columns = ['Date', 'Tweet', 'Swarm'])

    t = 5000
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(search).get_items()):

        if i > max_tweets:
            break
            
        info      = {'Date': tweet.date, 'Tweet': tweet, 'Swarm': tweet.outlinks}
        df_tweets = df_tweets.append(info, ignore_index = True)

        if i == t:
            print("Collected {} tweets".format(t))
            t = t + 5000

    filename = "{}_{}".format(date1, date2)
    df_tweets.to_csv("tweet_data/tweets_" + filename  + ".csv", index = False)