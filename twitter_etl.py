import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():
    access_key = '68A5yqubY59yv5i7UGMuAj6DX'
    access_secret = 'cDM5Yc3sqLXYHC2PsTiYGL8mF9JfgKaXdxqwIVaXCc56AyEUyB'
    consumer_key = '1370338600609488902-cyEyo9zQaEj0YVlHeCJIOq9SDzmwoO'
    consumer_secret = 'p4yYDfDaN9ZJDMRHMLHmjrvSrViNEOOgJOQJBICdZi4T2'

    #Twitter Authentication
    auth = tweepy.OAuthHandler(access_key,access_secret)
    auth.set_access_token(consumer_key,consumer_secret)

    #Create an API Object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name ='elonmusk',
                            #500 is the maximum allowed count
                            count=500,
                            include_rts=False,
                            #Necessary to keep full text
                            #otherwise only the first 140 words are extracted 
                            tweet_mode = 'extended'
                            )

    tweet_list = []

    for tweet in tweets:
        text= tweet._json['full_text']
        
        refined_tweet = {"user":tweet.user.screen_name,
                        "text":text,
                        'favorite_count':tweet.favorite_count,
                        'retweet_count':tweet.retweet_count,
                        'created_at': tweet.created_at }
        tweet_list.append(refined_tweet)
        
    df = pd.DataFrame(tweet_list)
    df.to_csv('s3://bhupendra-airflow-twitter-bucket/ElonMusk_Twitter_Data.csv')
