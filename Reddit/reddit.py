import praw
import pandas as pd
from praw.models import MoreComments
from bs4 import BeautifulSoup
from sys import stdout 
from datetime import datetime

reddit = praw.Reddit(client_id="LNE1hH6AHwBRKA",
                    client_secret="EFC-zCeMztlM7UaXq5SIxIv4WcEueA",
                    password="scrapper",
                    username="scrapper123_",
                    user_agent="Data_Scraper")

limit=200
subreddits=['wallstreetbets','algotrading','thewallstreet','tradevol','finance','investing','pennystocks']

for subreddit in subreddits:
    count=1
    allComments = []
    ml_subreddit = reddit.subreddit(subreddit)
    print("\nProcessing", str(limit), "posts from",subreddit,"..")

    for post in ml_subreddit.hot(limit=limit):
        #print("\nProcessing post ", str(count), "/", str(limit))
        s = reddit.submission(url= "https://www.reddit.com/" + post.permalink)
        s.comments.replace_more(limit=0)
        commentsCount = len(s.comments.list())
        curCount=1
        for comment in s.comments.list():
            try:
                body = ''.join(BeautifulSoup(comment.body_html, "lxml").findAll(text=True)).rstrip()
                date = (datetime.fromtimestamp(comment.created_utc)-datetime(2000,1,1)).days
                transactTime=(comment.created_utc-datetime(2000,1,1).timestamp())*1000*1000*1000
                dt=datetime.fromtimestamp(comment.created_utc)
                author = 'Unknown'
                if comment.author is not None:
                    author=comment.author.name
                time=((dt-datetime(dt.year,dt.month,dt.day)).total_seconds()*1000)
                recvTime = (datetime.now().timestamp()-datetime(2000,1,1).timestamp())*1000*1000*1000
                #Schema: date (date), transactTime (timestamp), time (time), recvTime (timestamp), sender (symbol), UUIDsnd (long), recipient (string), UUIDrecip (list long), chatID (string),
                #messageID (string), companyName (symbol), subject (string), filepath (string), sourceData (string), batch (string), assetClass (symbol), messageBody (string), 
                #lemmas (string), tokens (string)
                #                   date, transactTime, time,recvTime,snder,UUIDs,recip,UUIDr,chtID, messageID,compName,subject,filePth, sourceData,                                 batch, asstClss, msgBdy, lems, tokens
                allComments.append([date, transactTime, time, recvTime, author, '', '', '', post.id, comment.id, '', post.title, '', "https://www.reddit.com" + comment.permalink, '', "reddit", body, '', ''])

                print('\r',"Processing post", str(count), "/", str(limit), "(",str(curCount),"/",str(commentsCount),")",end='')
                stdout.flush()
                curCount+=1
            except Exception as err:
                print("\nEncountered error on comment #"+str(curCount)+":", err)
                curCount+=1
                continue
        count+=1
    #Schema: date (date), transactTime (timestamp), time (time), recvTime (timestamp), sender (symbol), UUIDsnd (long), recipient (string), UUIDrecip (list long), chatID (string), 
    #messageID (string), companyName (symbol), subject (string), filepath (string), sourceData (string), batch (string), assetClass (symbol), messageBody (string), lemmas (string), 
    #tokens (string)
    allComments = pd.DataFrame(allComments,columns=['date', 'transactTime', 'time', 'recvTime', 'sender', 'UUIDsnd', 'recipient', 'UUIDrecip','chatID','messageID','companyName','subject',
    'filepath','sourceData', 'batch', 'assetClass', 'messageBody', 'lemmas','tokens'])
    if(len(allComments.index) > 0):
        print("\n","Recorded", len(allComments.index), "comments.")
        allComments.to_csv(path_or_buf="Reddit_"+subreddit + "_" + str(datetime.now().date()) + ".csv", index=False)
    else:
        print("No comments found for subreddit", subreddit)
