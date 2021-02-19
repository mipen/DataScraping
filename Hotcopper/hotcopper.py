import bs4
from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime
import pandas as pd

headers = {
    'authority': 'hotcopper.com.au',
    'cache-control': 'max-age=0',
    'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'referer': 'https://hotcopper.com.au/',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'cookie': '__cfduid=dc947278ab64fddaddaec4817c5302bde1613428095; _ga=GA1.3.1543151537.1613428139; hubspotutk=d7b042b7c081b979eb17d617bc7b5c1e; _gid=GA1.3.301962870.1613686039; __hstc=51568950.d7b042b7c081b979eb17d617bc7b5c1e.1613428142523.1613428142523.1613686044671.2; __hssrc=1; __gads=ID=2f891aff244c63ea:T=1613686053:S=ALNI_MZpGV16We9fPZH2jSpKSNCWUiBnzg; xf_threads_terms_conditions_pop=1; xf_show_post_view=1; hc_user_tracker=JeJqKeNhxJf3R8XomEUen_izfF0hMXMB; xf_session=ft883c8dv5ia3sv0hfo6dlaq6m; xf_user=779725%2C839e1007246bb73324dea8f6ddd3a9c9fc77ba6d; xf_disable_autoplay_videos=0; __hssc=51568950.14.1613686044671',
}
urls = ['https://hotcopper.com.au/discussions/asx---general/']
date: datetime.now()

#TODO::Create prompt when starting the script that asks the user how many posts and what date to run the script for.
#TODO::Work out how to remove URLs from the message body.

def getMainTableHTML(soup):
    postsArea = soup.find(name='main')
    return postsArea.find(name='table', class_='table is-fullwidth').tbody


def checkWithinTimePeriod(timeStr):
    #TODO: Return true if the passed time/date is within the given range.
    return True


def cleanText(dirtyText):
    return re.sub(' +', ' ', dirtyText.replace('\t', '').replace('\r', '').replace('\n', '').replace('\"','').replace('\'','').replace('”','').replace('“','').rstrip().lstrip())


def getPostTimestamp(metaDataArea):
    dateStr = metaDataArea.find('div', attrs={'class':'post-metadata-date'}).text.replace(' ','')
    timeStr = metaDataArea.find('div',attrs={'class':'post-metadata-time'}).text
    return (datetime.strptime(dateStr+timeStr, '%d/%m/%y %H:%M').timestamp()-datetime(2000,1,1).timestamp())*1000*1000*1000


def getMessageBody(message):
    return cleanText(''.join(BeautifulSoup(message, "lxml").findAll(text=True)).rstrip())


postUrls = []
limit = 20
count = 0

#TODO: Enable the scraper to go to the next page of comments.
for url in urls:
    print("Searching", url)
    # Request page from website
    request = requests.get(url, headers=headers).text
    soup = BeautifulSoup(request, 'lxml')
    # Find the table which contains the list of posts
    table = getMainTableHTML(soup)
    # Loop through the rows and find the ones which are within the given timespan
    for row in table.find_all('tr', attrs={'class': ''}, recursive=False):
        # Find the row which holds the post time
        tr_time = row.find_all('td', attrs={'class': [
                               'stats-td is-hidden-touch alt-tr', 'stats-td is-hidden-touch']}, recursive=False)[-1]
        if(checkWithinTimePeriod(tr_time.text)):
            # If true, we want to record the row
            tr_a = row.find('td', attrs={'class': [
                            'title-td no-overflow has-text-weight-semibold', 'title-td no-overflow has-text-weight-semibold alt-tr']}).find('a')
            postUrls.append("https://www.hotcopper.com.au"+tr_a['href'])

        count += 1
        if(count >= limit):
            break

if(len(postUrls)>0):
    print("Found", len(postUrls), "posts. Processing..")

    postsData=[]

    for postUrl in postUrls:
        print('\r',"Processing post", postUrl, end='')
        request = requests.get(postUrl, headers=headers).text
        soup = BeautifulSoup(request, 'lxml')
        mainArea = soup.find('main', attrs={'id': 'thread-page'})
        postArea = mainArea.find('div', attrs={'class': 'hc-content columns'}).find('div', attrs={'class': 'column left'}).find('div', attrs={
        'class': 'thread-content'}).find('div', attrs={'class': 'thread-full-paywall'}).find('div', attrs={'class': 'message-columns'})
        
        subject = cleanText(mainArea.find('nav', attrs={'class': 'breadcrumb is-left'}).find('li', attrs={'class': 'is-active'}).text)
        timestamp = getPostTimestamp(postArea.find('div', attrs={'class': 'post-metadata'}).find('div', attrs={'class': 'post-metadata-inner'}))
        messageBody = getMessageBody(postArea.find('div', attrs={'class': 'message-post'}).find('article').find('blockquote').text)
        username= postArea.find('div',attrs={'class':'post-userdata'}).find('div',attrs={'class':'user-username'}).a.text
        postNumber = postArea.find('div', attrs={'class': 'post-metadata'}).find('div', attrs={'class': 'post-metadata-inner'}).find('div', attrs={'class':'post-link'}).a.text

        postsData.append([timestamp,username,subject,messageBody,postNumber])

    df = pd.DataFrame(postsData,columns=['transactTime', 'sender','subject', 'messageBody','postNumber'])
    fp='hotcopper_'+str(datetime.now().date())+'.csv'
    df.to_csv(path_or_buf=fp, index=False)

    print('\n')
    print('Finished processesing',len(postUrls),'posts. Saved to:', fp)
else:
    print('No posts found.')


