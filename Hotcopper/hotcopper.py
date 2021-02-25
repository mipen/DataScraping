import bs4
from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime
import pandas as pd

#Set this to false to run the script without asking the user for settings.
PROMPT_USER_INPUT = True
POST_LIMIT = 1000 #This is the amount of posts to find per url
HISTORICAL_PAGES_LIMIT = 5
ANY_DATE = False
POST_DATE_MIN = datetime(2021,1,1)
POST_DATE_MAX = datetime.now()
DATE_RANGE = False
POST_DATE = datetime.now()

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
urls = ['https://hotcopper.com.au/discussions/asx---general/', 'https://hotcopper.com.au/discussions/asx---day-trading/', 'https://hotcopper.com.au/discussions/asx---by-stock/', 'https://hotcopper.com.au/discussions/asx---short---term---trading/']

#TODO::Work out how to remove URLs from the message body.

def determineSettings():
    global POST_LIMIT
    global POST_DATE
    global HISTORICAL_PAGES_LIMIT
    if(PROMPT_USER_INPUT):
        POST_LIMIT = handleInputInt("Maximum number of posts to pull (min 1)", POST_LIMIT, 1)
        #POST_DATE=handleInputDate("Date to pull from (yyyy.mm.dd). \'any\' for any date.", datetime.now())
        handleDateInput()
        HISTORICAL_PAGES_LIMIT = handleInputInt("Maximum number of pages to go back through (min 1)", HISTORICAL_PAGES_LIMIT, 1)
    if(ANY_DATE):
        s = 'all dates.'
    elif(DATE_RANGE):
        s = POST_DATE_MIN.strftime('%Y.%m.%d') + ' - ' + POST_DATE_MAX.strftime('%Y.%m.%d') + '.'
    else:
        s = POST_DATE.strftime('%Y.%m.%d') + '.'
    print("--- Pulling", str(POST_LIMIT), 'posts from', s,'Going back a maximum of', HISTORICAL_PAGES_LIMIT,'pages. ---')

def handleDateInput():
    global ANY_DATE
    global POST_DATE_MAX
    global POST_DATE_MIN
    global DATE_RANGE
    global POST_DATE
    try:
        res = requestInput("Date to pull from (yyyy.mm.dd). A date range can be added by separating dates with a \'-\', eg: 2021.01.01-2021.02.01. \'any\' for any date.", datetime.now().strftime('%Y.%m.%d'))
        if(res ==''):
            POST_DATE = datetime.now()
            DATE_RANGE = False
        if(res == 'any'):
            ANY_DATE = True
            return
        if('-' in res):
            strs = res.split('-')
            POST_DATE_MIN = datetime.strptime(strs[0], '%Y.%m.%d')
            POST_DATE_MAX = datetime.strptime(strs[1], '%Y.%m.%d')
            DATE_RANGE = True
        else:
            POST_DATE = datetime.strptime(res, '%Y.%m.%d')
            DATE_RANGE = False
    except:
        POST_DATE = datetime.now()
        DATE_RANGE = False
    
def handleInputInt(message, defaultValue, minimumValue):
    try:
        res = int(requestInput(message, defaultValue))
    except ValueError:
        res = defaultValue
    if(res < minimumValue):
        res = minimumValue
    return res

def handleInputDate(message, defaultValue):
    global ANY_DATE
    try:
        res = requestInput(message, defaultValue.strftime('%Y.%m.%d'))
        if(res == 'any'):
            ANY_DATE = True
            res = defaultValue
        else:
            res = datetime.strptime(res,'%Y.%m.%d')
    except ValueError:
        res = POST_DATE
    return res

def requestInput(message, defaultValue):
    return input(message+" ["+str(defaultValue)+"]:")

def getMainTableHTML(soup):
    postsArea = soup.find(name='main')
    return postsArea.find(name='table', class_='table is-fullwidth').tbody

def checkWithinTimePeriod(timeStr):
    if(ANY_DATE):
        return True
    if(DATE_RANGE):
        if (':' in timeStr):
            return POST_DATE_MAX.date() == datetime.now().date()
        else:
            pd = datetime.strptime(timeStr, '%y/%m/%d')
            return pd.date() >= POST_DATE_MIN.date() and pd.date() <= POST_DATE_MAX.date()
    else:
        if (':' in timeStr):
            return datetime.now().date() == POST_DATE.date()
        else:
            pd = datetime.strptime(timeStr, '%y/%m/%d')
            return pd.date() == POST_DATE.date()

def cleanText(dirtyText):
    return re.sub(' +', ' ', dirtyText.replace('\t', '').replace('\r', '').replace('\n', '').replace('\"','').replace('\'','').replace('”','').replace('“','').rstrip().lstrip())

def getPostTimestamp(metaDataArea):
    dateStr = metaDataArea.find('div', attrs={'class':'post-metadata-date'}).text.replace(' ','')
    timeStr = metaDataArea.find('div',attrs={'class':'post-metadata-time'}).text
    return (datetime.strptime(dateStr+timeStr, '%d/%m/%y %H:%M').timestamp()-datetime(2000,1,1).timestamp())*1000*1000*1000

def getMessageBody(message):
    return cleanText(''.join(BeautifulSoup(message, "lxml").findAll(text=True)).rstrip())

postUrls = []
foundCount = 0

determineSettings()

for url in urls:
    for x in range(1, HISTORICAL_PAGES_LIMIT + 1):
        if(x != 1):
            pageUrl = url + 'page-' + str(x)
        else:
            pageUrl = url

        print('\r',"Searching", pageUrl, end='')

        # Request page from website
        request = requests.get(pageUrl, headers=headers).text
        soup = BeautifulSoup(request, 'lxml')
        # Find the table which contains the list of posts
        table = getMainTableHTML(soup)

        # Loop through the rows and find the ones which are within the given timespan
        for row in table.find_all('tr', attrs={'class': ''}, recursive = False):
            #First check if the post has been deleted. If it has, skip it.
            firstRow = row.find('td')
            if('deleted' in firstRow['class'] or 'has-black-link' in firstRow['class']):
                continue
            # Find the td which holds the time the post was made
            try:
                tr_time = row.find_all('td', attrs={'class': ['stats-td is-hidden-touch alt-tr', 'stats-td is-hidden-touch', 'stats-td is-hidden-touch deleted', 'stats-td is-hidden-touch deleted alt-tr']}, recursive=False)[-1]
            except:
                print('An error ocurred while reading table data:\n',row)
            if(checkWithinTimePeriod(tr_time.text)):
                # If true, we want to record the row
                tr_a = row.find('td', attrs={'class': [
                                'title-td no-overflow has-text-weight-semibold', 'title-td no-overflow has-text-weight-semibold alt-tr', 'title-td no-overflow has-text-weight-semibold deleted', 'title-td no-overflow has-text-weight-semibold deleted alt-tr']}).find('a')
                postUrls.append("https://www.hotcopper.com.au" + tr_a['href'])
                foundCount += 1

            if(foundCount >= POST_LIMIT):
                break
        if(foundCount >= POST_LIMIT):
            break
    print('\n',end='')
    foundCount = 0

if(len(postUrls) > 0):
    print("\nFound", len(postUrls), "posts. Processing..")

    postsData=[]
    length = len(postUrls)
    #Loop through all the found posts and extract the data from them
    for idx, postUrl in enumerate(postUrls):
        print('\r', "Processing post", idx + 1, '/', length, '--', postUrl, end='')
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

        postsData.append([timestamp, username, subject, messageBody, postNumber])

    df = pd.DataFrame(postsData,columns=['transactTime', 'sender','subject', 'messageBody','postNumber'])
    fp='hotcopper_' + str(datetime.now().date()) + '.csv'
    df.to_csv(path_or_buf=fp, index=False)

    print('\nFinished processesing', len(postUrls), 'posts. Saved to -->', fp)
else:
    print('\nNo posts found.')


