import requests
import time
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
from textblob import TextBlob
import sys

server_url = "https://www.khabaronline.ir/detail/"
path_log = "./log/khabaronline.log"

mongo_server = "localhost"
mongo_port = 27017
client = MongoClient(mongo_server, mongo_port)
db = client['news_sites']
news = db['kahabaronline']

if len(sys.argv)>1:
    start = sys.argv[1]
    end = sys.argv[2]
else:
    f = open(path_log, "r+") #812900, 815078
    start, end = str(f.read()).split(",")

docs = []
for i in range(int(start), int(end)):
    try:
        link = server_url + str(i)
        print(link)
        request = requests.get(link)
        content = request.text

        soup = BeautifulSoup(content, "html.parser")

        title = str(soup.select('h2')[0].getText().strip())
        body = str(soup.select('div.body')[0].getText().strip())
        abstract = str(soup.select('div.leadCont')[0].getText().strip())
        time, date = soup.select('span.margin-l-md')[0].getText().strip().split('-')
        time = str(time)
        date = str(date)

        raters  = soup.select('span.voteNewsNum')[0].getText().strip()
        raters = int(re.findall(r'\d+', raters)[0])

        if len(soup.select("div.commentInfo"))>1:
            comments_count = soup.select('div.headerWrappers')[1].getText().strip()
            comments_count = int(re.findall(r'\d+', comments_count)[0])
        else:
            comments_count = 0

        doc = {
                "title": title,
                "body" : body,
                "abstract" : abstract,
                "time" : time,
                "date_shamsi" : date,
                "raters" : raters,
                "comments_count" : comments_count,
                "link": link
        }
        docs.append(doc)
        if len(docs)>=20:
            news.insert_many(docs)
            docs.clear()

            f = open(path_log, "w+")
            f.write(str(i) + "," + str(end))

    except Exception as e:
        print(e)
        pass

