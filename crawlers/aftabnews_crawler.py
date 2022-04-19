import requests
import time
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
from textblob import TextBlob
import sys
from unidecode import unidecode

server_url = "http://aftabnews.ir/fa/news/"
path_log = "./log/aftabnews.log"

mongo_server = "localhost"
mongo_port = 27017
client = MongoClient(mongo_server, mongo_port)
db = client['news_sites']
news = db['aftabnews']

if len(sys.argv)>1:
    start = sys.argv[1]
    end = sys.argv[2]
else:
    f = open(path_log, "r+") #551349,551798
    start, end = str(f.read()).split(",")

docs = []
for i in range(int(start), int(end)):
    try:
        link = server_url + str(i)
        print(link)
        request = requests.get(link)
        content = request.text

        soup = BeautifulSoup(content, "html.parser")

        title = str(soup.select('h1.title')[0].getText().strip())
        subtitle = None
        if len(soup.select('div.subtitle'))>0:
            subtitle = str(soup.select('div.subtitle')[0].getText().strip())
        else:
            subtitle = ""

        body = str(soup.select('div.body')[0].getText().strip())

        comments_count = soup.findAll("a", {"href": "#comments"})
        if len(comments_count) > 0:
            comments_count = unidecode(comments_count[0].getText().strip())
            comments_count = re.findall(r'\d+', comments_count)[0]
        else:
            comments_count = 0

        date, time  = soup.select('div.news_pdate_c')[0].getText().strip().replace("تاریخ انتشار:","").split('-')
        date = str(date).strip()
        time = str(time).strip()

        doc = {
                "title": title,
                "body" : body,
                "abstract": subtitle,
                "time": time,
                "date_shamsi": date,
                "comment_count": comments_count,
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