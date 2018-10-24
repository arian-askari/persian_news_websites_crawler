import requests
import time
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
from textblob import TextBlob
import sys
from unidecode import unidecode

server_url = "https://fararu.com/fa/news/"
path_log = "./log/fararu.log"

mongo_server = "localhost"
mongo_port = 27017
client = MongoClient(mongo_server, mongo_port)
db = client['news_sites']
news = db['fararu']

if len(sys.argv)>1:
    start = sys.argv[1]
    end = sys.argv[2]
else:
    f = open(path_log, "r+") #845147, 845829
    start, end = str(f.read()).split(",")

docs = []
for i in range(int(start), int(end)):
    try:
        link = server_url + str(i)
        print(link)
        request = requests.get(link)
        content = request.text

        soup = BeautifulSoup(content, "html.parser")

        title = str(soup.select('div.title_rutitr_body')[0].getText().strip())
        title = re.sub(r"\s+", " ", title, flags=re.UNICODE)

        subtitle = None
        if len(soup.select('div.news_body_lead'))>0:
            subtitle = str(soup.select('div.news_body_lead')[0].getText().strip())
        else:
            subtitle = ""

        body = str(soup.select('div.body')[0].getText().strip())

        comments_count = soup.findAll("a", {"href": "#comments"})
        if len(comments_count) > 0:
            comments_count = unidecode(comments_count[0].getText().strip())
            comments_count = re.findall(r'\d+', comments_count)[0]
        else:
            comments_count = 0

        like_count = soup.select("span.like_number")[0].getText().strip()
        like_count = re.findall(r'\d+', like_count)[0]
        if len(like_count) == 0:
            like_count = 0

        time, date = soup.select('div.news_pdate_c')[0].getText().strip().replace("تاریخ انتشار:","").split('-')
        time = str(unidecode(time))
        date = str(date)

        doc = {
                "title": title,
                "body" : body,
                "abstract": subtitle,
                "time": time,
                "date_shamsi": date,
                "comment_count": comments_count,
                "like_count" : like_count,
                "link": link
        }
        docs.append(doc)
        if len(docs)>=2:
            news.insert_many(docs)
            docs.clear()

            f = open(path_log, "w+")
            f.write(str(i) + "," + str(end))

    except Exception as e:
        print(e)
        pass