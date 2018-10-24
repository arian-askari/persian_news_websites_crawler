import requests
import time
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
from textblob import TextBlob
import sys
from unidecode import unidecode

server_url = "https://www.tabnak.ir/fa/news/"
path_log = "./log/tabnak.log"

mongo_server = "localhost"
mongo_port = 27017
client = MongoClient(mongo_server, mongo_port)
db = client['news_sites']
news = db['tabnak']

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

        title = str(soup.select('h1.Htag')[0].getText().strip())
        subtitle = None
        if len(soup.select('div.subtitle'))>0:
            subtitle = str(soup.select('div.subtitle')[0].getText().strip())
        else:
            subtitle = ""
        body = str(soup.select('div.body')[0].getText().strip())

        view_count = soup.select("div.news_hits")[0].getText().strip()
        view_count = re.findall(r'\d+', view_count)[0]
        if len(view_count)==0:
            view_count = 0

        comments_count = soup.findAll("a", {"href": "#comments"})
        if len(comments_count)>0:
            comments_count = unidecode(comments_count[0].getText().strip())
        else:
            comments_count = 0

        date_shamsi, time = soup.select('sapn.fa_date')[0].getText().strip().split('-')
        date_shamsi = str(date_shamsi)
        time = str(time)
        date_georgian = soup.select('span.en_date')[0].getText().strip()

        doc = {
                "title": title,
                "body" : body,
                "abstract": subtitle,
                "time": time,
                "date_shamsi": date_shamsi,
                "date_georgian" : date_georgian,
                "comment_count": comments_count,
                "view_count" : view_count,
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