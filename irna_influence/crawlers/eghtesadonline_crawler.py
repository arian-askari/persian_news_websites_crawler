import re
import sys
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from unidecode import unidecode

server_url = "http://www.eghtesadonline.com"

mongo_server = "localhost"
mongo_port = 27017
client = MongoClient(mongo_server, mongo_port)
db = client['news_sites']
news = db['eghtesadonline']

def get_news_links(nextpage = ""):
    links_list = []
    base_url = server_url + "/newsstudios/archive/"
    latest_url = base_url +  nextpage

    content = requests.get(latest_url).text
    soup = BeautifulSoup(content, "html.parser")
    nextpage = re.findall("(\?.*)", str(soup.findAll('a', attrs={'class': 'transition02'})[0]['href']))[0]

    tags = soup.select('a.clr04')
    for tag in tags:
        soup = BeautifulSoup(str(tag), 'html.parser')
        href = server_url + soup.a['href']
        links_list.append(href)

    links_list = list(set(links_list))
    link_nextpage_dict = (links_list, nextpage)
    return link_nextpage_dict


pagination_num = 30

news_cnt = 0
nextpage = ""
for i in range(1, pagination_num):
    links, nextpage = get_news_links(nextpage)
    print("nextpage", nextpage)
    docs = []
    for link in links:
        print(link)
        news_cnt += 1
        content = requests.get(link).text
        soup = BeautifulSoup(content, "html.parser")

        body_raw_txt = ''.join([str(i) for i in soup.findAll("div", {"itemprop": "articlebody"})])
        soup_body = BeautifulSoup(str(re.sub('<br.*?>', '\n', body_raw_txt)), "html.parser")

        title = str(soup.h1.getText().strip())

        subtitle = None
        if len(soup.findAll("p", {"itemprop": "description"})) > 0:
            subtitle = str(soup.findAll("p", {"itemprop": "description"})[0].getText().strip())
        else:
            subtitle = ""
        body = str(soup_body.getText())

        date, time = soup.findAll("time", {"itemprop": "datepublished"})[0].getText().strip().replace("تاریخ انتشار:", "").split('-')
        date = str(date).strip()
        time = str(time).strip()

        comments_count = 0
        if(soup.find('ul', {'class': 'level-0'}) != None):
            parent = soup.find('ul', {'class': 'level-0'})
            children = parent.findChildren("li", recursive=False)
            comments_count = len(children)

        doc = {
            "title": title,
            "abstract": subtitle,
            "body": body,
            "date_shamsi": date,
            "time": time,
            "comment_count": comments_count,
            "link": str(link)
        }
        docs.append(doc)

    news.insert_many(docs)
    print("news_cnt : " + str(news_cnt))