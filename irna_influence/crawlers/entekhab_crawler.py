import re
import sys
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from unidecode import unidecode

server_url = "http://www.entekhab.ir"

mongo_server = "localhost"
mongo_port = 27017
client = MongoClient(mongo_server, mongo_port)
db = client['news_sites']
news = db['entekhab']

def get_news_links(page_number):
    links_list = []
    base_url = server_url + "/fa/archive?service_id=0&sec_id=0&cat_id=0&rpp=10&from_date=1397/07/20&to_date=1397/08/05&p=1"
    latest_url = base_url + str(page_number)

    content = requests.get(latest_url).text
    soup = BeautifulSoup(content, "html.parser")

    archive_content_txt = ''.join([str(i) for i in soup.select("div.archive_content")])
    soup_archive = BeautifulSoup(archive_content_txt, "html.parser")

    tags = soup_archive.select('a.title5')
    for tag in tags:
        soup = BeautifulSoup(str(tag), 'html.parser')
        href = server_url + soup.a['href']
        href = str(re.findall(r'(htt.*\d+/)', href)[0])
        links_list.append(href)

    links_list = list(set(links_list))
    return links_list


pagination_num = 30

news_cnt = 0
for i in range(1, pagination_num):
    links = get_news_links(i)
    docs = []
    for link in links:
        print(link)
        news_cnt += 1
        content = requests.get(link).text
        soup = BeautifulSoup(content, "html.parser")
        body_raw_txt  = ''.join([str(i) for i in soup.select("div.khabar-matn")])
        soup_body = BeautifulSoup(str(re.sub('<br.*?>', '\n', body_raw_txt)),"html.parser")

        title = str(soup.h1.getText().strip())
        subtitle = None
        if len(soup.select('div.subtitle')) > 0:
            subtitle = str(soup.select('div.subtitle')[0].getText().strip())
        else:
            subtitle = ""

        body = str(soup_body.getText())
        body = str(re.sub('(var.*?error.*}\);)', '', body))

        time, date = soup.select('div.news_pdate_c')[0].getText().strip().replace("تاریخ انتشار:", "").split('-')
        date = str(date)
        time = str(time)

        doc = {
            "title": title,
            "abstract": subtitle,
            "body": body,
            "date_shamsi": date,
            "time": time,
            "link": str(link)
        }
        docs.append(doc)

    news.insert_many(docs)
    print("news_cnt : " + str(news_cnt))