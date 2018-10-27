import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

server_url = "http://www.irna.ir"

mongo_server = "localhost"
mongo_port = 27017
client = MongoClient(mongo_server, mongo_port)
db = client['news_sites']
news = db['irna']

def get_news_links(page_number):
    links_list = []
    base_url = server_url + "/fa/page/260/ResultSearch?zone=27&lm=Latest&area=0&title=Euw%2bvo0paBHX2%2bYCUoyt9w%3d%3d&lang=fa&minify=t&"
    latest_url = base_url + str(page_number)

    content = requests.get(latest_url).text
    soup = BeautifulSoup(content, "html.parser")

    tags = soup.find_all(lambda tag: tag.name == 'a' and 'title' in tag.attrs)
    for tag in tags:
        soup = BeautifulSoup(str(tag), 'html.parser')
        href = server_url + soup.a['href']
        links_list.append(href)

    links_list = list(set(links_list))
    return links_list


BODY_ID = "ctl00_ctl00_ContentPlaceHolder_ContentPlaceHolder_NewsContent4_BodyLabel"
DATE_ID = "ctl00_ctl00_ContentPlaceHolder_ContentPlaceHolder_NewsContent4_NofaDateLabel2"
TIME_ID = "ctl00_ctl00_ContentPlaceHolder_ContentPlaceHolder_NewsContent4_NofaDateLabel3"

pagination_num = 16

news_cnt = 0
for i in range(1, pagination_num):
    links = get_news_links(i)
    docs = []
    for link in links:
        news_cnt += 1
        content = requests.get(link).text
        soup = BeautifulSoup(content, "html.parser")
        soup_body = BeautifulSoup(re.sub('<br.*?>', '\n', str(soup.findAll("p", {"id": BODY_ID})[0])), "html.parser")

        title = str(soup.h1.string)
        abstract = str(soup.h3.string)
        body = str(soup_body.p.text)
        date_shamsi = str(soup.find_all('span', {'id': DATE_ID})[0].text)
        time = str(soup.find_all('span', {'id': TIME_ID})[0].text)

        doc = {"title": title,
                "abstract": abstract,
                "body": body,
                "date_shamsi": date_shamsi,
                "time": time,
                "link": str(link)}

        docs.append(doc)
    news.insert_many(docs)
    print("news_cnt : " + str(news_cnt))