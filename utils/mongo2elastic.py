import sys, os, operator, datetime
from time import gmtime, strftime, localtime
import jalali
from pymongo import MongoClient
from elasticsearch import Elasticsearch
es = Elasticsearch(timeout=100)

def index_creator(index_name):
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    conf = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": "0",
        },
        "mappings": {
            "doc": {
                "properties": {
                    "title": {"type": "text", "term_vector": "yes", "analyzer": "parsi"},
                    "abstract": {"type": "text", "term_vector": "yes", "analyzer": "parsi"},
                    "body": {"type": "text", "term_vector": "yes", "analyzer": "parsi"},
                    "view_count": {"type": "integer"},
                    "like_count": {"type": "integer"},
                    "comment_count": {"type": "integer"},
                    "link": {"type": "text", "type": "keyword"},
                    "publication_time": {"type": "date", "format": "HH:mm"}, #default: "strict_date_optional_time||epoch_millis"
                    "publication_date_shamsi": {"type": "text", "type": "keyword"},
                    "publication_date_gregorian": {"type": "date", "format": "yyyy-MM-dd"},
                    "crawl_datetime_gregorian": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                }
            }
        }
    }

    es.indices.create(index=index_name, body=conf)
    print("Index Created")
def add_docs(index_name, docs):
    cnt = 0
    for doc in docs:
        cnt += 1
        res = es.index(index=index_name, doc_type='doc', body=doc, id = cnt)
    # print("Index '" + str(index_name) + "' status = " + res['result'])

def _test():
    index_name = "test_structure"
    index_creator(index_name)
    doc1 = {
        'title': ' 540 دستگاه دیالیز تولید داخل توزیع می شود',
        'abstract': 'تهران- ایرنا- رئیس مرکز مدیریت پیوند و بیماریهای وزارت بهداشت، درمان و آموزش پزشکی گفت: کشورمان در زمینه تولید دستگاه دیالیز داخلی به خودکفایی رسیده و در 6 ماه آینده 540 دستگاه دیالیز در بیمارستانها توزیع می شود.',
        'body': 'به گزارش خبرنگار حوزه سلامت ایرنا، مهدی شادنوش روز یکشنبه در آستانه هفته حمایت از بیماران کلیوی (23 تا 30 آبان) در یک نشست خبری در محل انجمن حمایت از بیماران کلیوی افزود: در راستای اقتصاد مقاومتی به همت یکی از شرکت های دانش بنیان، دستگاه دیالیز داخلی تولید و مراحل آزمایش آن طی 2 سال انجام شده و بر اساس قراردادی که وزارت بهداشت با این شرکت منعقد شده اکنون هزار دستگاه دیالیز در کشور در حال استفاده است و در 6 ماه آینده 540 دستگاه نیز در بیمارستانها توزیع می شود.\nوی اظهار داشت: تخت های دیالیزی در سال 92، 475 تخت بود که اکنون به 6 هزار و 987 بخش رسیده است.\nشادنوش ادامه داد: در حال حاضر 32 هزار و 169 بیمار دیالیز خونی و هزار و 744 نفر دیالیزی صفاقی می شوند و قرار است طی برنامه ریزی های وزارت بهداشت و فرهنگ سازی در کشور، طی سه سال آینده تعداد بیماران دیالیز صفاقی به 6 هزار بیمار افزایش یابد.\nوی با بیان اینکه به ازای هر 6 بیمار چهار دستگاه دیالیز در کشور وجود دارد، از توزیع نامناسب این دستگاهها انتقاد کرد و گفت: باید تناسب توزیع دستگاههای دیالیز در استانها و شهرها مناسب باشد و قرار است طی سه سال آینده سه هزار دستگاه در ناوگان دیالیز اضافه شود.\nشادنوش تصریح کرد: سالانه 16 هزار مرگ بر اثر حادثه در کشور رخ می دهد که از این تعداد هشت هزار نفر مرگ مغزی می شوند و 2500 تا چهار هزار نفر نیز امکان اهدای عضو دارند.\n\n* 25 هزار نفر در لیست پیوند کلیه\nوی، شمار لیست انتظار برای پیوند کلیه در کشور را 25 هزار نفر دانست و افزود: روزانه هفت تا 10 نفر به دلیل نبود پیوند کلیه در لیست انتظار فوت می شوند.\nشادنوش با اشاره به اینکه در سال 96، 926 عضو اهدا شده است، تصریح کرد: باید در این باره فرهنگ سازی انجام شود به طوری که در کشورهای مختلف زمان رضایت دادن اولیای دم برای اهدای عضو از فرد مرگ مغزی سه تا پنج دقیقه است ولی در ایران این زمان 90 ساعت است.\nاین مقام مسئول در وزارت بهداشت، درمان و آموزش پزشکی خاطرنشان کرد: فرد دچار مرگ مغزی امکان بازگشت به زندگی دوباره را ندارد و باید مردم ما را برای نجات بیماران و ارتقای کیفیت زندگی آنها یاری کنند تا از درد و رنج آنان کاسته شود.\nشادنوش اظهار داشت: بیماران خاص مورد توجه وزیر بهداشت،درمان و آموزش پزشکی هستند و قرار است 83 مرکز خدمات به بیماران خاص در 52 دانشگاه علوم پزشکی افتتاح شود و انجمن حمایت از بیماران کلیوی وزارت بهداشت را در این زمینه همراهی می کند.\nوی ادامه داد: در حال حاضر 20 درصد تخت های دیالیز توسط بخش های خصوصی و خیریه به بیمارا خدمات ارائه می شود و سازمان های بیمه گر نیز 46درصد هزینه ملزومات دیالیز را در پوشش بیمه ای قرار دادند.\nشانوش تاکید کرد: هماهنگی و همراهی بین حوزه های مختلف در حال انجام است تا بیماران خاص درد کمتری را تحمل کنند.\n\n*دیابت اپیدمی می شود\nوی در ادامه این نشست به هفته ملی دیابت (19 تا24 آبان ) اشاره کرد و گفت: دیابت و فشار خون از عوامل اصلی نارسایی کلیه هستند و تعداد بیماران مبتلا به دیابت رو به افزایش است.\nشادنوش افزود: اکنون 429 میلیون نفر در گروه سنی 20 تا 79 سال در جهان دیابت دارند که تا سال 2045 به 619 میلیون نفر می رسد.\nوی اظهار داشت: سالانه چهار میلیون نفر در جهان جان خود را به دلیل دیابت از دست می دهند و 12 درصد جمعیت بالای 25 سال در کشور مبتلا به این بیماری هستند.\nشادنوش تصریح کرد: همچنین 25 درصد از این جمعیت مبتلا از بیماری خود آگاهی ندارند. مردم باید به تغذیه درست و داشتن تحرک توجه اساسی کنند و آموزش در مورد دیابت و عوارض آن برای خانواده ها ضروری است.\nوی خاطرنشن کرد: خانواده ها می توانند برای انجام تست فشارخون و قندخون در این هفته به خانه های بهداشت و مراکز جامع سلامت مراجعه کنند.',
        'view_count': '0',
        'like_count': '0',
        'comment_count': '0',
        'link': 'http://www.irna.ir/fa/News/83095371',
        'publication_time': '13:26',
        'publication_date_shamsi': '1397/08/20',
        'publication_date_gregorian': jalali.Persian('1397/08/20').gregorian_string(),
        'crawl_datetime_gregorian': str(strftime("%Y-%m-%d %H:%M:%S", localtime())),

    }
    res = es.index(index=index_name, doc_type='doc', body=doc1, id=1)

def get_json(document):
    i = 0
    #bayad ye sakhtar koli dar biaram ke hjson ro chek kone va bebine agar field haye morede
    # niaz ro dasht fill kone, agar nadasht hame bejashun meghdar pishfarz bezare
    # dar nahayad yejson object ke kamelan sazegar bashe ba elastic tolid kone
    # in marhale ro daghigh anjam bedam rooye collection ha o site ha mokhtalef kar kone
    #nokte mohem ine ke bayad az asami vahed baraye field hashun dar mongo estefade kardee bashan
    # faghat mitonnan baziashun hala nadashte bashan un field haro k ma pish farz bezarim
    # na inke dar har collection field haye khase collection ba esme khas tarif shode bashan ! kar sakht mishe untro!

irna_index_name = "irna"
others_index_names = []
'''
    steps:
        1- itteration on all collection in db
        2- creat index for each collection add 
        3- add collection name to others_index_names
        4- index all docs of collection in it's index_name
'''
client = MongoClient("localhost", 27017)
db = client['news_sites']
collection_names = db.collection_names()
if irna_index_name in collection_names: collection_names.remove(irna_index_name)

for collection_name in collection_names:
    index_creator(collection_name)
    collection  = db[collection_name]
    for document in collection.find():
        print(document)  # iterate the cursor
        doc_json = get_json(document)
        add_docs(collection_name, doc_json)
        sys.exit(1)
    #create index with collection name
    #itteration on docs of collection and insert them in their index
    i = 0

print(collection_names)

# d = dict((db, [collection for collection in client[db].collection_names()])
#          for db in client.database_names())