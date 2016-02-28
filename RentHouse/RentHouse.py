__author__ = 'WJJ'
import requests
from bs4 import BeautifulSoup
import Queue
import threading
import torndb

class DB:
    def __init__(self):
        self.conn = torndb.Connection(
            "localhost",
            "rent_house",
            user="root",
            password="1234",
        )

    def save(self, obj):
        key_string = ""
        value_string = ""
        for key, value in vars(obj).items():
            key_string += key + ","
            if value is None:
                value = "null"
            value_string += "'" + value + "',"
        key_string = "(" + key_string.strip(",") + ")"
        value_string = "(" + value_string.strip(",") + ")"
        if isinstance(obj, House):
            sql = "insert into house" + key_string + " VALUES" + value_string
        # print sql
        # print value_string
        self.conn.execute(sql)


class RentHouse:
    def __init__(self, url):
        self.url = url

    def getPage(self):
        r = requests.get(self.url)
        r.encoding = "gbk"
        return r

    def getHouseItems(self):
        soup = BeautifulSoup(self.getPage().text, from_encoding="gbk")
        return soup.find_all("dl", class_="list hiddenMap rel")


class House:
    def __init__(self, pic_title, href, pic_preview, content_title, content_location, content_rent_type, content_area, content_extra, content_price, content_house_type):
        self.pic_title = pic_title
        self.href = href
        self.pic_preview = pic_preview
        self.content_title = content_title
        self.content_location = content_location
        self.content_rent_type = content_rent_type
        self.content_area = content_area
        # self.content_decoration = content_decoration
        # self.content_floor = content_floor
        # self.content_direction = content_direction
        self.content_extra = content_extra
        self.content_price = content_price
        self.content_house_type = content_house_type


class CrawlerThread(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            url = self.queue.get()
            print "Proccess: " + url
            house_items = RentHouse(url).getHouseItems()
            houses_list = []
            for item in house_items:
                pic_part = item.dt
                content_part = item.dd
                house = House(None, None, None, None, None, None, None, None, None, None)
                house.pic_title = pic_part.a["title"]
                house.href = pic_part.a["href"]
                try:
                    house.pic_preview = pic_part.a.img["src2"]
                except Exception, e:
                    house.pic_preview = "NULL"
                house.content_title = content_part.find("p", class_="title").a.string.strip()
                # if len(content_part.find("p", class_="gray6 mt12").findAll("a")) ==2:
                #     house.content_location = content_part.find("p", class_="gray6 mt12").findAll("a")[0].span.string + " " +\
                #                          content_part.find("p", class_="gray6 mt12").findAll("a")[1].span.string + " " +\
                #                         content_part.find("p", class_="gray6 mt12").findAll("span")[2].string
                # else:
                location_string = ""
                for i in range(len(content_part.find("p", class_="gray6 mt12").findAll("span"))):
                    location_string += content_part.find("p", class_="gray6 mt12").findAll("span")[i].string + "|"
                house.content_location = location_string
                extra_part = house.content_rent_type = content_part.find("p", class_="gray6 mt10").contents
                house.content_rent_type = extra_part[0].strip()
                house.content_area = extra_part[2].strip()
                extra_string = ""
                for i in range(3, len(extra_part)):
                    extra_string += extra_part[i].string.strip()
                house.content_extra = extra_string
                house.content_price = content_part.find("div", class_="moreInfo").find("p", class_="mt5 alignR").span.string.strip()
                house.content_house_type = content_part.find("div", class_="moreInfo").find("p", class_="alignR mt8").string.strip()
                houses_list.append(house)
            for house in houses_list:
                db = DB()
                db.save(house)

queue = Queue.Queue()
for i in range(1, 1000):
    page = 30 + i
    url = "http://zu.xian.fang.com/house/i" + str(page)
    queue.put(url)
for i in range(10):
    CrawlerThread(queue).start()


