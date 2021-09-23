import queue
import re
import requests
import sqlite3
import sys
import threading
import time

import dateparser
from bs4 import BeautifulSoup

locker = threading.Lock()

_console = True
_max_threads = 15


def sprint(*a, **b):
    with locker:
        print(*a, **b)


def get_link(link, url):
    fin_link = link.get('href')
    if fin_link is None:
        for child in link.recursiveChildGenerator():
            if child.name:
                fin_link = child.get('href')
                if fin_link:
                    break
    if fin_link.find("http") == -1:
        fin_link = url + fin_link
    return fin_link


def get_title(title):
    t = title.text.strip()
    if t is None:
        for child in title.recursiveChildGenerator():
            if child.name:
                t = title.text.strip()
                if t:
                    break
    return t


def get_date(date):
    try:
        d = dateparser.parse(date.text.strip().replace(",", ""))
    except:
        d = None
    if d is None:
        try:
            d = dateparser.parse(str(date.get('datetime')))
        except:
            d = None
        if d is None:
            for child in date.recursiveChildGenerator():
                if child.name:
                    try:
                        d = dateparser.parse(child.text.strip().replace(",", ""))
                    except:
                        d = None
                    if d is None:
                        d = dateparser.parse(str(child.get('datetime')))
                    if d:
                        break
    return d


def get_content(_id, tag, link, soup):
    if _console:
        sprint("ПОТОК #" + str(_id) + ". Получение контента: " + link)
    fin_content = ""
    contents = finder(tag, soup)
    url_pattern = r'https://[\S]+'
    for content in contents:
        fin_content = content.text.strip()
        if fin_content is None:
            for child in content.recursiveChildGenerator():
                if child.name:
                    fin_content = content.text.strip()
                    if fin_content:
                        break
    try:
        urls = re.findall(url_pattern, fin_content)
        for u in urls:
            fin_content = fin_content.replace(u, "")
    except:
        return fin_content
    return fin_content


def finder(el, soup):
    _data = []
    if el.find("class_=") != -1:
        el = el.replace("class_=", "")
        _data += soup.find_all(class_=el)
    elif el.find("name=") != -1:
        el = el.replace("name=", "")
        _data += soup.find_all(name=el)
    elif el.find("/") != -1:
        tags = el.split("/")
        first_t = soup.find_all(tags[0])
        for _e in first_t:
            count = 1
            children = [ee for ee in _e.recursiveChildGenerator() if ee.name is not None]
            if len(children) > 0:
                for child in children:
                    if child.name == tags[count]:
                        if count == len(tags) - 1:
                            _data.append(child)
                            continue
                        count += 1
    else:
        _data += soup.find_all(el)
    return _data


class Parser:
    def __init__(self):
        self.db_name = 'database.db'
        self.res = self.db_get()
        self.link_title = {}
        self.link_date = {}
        self.link_contents = {}
        self.news_count = 0

    def db_insert(self, res_id, link, title, content, nd_date, s_date, not_date):
        if _console:
            sprint("[" + self.db_name + "] " + "Запись данных. ЗАГОЛОВОК: " + title)
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute(
            f"""INSERT INTO items(res_id, link, title, content, nd_date, s_date, not_date) VALUES({res_id}, '{link}', 
            '{title}', '{content}', {nd_date}, {s_date}, '{not_date}');""")
        conn.commit()
        self.news_count += 1

    def db_get(self):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("""SELECT * FROM resource""")
        data = cur.fetchall()
        return data

    def compare(self, i_url, _id, titles, q, fin_dates):
        _id += 1
        while True:
            link = q.get()
            if _console:
                sprint("ПОТОК #" + str(_id) + ". Исследуем ссылку: " + link)
            try:
                response = requests.get(link)
                soup = BeautifulSoup(response.text, 'lxml')
            except:
                continue
            for i in range(len(titles)):
                if len(soup.find_all(text=titles[i])) != 0:
                    elements = soup.find_all('a')
                    flag = False
                    for element in elements:
                        if element.text.strip() == titles[i]:
                            flag = True
                            break
                    if not flag:

                        fin_date = None
                        _dates = finder('time', soup)
                        d_flag = False
                        for _date in _dates:
                            for date in fin_dates:
                                if get_date(_date) == date:
                                    d_flag = True
                                    fin_date = date
                                    break
                            if d_flag:
                                break

                        if not d_flag:
                            _dates = []
                            body = soup.find_all('body')[0]
                            for child in body.recursiveChildGenerator():
                                try:
                                    date_pattern = r'\d\d:\d\d'
                                    d = re.findall(date_pattern, child.text)[0]
                                except:
                                    d = None
                                if not d is None:
                                    _dates.append(str(d))
                                if len(_dates) == 50:
                                    break
                        for _date in _dates:
                            for date in fin_dates:
                                try:
                                    s_date = date.strftime("%H:%M")
                                    if _date == s_date:
                                        fin_date = date
                                        d_flag = True
                                        break
                                except:
                                    continue
                            if d_flag:
                                break

                        if d_flag:
                            content = get_content(_id, self.res[i_url][4], link, soup)
                            self.link_title[self.res[i_url][2]][link] = titles[i]
                            self.link_date[self.res[i_url][2]][link] = fin_date
                            self.link_contents[self.res[i_url][2]][link] = content
                            break
            q.task_done()

    def process(self, i_url):
        resource_id = self.res[i_url][0]
        resource_name = self.res[i_url][1]
        resource_url = self.res[i_url][2]
        top_tag = self.res[i_url][3]
        bottom_tag = self.res[i_url][4]
        title_cut = self.res[i_url][5]
        date_cut = self.res[i_url][6]

        fin_links = []
        fin_titles = []
        fin_dates = []

        self.link_title[resource_url] = {}
        self.link_date[resource_url] = {}
        self.link_contents[resource_url] = {}

        try:
            response = requests.get(resource_url)
        except BaseException as e:
            if _console:
                sprint(e)
            sys.exit()
        soup = BeautifulSoup(response.text, 'lxml')

        links = finder(top_tag, soup)
        titles = finder(title_cut, soup)
        dates = finder(date_cut, soup)

        for fin_i in range(len(links)):
            link = get_link(links[fin_i], resource_url)
            if link != "":
                fin_links.append(link)
            try:
                title = get_title(titles[fin_i])
                if title != "":
                    fin_titles.append(title)
            except:
                pass
            try:
                date = get_date(dates[fin_i])
                if date != "":
                    fin_dates.append(date)
            except:
                pass

        fin_links = list(set(fin_links))
        fin_titles = list(set(fin_titles))

        links_queue = queue.Queue()
        for link in fin_links:
            links_queue.put(link)
        for i_compare in range(_max_threads):
            t = threading.Thread(target=self.compare,
                                 args=(i_url, i_compare, fin_titles, links_queue, fin_dates))
            t.setDaemon(True)
            t.start()
        links_queue.join()

        if _console:
            sprint("РЕСУРС: " + resource_url + " найдено " + str(
                len(self.link_title[resource_url].items()) + 1) + " новостей.")

        for link in fin_links:
            try:
                title = self.link_title[resource_url][link]
                content = self.link_contents[resource_url][link]
                d = self.link_date[resource_url][link]
                s_date = str(time.time())
                not_date = str(dateparser.parse(str(d), date_formats=['%Y %B %d']))
                t = (d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond, 0, 0)
                nd_date = str(time.mktime(t))
                self.db_insert(resource_id, link, title, content, nd_date, s_date, not_date)
            except:
                pass

    def start(self):
        for i in range(len(self.res)):
            self.process(i)
        sprint("\n[" + self.db_name + "] " + "Количество новых записей в базе: " + str(self.news_count))
        sprint("Парсинг окончен.")


if __name__ == '__main__':
    p = Parser()
    p.start()
