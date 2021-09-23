import requests, time, sys, dateparser, re, sqlite3, threading, queue
from bs4 import BeautifulSoup

locker = threading.Lock()


def sprint(*a, **b):
    with locker:
        print(*a, **b)


class Parser:
    def __init__(self):
        self._console = True
        self.db_name = 'database.db'
        self.res = self.db_get()
        self.link_title = {}
        self.link_date = {}
        self.link_contents = {}
        self.max_thread = 15

    def db_insert(self, res_id, link, title, content, nd_date, s_date, not_date):
        if self._console:
            sprint("[" + self.db_name + "] " + "Запись данных. ЗАГОЛОВОК: " + title)
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute(
            f"""INSERT INTO items(res_id, link, title, content, nd_date, s_date, not_date) VALUES({res_id}, '{link}', 
            '{title}', '{content}', {nd_date}, {s_date}, '{not_date}');""")
        conn.commit()

    def db_get(self):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("""SELECT * FROM resource""")
        return cur.fetchall()

    def get_link(self, link, url):
        l = link.get('href')
        if l is None:
            for child in link.recursiveChildGenerator():
                if child.name:
                    l = child.get('href')
                    if l:
                        break
        if l.find("http") == -1:
            l = url + l
        return l

    def get_title(self, title):
        t = title.text.strip()
        if t is None:
            for child in title.recursiveChildGenerator():
                if child.name:
                    t = title.text.strip()
                    if t:
                        break
        return t

    def get_date(self, date):
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

    def get_content(self, tag, dates, link, soup, RESOURCE_URL):
        if self._console:
            sprint("Получение контента: " + link)
        fin_content = ""
        contents = self.finder(tag, soup)
        url_pattern = r'https://[\S]+'
        for content in contents:
            fin_content = content.text.strip()
            if fin_content is None:
                for child in content.recursiveChildGenerator():
                    if child.name:
                        fin_content = content.text.strip()
                        if fin_content:
                            break

        _dates = self.finder('time', soup)
        d_flag = False
        for _date in _dates:
            for date in dates:
                if self.get_date(_date) == date:
                    d_flag = True
                    self.link_date[RESOURCE_URL][link] = date
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
            for date in dates:
                try:
                    s_date = date.strftime("%H:%M")
                    if _date == s_date:
                        d_flag = True
                        self.link_date[RESOURCE_URL][link] = date
                        break
                except:
                    continue
            if d_flag:
                break

        try:
            urls = re.findall(url_pattern, fin_content)
            for u in urls:
                fin_content = fin_content.replace(u, "")
        except:
            return fin_content
        return fin_content

    def finder(self, el, soup):
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
                childs = [ee for ee in _e.recursiveChildGenerator() if ee.name is not None]
                if len(childs) > 0:
                    for child in childs:
                        if child.name == tags[count]:
                            if count == len(tags) - 1:
                                _data.append(child)
                                continue
                            count += 1
        else:
            _data += soup.find_all(el)
        return _data

    def compare(self, _id, titles, q, RESOURCE_URL, bottom_tag, fin_dates):
        while True:
            link = q.get()
            if self._console:
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
                        content = self.get_content(bottom_tag, fin_dates, link, soup, RESOURCE_URL)
                        self.link_title[RESOURCE_URL][link] = titles[i]
                        self.link_contents[RESOURCE_URL][link] = content
                        break
            q.task_done()

    def process(self, q):
        i = q.get()
        RESOURCE_ID = self.res[i][0]
        RESOURCE_NAME = self.res[i][1]
        RESOURCE_URL = self.res[i][2]
        top_tag = self.res[i][3]
        bottom_tag = self.res[i][4]
        title_cut = self.res[i][5]
        date_cut = self.res[i][6]

        fin_links = []
        fin_titles = []
        fin_dates = []

        self.link_title[RESOURCE_URL] = {}
        self.link_date[RESOURCE_URL] = {}
        self.link_contents[RESOURCE_URL] = {}

        try:
            response = requests.get(RESOURCE_URL)
        except BaseException as e:
            print(e)
            sys.exit()
        soup = BeautifulSoup(response.text, 'lxml')

        links = self.finder(top_tag, soup)
        titles = self.finder(title_cut, soup)
        dates = self.finder(date_cut, soup)

        for fin_i in range(len(links)):
            link = self.get_link(links[fin_i], RESOURCE_URL)
            if link != "":
                fin_links.append(link)
            try:
                title = self.get_title(titles[fin_i])
                if title != "":
                    fin_titles.append(title)
            except:
                pass
            try:
                date = self.get_date(dates[fin_i])
                if date != "":
                    fin_dates.append(date)
            except:
                pass

        fin_links = list(set(fin_links))
        fin_titles = list(set(fin_titles))

        links_queue = queue.Queue()
        for link in fin_links:
            links_queue.put(link)
        for i_compare in range(self.max_thread):
            t = threading.Thread(target=self.compare,
                                 args=(i_compare, fin_titles, links_queue, RESOURCE_URL, bottom_tag, fin_dates))
            t.setDaemon(True)
            t.start()
        links_queue.join()

        if self._console:
            sprint("РЕСУРС: " + RESOURCE_URL + " найдено " + str(
                len(self.link_title[RESOURCE_URL].items()) + 1) + " новостей.")

        for link in fin_links:
            try:
                title = self.link_title[RESOURCE_URL][link]
                content = self.link_contents[RESOURCE_URL][link]
                d = self.link_date[RESOURCE_URL][link]
                s_date = str(time.time())
                not_date = str(dateparser.parse(str(d), date_formats=['%Y %B %d']))
                t = (d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond, 0, 0)
                nd_date = str(time.mktime(t))
                self.db_insert(RESOURCE_ID, link, title, content, nd_date, s_date, not_date)
            except:
                pass
        q.task_done()

    def start(self):
        i_queue = queue.Queue()
        for i in range(len(self.res)):
            i_queue.put(i)
        ran = min(self.max_thread, len(self.res))
        for i in range(ran):
            t = threading.Thread(target=self.process, args=(i_queue,))
            t.setDaemon(True)
            t.start()
        i_queue.join()


if __name__ == '__main__':
    p = Parser()
    p.start()
