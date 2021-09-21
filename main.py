import requests, time, sys, dateparser, re, sqlite3, threading, queue
from bs4 import BeautifulSoup

locker = threading.Lock()
def sprint(*a, **b):
    with locker:
        print(*a, **b)

class Parser:
    def __init__(self):
        self._console = True
        self.db_name = 'datebase.db'
        self.res = self.db_get()
        self.link_title = {}
        self.link_date = {}
        self.max_thread = 15


    def db_insert(self, res_id, link, title, content, nd_date, s_date, not_date):
        if self._console:
            sprint("[" + self.db_name + "] " + "Запись данных. ЗАГОЛОВОК: " + title)
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute(f"""INSERT INTO items(res_id, link, title, content, nd_date, s_date, not_date) VALUES({res_id}, '{link}', '{title}', '{content}', {nd_date}, {s_date}, '{not_date}');""")
        conn.commit()

    def db_get(self):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("""SELECT * FROM resource""")
        return cur.fetchall()

    def get_link(self, link, url):
        l = link.get('href')
        if link is None:
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
        d = dateparser.parse(date.text.strip().replace(",", ""))
        if d is None:
            d = dateparser.parse(str(date.get('datetime')))
            if d is None:
                for child in date.recursiveChildGenerator():
                    if child.name:
                        d = dateparser.parse(child.text.strip().replace(",", ""))
                        if d is None:
                            d = dateparser.parse(str(child.get('datetime')))
                        if d:
                            break
        return d

    def get_content(self, tag, url):
        if self._console:
            sprint("Получение контента: " + url)
        fin_content = ""
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')
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

    def compare(self, _id, titles, dates, q, url):
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
                        self.link_date[url][link] = dates[i]
                        self.link_title[url][link] = titles[i]
                        break
            q.task_done()

    def final(self,bottom_tag, RESOURCE_ID, q, RESOURCE_URL):
        link = q.get()
        try:
            title = self.link_title[RESOURCE_URL][link]
            d = self.link_date[RESOURCE_URL][link]
            s_date = str(time.time())
            not_date = str(dateparser.parse(str(d), date_formats=['%Y %B %d']))
            t = (d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond, 0, 0)
            nd_date = str(time.mktime(t))
            content = self.get_content(bottom_tag, link)
            self.db_insert(RESOURCE_ID, link, title, content, nd_date, s_date, not_date)
        except:
            pass
        q.task_done()

    def process(self, i):
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
        self.link_date[RESOURCE_URL]= {}

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
            t = threading.Thread(target=self.compare, args=(i_compare, fin_titles, fin_dates, links_queue, RESOURCE_URL,))
            t.setDaemon(True)
            t.start()
        links_queue.join()

        if self._console:
            sprint("РЕСУРС: " + RESOURCE_URL + " найдено " + str(len(self.link_title[RESOURCE_URL].items())+1) + " новостей.")

        links_queue = queue.Queue()
        for link in fin_links:
            links_queue.put(link)
        for i_final in range(len(fin_links)):
            t = threading.Thread(target=self.final, args=(bottom_tag, RESOURCE_ID, links_queue, RESOURCE_URL,))
            t.setDaemon(True)
            t.start()
        links_queue.join()

    def start(self):
        for i in range(len(self.res)):
            t = threading.Thread(target=self.process(i))
            t.setDaemon(True)
            t.start()

if __name__ == '__main__':
    p = Parser()
    p.start()
