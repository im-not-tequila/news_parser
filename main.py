import json, requests, time, sys
from bs4 import BeautifulSoup


class Parser:
    def __init__(self):
        self.res = self.resource_load()

    def resource_load(self):
        try:
            with open("resource.json", "r") as f:
                res = json.load(f)
                return res
        except BaseException as e:
            print(e)
            sys.exit()

    def items_dump(self, js):
        with open("items.json", "w", encoding="utf-8") as f:
            json.dump(js, f, ensure_ascii=False)

    def start(self):
        links = []
        contents = []
        titles= []
        dates = []
        id = 0
        data = {}
        for i in range(len(self.res)):
            res = self.res[str(i)]
            try:
                response = requests.get(res["RESOURCE_URL"])
            except BaseException as e:
                print(e)
                sys.exit()
            soup = BeautifulSoup(response.text, 'lxml')

            links.append(soup.find_all(class_=res["top_tag"]))
            contents.append(soup.find_all(class_=res["bottom_tag"]))
            titles.append(soup.find_all(class_=res["title_cut"]))
            dates.append(soup.find_all(class_=res["date_cut"]))

        for i in range(len(links)):
            for ii in range(len(links[i])):
                link = links[i][ii].get('href')
                if link is None:
                    link = self.res[str(i)]["RESOURCE_URL"] + links[i][ii].a.get('href')

                content = contents[i][ii].text.strip()

                title = titles[i][ii].text.strip()

                date = dates[i][ii].text.strip()
                date = date.replace("Сегодня, ", "")
                date = date.replace(", сегодня", "")
                date.split(":")

                s_date = time.time()
                d = time.localtime(s_date)
                t = (d.tm_year, d.tm_mon, d.tm_mday, int(date[0]), int(date[1]), 0, 0, 0, 0)
                nd_date = time.mktime(t)
                not_date = str(time.localtime(nd_date).tm_year) + "-" + str(time.localtime(nd_date).tm_mon) + "-" + str(time.localtime(nd_date).tm_mday)

                data.update({str(id): {"id": id, "res_id": self.res[str(i)]["RESOURCE_ID"], "link": link,
                             "title": title, "content": content, "nd_date": nd_date,
                             "s_date": s_date, "not_date": not_date}})

                id += 1
        self.items_dump(data)

if __name__ == '__main__':
    p = Parser()
    p.start()
