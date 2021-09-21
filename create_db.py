import sqlite3

def d1():
    conn = sqlite3.connect('datebase.db')
    cur = conn.cursor()
    cur.execute("CREATE TABLE resource (RESOURCE_ID INTEGER PRIMARY KEY, RESOURCE_NAME varchar(255), RESOURCE_URL varchar(255), top_tag varchar(255), bottom_tag varchar(255), title_cut varchar(255), date_cut varchar(255));")
    conn.commit()

    cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, res_id int, link varchar(255), title varchar(255), content text, nd_date timestamp, s_date timestamp, not_date DATETIME);")

    conn.commit()
    cur.execute("""INSERT INTO resource(RESOURCE_ID, RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut) 
       VALUES(0, 'nur.kz', 'https://www.nur.kz', 'class_=article-preview-mixed article-preview-mixed--secondary article-preview-mixed--with-absolute-secondary-item', 'class_=formatted-body io-article-body', 'ul/li/article/a/div/h3', 'class_=preview-info-item-secondary');""")
    conn.commit()
    cur.execute("""INSERT INTO resource(RESOURCE_ID, RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut) 
       VALUES(1, 'kaztag.info', 'https://kaztag.info', 'ul/li/div/h2/a', 'class_=content', 'ul/li/div/h2/a', 'class_=t-info-2');""")
    conn.commit()

if __name__ == '__main__':
    d1()
