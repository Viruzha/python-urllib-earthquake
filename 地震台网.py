from lxml import etree
import MySQLdb
import urllib.request as req
from random import randint as randint
import json
import threading


def InsertValues(conn, id, LOCATION_C, EPI_LAT, EPI_LON, m, EPI_DEPTH, O_TIME):
    try:
        conn.execute(f'''INSERT INTO data VALUES('{id}','{LOCATION_C}','{EPI_LAT}','{EPI_LON}','{m}','{EPI_DEPTH}','{O_TIME}');
        commit;''')
        return True
    except Exception as e:
        print(e)
        return False


def GetData(html):
    UserAgent = [
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
        'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Mobile Safari/537.36',
        'Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1'
    ]
    r = req.Request(html, headers={'user-agent': UserAgent[randint(0, 3)]})
    d = req.urlopen(r, timeout=5)
    data = json.loads((d.read().decode('utf8'))[41:-1])
    return (data['shuju'], d.status)


threadLock = threading.Lock()


class threading_run(threading.Thread):
    def __init__(self, a, b, conn):
        threading.Thread.__init__(self)
        self.a = a
        self.b = b
        self.conn = conn

    def run(self):
        for i in range(self.a, self.b):
            m = 0
            html = f'http://www.ceic.ac.cn/ajax/search?page={i}&&start=2007-01-01&&end=2019-03-06&&jingdu1=&&jingdu2=&&weidu1=&&weidu2=&&height1=&&height2=&&zhenji1=&&zhenji2=&&callback=jQuery18007183313762912973_1551851155694&_=1551851208104'
            d = GetData(html)
            data = d[0]
            status_code = d[1]
            for i in data:
                id = i['id']
                LOCATION_C = i['LOCATION_C']  # 地点名
                EPI_DEPTH = i['EPI_DEPTH']  # 震源深度
                EPI_LAT = i['EPI_LAT']  # 维度
                EPI_LON = i['EPI_LON']  # 精读
                O_TIME = i['O_TIME']  # 发生时间
                m = i['M']  # 震级
                threadLock.acquire()  # 同步堵塞
                InsertValues(self.conn, id, LOCATION_C, EPI_LAT,
                             EPI_LON, m, EPI_DEPTH, O_TIME)
                if m == 0:
                    print('status_code:', status_code)
                    m = 1
                threadLock.release()


def main():
    CONN = MySQLdb.connect(host='localhost', port=3306,
                           user='root', passwd='1234', db='data', charset='utf8')
    conn = CONN.cursor()
    conn.execute('''DROP TABLE IF EXISTS data;
    CREATE TABLE data(id char(10),LOCATION_C char(50),EPI_LAT char(10),EPI_LON char(10),level FLOAT,EPI_DEPTH int(10),O_TIME datetime);
    commit;
    ''')
    threading_list = []

    ex1 = threading_run(1, 50, conn)
    ex2 = threading_run(50, 100, conn)
    ex3 = threading_run(100, 150, conn)
    ex4 = threading_run(150, 200, conn)
    ex5 = threading_run(200, 250, conn)
    ex6 = threading_run(250, 300, conn)
    ex7 = threading_run(300, 328, conn)

    ex1.start()
    ex2.start()
    ex3.start()
    ex4.start()
    ex5.start()
    ex6.start()
    ex7.start()

    threading_list.append(ex1)
    threading_list.append(ex2)
    threading_list.append(ex3)
    threading_list.append(ex4)
    threading_list.append(ex5)
    threading_list.append(ex6)
    threading_list.append(ex7)

    for i in threading_list:
        i.join()
    print('所有进程运行完毕')


main()
