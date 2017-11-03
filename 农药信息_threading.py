#!coding:utf-8
import urllib
import urllib2
import re
import MySQLdb
import string
from bs4 import BeautifulSoup
from threading import Thread
from threading import Lock
from lxml import etree


def getUrl(page, search, per):
    url = 'http://www.chinapesticide.gov.cn/myquery/queryselect'  # 搜索链接
    user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
    values = {
        'pageNo': '%s' % page,
        'pageSize': '%s' % per,
        'djzh': '%s' % search,
        'cjmc': '',
        'sf': '',
        'nylb': '',
        'zhl': '',
        'jx': '',
        'zwmc': '',
        'fzdx': '',
        'syff': '',
        'dx': '',
        'yxcf': '',
        'yxcf_en': '',
        'yxcfhl': '',
        'yxcf2': '',
        'yxcf2_en': '',
        'yxcf2hl': '',
        'yxcf3': '',
        'yxcf3_en': '',
        'yxcf3hl': '',
        'yxqs_start': '',
        'yxqs_end': '',
        'yxjz_start': '',
        'yxjz_end': ''
    }
    data = urllib.urlencode(values)
    retime = 0
    while True:
        try:
            req = urllib2.Request(url, data)
            response = urllib2.urlopen(req)
            select_page = response.read()
            break
        except:
            retime = retime + 1
            print "重试" + str(re) + " " + url
    m = "t3.*?\'(.*?)\',\'%s" % search
    url = re.findall(m, select_page)
    html = etree.HTML(select_page)
    temp = html.xpath("/html/body/div/div/ul/li/a/text()[3]")
    sum = "".join(temp)
    m = u"共 (\d+) 条"
    # /html/body/div/div/ul/li[5]/a/text()[3]
    sum = re.findall(m, sum)
    sum = int(sum[0])
    print sum
    if url == []:
        urllist = []
        return urllist
    urllist = []
    for i in url:
        urllist.append('http://www.chinapesticide.gov.cn/myquery/querydetail?pdno=' + i)

    # return getHtml(urllist[0])#只取第一条链接
    return urllist, sum


def getInfo(id, url):
    id = str(id)
    re = 0
    while True:
        try:
            req = urllib2.Request(url)
            data = urllib2.urlopen(req)
            html = data.read()
            soup = BeautifulSoup(html, 'lxml', from_encoding="utf8")
            break
        except:
            re = re + 1
            print "重试" + str(re) + " " + url
    # print soup
    allList = []
    # 基本信息
    f = soup.find(class_="tab_lef_bot")
    pd = f.text.strip()
    if pd == '':
        return url
    # print pd

    startTime = f.find_next(class_="tab_lef_bot").text
    # print startTime

    endTime = f.find_next(class_="tab_lef_bot_rig").text
    # print endTime

    f = soup.find(text="登记名称：")
    name = f.find_next("td").text
    # print name

    dx = f.find_next(class_="tab_lef_bot").find_next(class_="tab_lef_bot").text
    # print dx

    type = f.find_next(class_="tab_lef_bot").find_next(class_="tab_lef_bot").find_next(class_="tab_lef_bot_rig").text
    # print type

    temp = soup.find(attrs={"colspan": "3"})
    enterPrise = temp.text.strip()
    # print enterPrise

    temp = temp.find_next(class_="tab_lef_bot_rig")
    country = temp.text
    # print country

    temp = soup.find(text="总含量：")
    main = temp.find_next(class_="tab_lef_bot_rig").text
    # print main

    temp = soup.find(text="备注：")
    remark = temp.find_next(class_="tab_lef_bot_rig").text
    if remark == "":
        remark = "Null"
    # print remark
    list1 = [id, pd, startTime, endTime, name, dx, type, enterPrise, country, main, remark]

    tagUrl = "http://www.chinapesticide.gov.cn" + soup.find(text="查看标签").find_parent().get("href")
    # print tagUrl

    # 有效成分信息

    act = [1, 2]
    if soup.find(text="有效成分").find_parent().find_parent().find_next("tr") == None:
        act[0] = "Null"
        act[1] = "Null"
        list2 = [[id, pd, act[0], act[1]]]
    else:
        act = soup.find(text="有效成分").find_parent().find_parent().find_parent().findAll("td")
        # print act
        for i in range(len(act)):
            act[i] = act[i].text
        list2 = []
        for i in range((len(act) - 3) / 2):
            list2.append([id, pd, act[3 + i * 2], act[4 + i * 2]])
            # print list2

    # 有效成分用药量信息
    crop = [1, 2, 3, 4]
    if soup.find(text="作物").find_parent().find_parent().find_next("tr") == None:
        crop[0] = "Null"
        crop[1] = "Null"
        crop[2] = "Null"
        crop[3] = "Null"
        list3 = [[id, pd, crop[0], crop[1], crop[2], crop[3]]]
    else:
        crop = soup.find(text="作物").find_parent().find_parent().find_parent().findAll("td")
        for i in range(len(crop)):
            crop[i] = crop[i].text
        list3 = []
        for i in range((len(crop) - 5) / 4):
            list3.append([id, pd, crop[5 + i * 4], crop[6 + i * 4], crop[7 + i * 4], crop[8 + i * 4]])
            # print list3

    list4 = [id, pd, tagUrl]
    allList = [list1, list2, list3, list4]
    # print allList
    return allList


# PD20080895

def writeDb(regul, list):
    listnew = "\',\'".join(list)
    d = MySQLdb.connect(db='reptile', user='root', passwd='4399', host='127.0.0.1', charset='utf8')
    cur = d.cursor()
    m = "insert into %s VALUES('%s')" % (regul, listnew)
    # print m

    try:
        cur.execute(m)
    except:
        print " 数据存在，已跳过,输入的数据为："
        print list
        print ''
        d.close()
        return 0

    d.commit()
    d.close()
    return 1


def writeInfo(allList):
    # 写入表一
    result = writeDb(regul='product_main_copy1(ID, PDnumber, startDate, overDate, name, virulence, genre, producter,' \
                           'country, total, ps)', list=allList[0])
    if result == 0:
        return 0
    # 写入表二
    # print allList[1]
    for i in allList[1]:
        writeDb(regul='product_2_copy1(ID, PDnumber, actmain, actper)', list=i)

    # 写入表三
    # print allList[2]
    for i in allList[2]:
        writeDb(regul='product_3_copy1(ID, PDnumber, crop, defense,drugper,way)', list=i)

    # 写入表四
    # print allList[3]
    writeDb(regul='product_4_copy1(ID, PDnumber, tagUrl)', list=allList[3])


class MyThread(Thread):
    def __init__(self, n, m):
        super(MyThread, self).__init__()
        self.n = n
        self.m = m

    def run(self):
        self.result = getInfo(self.n, self.m)

    def get_result(self):
        return self.result


# 已抓取 PD WP WL LS


tnum = 50  # 进程数
oldId = 0  # 已存在
page = 1
PDs = ["WL", "PD", "WP", "LS"]
per = 500

# stop = 33385+2447+37+965#+2447+37
stop = 0
n = 0
m = -1
T = []
lock = Lock()
id = oldId
for th in range(tnum):
    T.append('T' + str(th))
while True:
    if n == len(PDs):
        break;
    urllist, sum = getUrl(page, PDs[n], per)
    if m != n:
        stop = stop + sum
    m = n
    for url in urllist:
        id = id + 1
        if id <= oldId:
            print id
            continue
        T[(id + 9) % tnum] = MyThread(id, url)
        # allList = getInfo(id,url)
        T[(id + 9) % tnum].start()

        if id % tnum == 0 or id == stop:
            for i in range(tnum):
                if type(T[i]) == str:
                    continue
                T[i].join()
                lock.acquire()
                allList = T[i].get_result()
                if type(allList) == str:
                    pass
                    # 写log
                else:
                    print allList[0]
                    writeInfo(allList)
                lock.release()
            # t = 0
            if id == stop:
                n = n + 1
                print id
                break
    if id != stop:
        page = page + 1
