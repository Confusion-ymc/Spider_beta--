#!coding:utf-8
import requests
import MySQLdb
import time
import re
from lxml import etree
from gevent.pool import Pool
from gevent import monkey

monkey.patch_all(socket=True, select=True)

t = 0
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
}

proxies = {
    'http': 'http://118.114.77.47:8080'
}


def get_proxies():
    while True:
        url = 'http://www.xicidaili.com/'
        data = requests.get(url=url, headers=headers).text
        tree = etree.HTML(data)
        result = tree.xpath('//*[@id="ip_list"]/tr')
        https = {}
        for i in range(len(result)):
            if i < 2:
                continue
            try:
                if result[i].xpath('td[6]')[0].text == "HTTP":
                    http = {
                        'http': 'http://' + result[i].xpath('td[2]')[0].text + ":" + result[i].xpath('td[3]')[0].text}
                    print u'测试代理... \n%s' % http['http'].lstrip('http://')
                    yield http
                else:
                    https[i] = 'https,' + 'http://' + result[i].xpath('td[2]')[0].text + ":" + result[i].xpath('td[3]')[
                        0].text
            except Exception as e:
                print e
                continue


def change_proxies(url):
    global proxies
    print '\n尝试3次失败, 更换代理中...'
    for i in http_proxy:
        proxies = i
        try:
            res = requests.get(url, proxies=proxies, headers=headers, timeout=5).text
            title = u'中国农药信息网'
            f = re.compile(title)
            m = f.findall(res)
            if m == []:
                print '尝试下一个代理...'
                continue
            else:
                print '切换代理成功! 当前代理为%s' % proxies['http'].lstrip('http://')
                break
        except Exception as e:
            # print e
            print '尝试下一个代理...'
    return 1


def creatTable():
    global table1, table2, table3
    date = time.strftime('%Y%m%d', time.localtime(time.time()))
    table1 = str(date) + '_product_1'
    table2 = str(date) + '_product_2'
    table3 = str(date) + '_product_3'
    d = MySQLdb.connect(db='reptile', user='root', passwd='4399', host='127.0.0.1', charset='utf8')
    cur = d.cursor()
    sql1 = 'CREATE TABLE `%s` (' \
           '`PDnumber` varchar(255) NOT NULL,' \
           '`StartTime` varchar(255) DEFAULT NULL,' \
           '`EndTime` varchar(255) DEFAULT NULL,' \
           '`Name` varchar(255) DEFAULT NULL,' \
           '`Dx` varchar(255) DEFAULT NULL,' \
           '`Jx` varchar(255) DEFAULT NULL,' \
           '`Supplier` varchar(255) DEFAULT NULL,' \
           '`Country` varchar(255) DEFAULT NULL,' \
           '`Tcontent` varchar(255) DEFAULT NULL,' \
           '`Remark` varchar(255) DEFAULT NULL,' \
           '`detail_html` longtext DEFAULT NULL,' \
           'PRIMARY KEY (`PDnumber`)' \
           ')ENGINE=InnoDB DEFAULT CHARSET=utf8;' % table1
    sql2 = 'CREATE TABLE `%s` (' \
           '`PDnumber` varchar(255) NOT NULL,' \
           '`Active` varchar(255) DEFAULT NULL,' \
           '`Aic` varchar(255) DEFAULT NULL' \
           ') ENGINE=InnoDB DEFAULT CHARSET=utf8;' % table2
    sql3 = 'CREATE TABLE `%s` (' \
           '`PDnumber` varchar(255) NOT NULL,' \
           '`Crop` varchar(255) DEFAULT NULL,' \
           '`Control` varchar(255) DEFAULT NULL,' \
           '`Dosage` varchar(255) DEFAULT NULL,' \
           '`Way` varchar(255) DEFAULT NULL' \
           ') ENGINE=InnoDB DEFAULT CHARSET=utf8;' % table3
    sql4 = ['DROP TABLE IF EXISTS `%s`;' % i for i in [table1, table2, table3]]
    for i in range(len(sql4)):
        try:
            cur.execute(sql4[i])
            d.commit()
            # print '清理表%d成功!' % eval('table' + str(i+1))
        except Exception as e:
            print e
            print i
            d.rollback()
    print ''
    sql = [sql1, sql2, sql3]
    for i in range(len(sql)):
        try:
            cur.execute(sql[i])
            d.commit()
            print '创建表%s成功!' % eval('table' + str(i + 1))
        except Exception as e:
            print e
            print i
            d.rollback()
    cur.close()
    d.close()


def saveDB(table, data):
    d = MySQLdb.connect(db='reptile', user='root', passwd='4399', host='127.0.0.1', charset='utf8')
    cur = d.cursor()
    flag_db = True
    for i in data:
        info = "\',\'".join(i)
        sql = "insert into %s VALUES('%s')" % (table, info)
        # print sql
        try:
            cur.execute(sql)
            d.commit()
            # print '%s 写入成功!' % table
        except Exception as e:
            print e
            # print sql
            d.rollback()
            flag_db = False
    cur.close()
    d.close()
    return flag_db


def get_urls():
    global t
    pageNo = 1
    keynum = 0
    while True:
        # keywords = ['PD']
        data = {
            'pageNo': '%d' % pageNo,
            'pageSize': '%d' % pageSize,
            'djzh': '%s' % keywords[keynum],
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
        try_time = 0
        while True:
            try:
                res = requests.post(url=start_url, data=data, proxies=proxies, headers=headers).text
                title = u'中国农药信息网'
                f = re.compile(title)
                m = f.findall(res)
                print u'%s访问成功!' % m[0]
                break
            except Exception as e:
                try_time += 1
                print '主网页访问失败,重试...'
                if try_time >= 3:
                    change_proxies(start_url)

        # print res
        # m = "t3.*?\'(.*?)\',\'"
        m = "javascript:open\(\'(.*?)\',\'"
        url_key = re.findall(m, res)
        # print url_key
        m = u"共 (\d+) 条"
        sum = re.findall(m, res)
        sum = int(sum[0])
        print '当前的关键词为%s, 共%d条!' % (keywords[keynum], sum)
        print '请求第%d页!' % pageNo
        real_url = 'http://www.chinapesticide.gov.cn/myquery/querydetail?pdno='
        url_list = [real_url + x for x in url_key]
        yield url_list
        pageNo += 1
        if sum == t:
            keynum += 1
            t = 0
            pageNo = 1
            if keynum == len(keywords):
                break


def parse(url):
    global t, url_list2
    try_time = 0
    while True:
        try:
            res = requests.post(url=url, proxies=proxies, headers=headers).text
            tree = etree.HTML(res)
            title = ''.join(tree.xpath('/html/body/div[1]/h1/text()'))
            if title != u'农药登记数据':
                print '返回错误网页!'
                continue
            break
        except Exception as e:
            try_time += 1
            if try_time >= 3:
                return 0
            # print e
            print '子网页访问失败,重试...'
    PDnumber = ''.join(tree.xpath('//*[@id="reg"][1]/tr[2]/td[2]/text()')).strip()
    if PDnumber == '':
        PDnumber = ''.join(tree.xpath('//*[@id="reg"][1]/tr[2]/td[2]/a/text()')).strip()
    StartTime = ''.join(tree.xpath('//*[@id="reg"][1]/tr[2]/td[4]/text()')).strip()
    EndTime = ''.join(tree.xpath('//*[@id="reg"][1]/tr[2]/td[6]/text()')).strip()
    Name = ''.join(tree.xpath('//*[@id="reg"][1]/tr[3]/td[2]/text()')).strip()
    Dx = ''.join(tree.xpath('//*[@id="reg"][1]/tr[3]/td[4]/text()')).strip()
    Jx = ''.join(tree.xpath('//*[@id="reg"][1]/tr[3]/td[6]/text()')).strip()
    Supplier = ''.join(tree.xpath('//*[@id="reg"][1]/tr[4]/td[2]/a/text()')).strip()
    if Supplier == '':
        Supplier = ''.join(tree.xpath('//*[@id="reg"][1]/tr[4]/td[2]/text()')).strip()
    Country = ''.join(tree.xpath('//*[@id="reg"][1]/tr[4]/td[4]/text()')).strip()
    Tcontent = ''.join(tree.xpath('//*[@id="reg"][1]/tr[5]/td[2]/text()')).strip()
    Remark = ''.join(tree.xpath('//*[@id="reg"][1]/tr[6]/td[2]/text()')).strip()
    detail_url = 'http://www.chinapesticide.gov.cn/myquery/tagdetail?pdno=%s' % PDnumber
    try:
        detail_html = requests.get(url=detail_url, proxies=proxies, headers=headers).text
        detail_html = detail_html.replace('\'', '\\\'')
    except Exception as e:
        print e
        detail_html = u'--'

    # 获取表1信息
    list1 = [[PDnumber, StartTime, EndTime, Name, Dx, Jx, Supplier, Country, Tcontent, Remark, detail_html]]
    for i in range(len(list1[0])):
        if list1[0][i] == '':
            list1[0][i] = 'null'
    # print list1
    if not saveDB(table1, list1):
        return 0
    # 获取表2信息
    Table2 = tree.xpath('//*[@id="reg"][3]/tr')
    if len(Table2) > 2:
        list2 = []
        for i in range(len(Table2)):
            if i > 1:
                Active = ''.join(Table2[i].xpath('td[1]/text()')).strip()
                Aic = ''.join(Table2[i].xpath('td[2]/text()')).strip()
                list2.append([PDnumber, Active, Aic])
    else:
        list2 = [[PDnumber, 'null', 'null']]
    # print list2
    if not saveDB(table2, list2):
        return 0
    # 获取表3信息
    Table3 = tree.xpath('//*[@id="reg"][4]/tr')
    if len(Table3) > 2:
        list3 = []
        for i in range(len(Table3)):
            if i > 1:
                Crop = ''.join(Table3[i].xpath('td[1]/text()')).strip()
                Control = ''.join(Table3[i].xpath('td[2]/text()')).strip()
                Dosage = ''.join(Table3[i].xpath('td[3]/text()')).strip()
                Way = ''.join(Table3[i].xpath('td[4]/text()')).strip()
                list3.append([PDnumber, Crop, Control, Dosage, Way])
    else:
        list3 = [[PDnumber, 'null', 'null', 'null', 'null']]
    # print list3
    saveDB(table3, list3)
    # print url
    t += 1
    print t
    url_list2.remove(url)


http_proxy = get_proxies()  # 代理生成器
start_url = 'http://www.chinapesticide.gov.cn/myquery/queryselect'
urls = get_urls()  # 每个信息的url生成器
url_list = []
pageSize = 500
keywords = ["WL", "WP", "LS", "PD"]
creatTable()  # 按日期创建数据表
for url_list2 in urls:
    p = Pool(pageSize)
    p.map(parse, url_list2)
    while url_list2:
        print url_list2
        change_proxies(start_url)
        p = Pool(pageSize)
        p.map(parse, url_list2)
