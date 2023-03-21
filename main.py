# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time : 2023/03/07 14:07
# @Author : xuyu
# @Site :0
# @Describe:0
import re
import geoip2.database
import datetime
from sql3 import dbsql3
from config import datapath, tbname
import sqlite3
from flask import Flask, jsonify,render_template,request
import json
import os
import time
import hashlib
import logging

ipdbname = "ip地址数据库文件"
today = time.strftime("%Y-%m-%d")
# nginx日志路径
logpath = os.getcwd()
# logpath="./"
# logpath="C:/soft_online/phpstudy_pro/Extensions/Nginx1.15.11/logs"
# sqliet数据库名
dbname = "nginx_log.db"
# sqliet数据库路径
datapath = os.path.join(logpath, dbname)
# print(datapath)
# exit()
# sqliet数据表名
tbname = "logtable"
# 定义分析的域名日志
cnamlist = ["casino-raiders.com"]





class dbsql3():
    def __init__(self, dbpathname, tablename):
        self.path = dbpathname
        self.tablename = tablename
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("""
        create table if not exists '{}'(
        id integer primary key autoincrement,
        ip varchar(32) default '',
        time varchar(32) default '',
        status varchar(5) default '',
        url varchar(128) default '',
        referer varchar(128) default '',
        user_agent varchar(128) default '',
        country varchar(512) default '',
        city varchar(512) default '',
        type varchar(512) default '')
        """.format(self.tablename))

    # 查询包含数据
    def getdate(self, cwom, keys):
        db = sqlite3.connect(self.path)
        cu = db.cursor()
        sql = "SELECT * FROM '{}' where '{}' like '%{}%'".format(self.tablename, cwom, keys)
        # print(sql)
        cu.execute(sql)
        res = cu.fetchall()
        cu.close()
        db.close()
        return res

    # 查询所有数据
    def getdates(self):
        db = sqlite3.connect(self.path)
        cu = db.cursor()
        sql = "SELECT * FROM '{}'".format(self.tablename)
        cu.execute(sql)
        res = cu.fetchall()
        # print(res)
        cu.close()
        db.close()
        return res

    # 查询所有数据1
    def getdates1(self, logdata):
        db = sqlite3.connect(self.path)
        cu = db.cursor()
        sql = "SELECT * FROM '{}' WHERE time = '{}'".format(self.tablename, logdata)
        cu.execute(sql)
        res = cu.fetchall()
        cu.close()
        db.close()
        return res

    # 添加 字段 关键字
    def add(self, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8, keys9):
        db = sqlite3.connect(self.path)
        cu = db.cursor()
        sql = "insert into '{}'(ip,status,time,url,user_agent,referer,country,city,type) values('{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(
            self.tablename, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8, keys9)
        # print(sql)
        cu.execute(sql)
        db.commit()
        cu.close()

    # 删除 字段 关键字
    def delate(self, cwom, name):
        db = sqlite3.connect(self.path)
        cu = db.cursor()
        sql = "delete FROM '{}' where '{}'='{}'".format(self.tablename, cwom, name)
        cu.execute(sql)
        db.commit()
        cu.close()

    # #获取指定位置后面的10条数据 （分页）
    def limit(self, p=0):
        db = sqlite3.connect(self.path)
        cu = db.cursor()
        cu.execute("SELECT * FROM '{}' limit '{}',5".format(self.tablename, p))
        res = cu.fetchall()
        cu.close()
        db.close()
        return res

    def grop_ip(self):
        db = sqlite3.connect(self.path)
        cu = db.cursor()
        cu.execute("SELECT ip,COUNT(*) FROM '{}' GROUP BY ip".format(self.tablename))
        res = cu.fetchall()
        # print(res)
        cu.close()
        db.close()
        return res

    def grop_aget(self):
        db = sqlite3.connect(self.path)
        cu = db.cursor()
        cu.execute("SELECT user_agent,COUNT(*) FROM '{}' GROUP BY user_agent".format(self.tablename))
        res = cu.fetchall()
        cu.close()
        db.close()
        return res

    def grop_provs(self):
        db = sqlite3.connect(self.path)
        cu = db.cursor()
        cu.execute("SELECT country,COUNT(*) FROM '{}' GROUP BY country".format(self.tablename))
        res = cu.fetchall()
        cu.close()
        db.close()
        return res

    def get_tables(self):
        db = sqlite3.connect(self.path)
        cu = db.cursor()
        cu.execute("select name from sqlite_master where type='table' order by name;")
        res = cu.fetchall()
        tableslist = []
        for i in res:
            tableslist.append(i[0])
        cu.close()
        db.close()
        return tableslist

    def add_ziduan(self, ziduan):
        db = sqlite3.connect(self.path)
        cu = db.cursor()
        cu.execute("ALTER TABLE '{}'  ADD  '{}' varchar(512) NOT NULL Default ''".format(self.tablename, ziduan))
        res = cu.fetchall()
        cu.close()
        db.close()
        return res

    # 更新数据库表字段值
    def upstat(self, status, day):
        db = sqlite3.connect(self.path)
        cu = db.cursor()
        sql = "UPDATE {} SET ip = {} WHERE time = '{}'".format(self.tablename, status, day)
        print(sql)
        cu.execute(sql)
        res = cu.fetchall()
        db.commit()
        cu.close()
        db.close()
        return res

class Data:
    def __init__(self, title=None, values=None, xAxis=None, legend=None, unit=None, special=None):
        self.title = title  # 标题
        self.values = values  # 图表数据
        self.xAxis = xAxis  # 图例名称【一般折线，柱形图需要】
        self.legend = legend  # 横坐标数据【一般折线，柱形图需要】
        self.unit = unit  # 单位【可选】
        self.special = special  # 特殊图标保留参数


class iplogdata:
    def __init__(self, tablename):
        self.tablename = tablename
        # self.timeliast = timeliast
        # self.loglist = loglist
        # print(self.tablename)

    @property
    def pie(self):
        data = Data()
        data.title = '设备占比'
        db = dbsql3(datapath, self.tablename)
        ip_list = []
        for i in db.grop_aget():
            dat = {"name": i[0], "value": i[1]}
            ip_list.append(dat)
        data.values = ip_list
        data.legend = [key['name'] for key in data.values]
        data.unit = '访问数'
        return data

    # 使用
    @property
    def wordcloud(self):
        data = Data()
        print(self.tablename)
        data.title = '高频ip'
        db = dbsql3(datapath, self.tablename)
        ip_list = []
        for i in db.grop_ip():
            dat = {"name": i[0], "value": i[1]}
            ip_list.append(dat)
        data.values = ip_list
        data.unit = '访问次数'
        return data

    @property
    def china(self):
        data = Data()
        data.title = '地图高频'
        db = dbsql3(datapath, self.tablename)
        ip_list = []
        for i in db.grop_provs():
            dat = {"name": i[0], "value": i[1]}
            ip_list.append(dat)
        print(ip_list)
        data.values = ip_list
        data.unit = '次数'
        return data


class SourceData(iplogdata):
    ...


def get_tables(dbpath):
    db = sqlite3.connect(dbpath)
    cu = db.cursor()
    cu.execute("select name from sqlite_master where type='table' order by name;")
    res = cu.fetchall()
    tableslist = []
    for i in res:
        tableslist.append(i[0])
    cu.close()
    db.close()
    return tableslist



def get_all_table_data1():
    db = sqlite3.connect(datapath)
    cu = db.cursor()
    info = []
    sql1 = "SELECT id,ip,time,user_agent,country,city from logtable ORDER BY id DESC"
    try:
        res_data = cu.execute(sql1)
        for k1 in res_data:
            if k1[1] != "ip未知":
                data = {"id": k1[0], "ip": k1[1], "time": k1[2], "user_agent": k1[3], "country": k1[4], "city": k1[5]}
                info.append(data)
    except Exception as e:
        info = []
    cu.close()
    db.close()

    return info

def get_all_table_data(time, tablename):
    db = sqlite3.connect(datapath)
    cu = db.cursor()
    dictall = {}
    shebei = []
    weizhi = []
    ipnumber = []
    if len(time) == 2:
        # print(time[0],time[1])
        for com in tablename:
            if datetime.datetime.strptime(time[0], "%Y-%m-%d") > datetime.datetime.strptime(time[1], "%Y-%m-%d"):
                sql1 = "SELECT user_agent,COUNT(*) FROM '{}' WHERE time BETWEEN '{}' AND '{}' AND type = '{}' GROUP BY user_agent".format(
                    tbname, time[1], time[0], com)
                sql2 = "SELECT country,COUNT(*) FROM '{}' WHERE time BETWEEN '{}' AND '{}' AND type = '{}' GROUP BY country".format(
                    tbname, time[1], time[0], com)
                sql3 = "SELECT ip,COUNT(*) FROM '{}' WHERE time BETWEEN '{}' AND '{}' AND type = '{}' GROUP BY ip".format(
                    tbname, time[1], time[0], com)
            elif datetime.datetime.strptime(time[0], "%Y-%m-%d") < datetime.datetime.strptime(time[1], "%Y-%m-%d"):
                sql1 = "SELECT user_agent,COUNT(*) FROM '{}' WHERE time BETWEEN '{}' AND '{}' AND type = '{}' GROUP BY user_agent".format(
                    tbname, time[0], time[1], com)
                sql2 = "SELECT country,COUNT(*) FROM '{}' WHERE time BETWEEN '{}' AND '{}' AND type = '{}' GROUP BY country".format(
                    tbname, time[0], time[1], com)
                sql3 = "SELECT ip,COUNT(*) FROM '{}' WHERE time BETWEEN '{}' AND '{}' AND type = '{}' GROUP BY ip".format(
                    tbname, time[0], time[1], com)
            else:
                sql1 = "SELECT user_agent,COUNT(*) FROM '{}' WHERE time LIKE '%{}%'  AND type = '{}' GROUP BY user_agent".format(
                    tbname, time[0], com)
                sql2 = "SELECT country,COUNT(*) FROM '{}' WHERE time LIKE '%{}%'  AND type = '{}' GROUP BY country".format(
                    tbname, time[0], com)
                sql3 = "SELECT ip,COUNT(*) FROM '{}' WHERE time LIKE '%{}%'  AND type = '{}' GROUP BY ip".format(
                    tbname, time[0], com)

            cu.execute(sql1)
            res_user_agent = cu.fetchall()
            for k1 in res_user_agent:
                data = {"设备": k1[0], "num": k1[1]}
                shebei.append(data)

            cu.execute(sql2)
            res_provs = cu.fetchall()
            for k1 in res_provs:
                data = {"城市": k1[0], "num": k1[1]}
                weizhi.append(data)

            cu.execute(sql3)
            res_ip = cu.fetchall()
            for k1 in res_ip:
                data = {"ip": k1[0], "num": k1[1]}
                ipnumber.append(data)

        def myFunc(e):
            print(e)
            return e[1]

        # print(ipnumber)
        dictall["user_agent"] = shebei
        dictall["provs"] = weizhi
        dictall["ip"] = ipnumber
        cu.close()
        db.close()
    if len(time) == 1:
        for com in tablename:
            sql1 = "SELECT user_agent,COUNT(*) FROM '{}' WHERE time LIKE '%{}%'  AND type = '{}' GROUP BY user_agent".format(
                tbname, time[0], com)
            sql2 = "SELECT country,COUNT(*) FROM '{}' WHERE time LIKE '%{}%'  AND type = '{}' GROUP BY country".format(
                tbname, time[0], com)
            sql3 = "SELECT ip,COUNT(*) FROM '{}' WHERE time LIKE '%{}%'  AND type = '{}' GROUP BY ip".format(
                tbname, time[0], com)
            cu.execute(sql1)
            res_user_agent = cu.fetchall()
            for k1 in res_user_agent:
                data = {"设备": k1[0], "num": k1[1]}
                shebei.append(data)

            cu.execute(sql2)
            res_provs = cu.fetchall()
            for k1 in res_provs:
                data = {"城市": k1[0], "num": k1[1]}
                weizhi.append(data)

            cu.execute(sql3)
            res_ip = cu.fetchall()
            for k1 in res_ip:
                data = {"ip": k1[0], "num": k1[1]}
                ipnumber.append(data)

        # print(ipnumber)
        def myFunc(e):
            print(e)
            return e[1]

        dictall["user_agent"] = shebei
        dictall["provs"] = weizhi
        dictall["ip"] = ipnumber
        cu.close()
        db.close()
    return dictall





# 通过ip获取国家
def get_ip_addr(ip):
    with geoip2.database.Reader(ipdbname) as reader:
        response = reader.city(ip)
        try:
            city = response.city.names["zh-CN"]
            # city=response.location.time_zone
        except Exception as e:
            city = response.location.time_zone.split("/")[1]
        addrs = response.country.names["zh-CN"]
        data = {"country": addrs, "city": city}
        return data

# 获取有效日志列表
def get_com_name(path):
    all_list = []
    for all_name in os.listdir(path):
        for com in cnamlist:
            if ".log" in all_name and com in all_name:
                # print(all_name)
                all_list.append(all_name)
    return all_list


# 处理每一行log数据 f日志列表 type域名 day后缀时间 number处理行号 插入sqlite3数据库
def upsql_3(info, iplist, f, type, day, number=0):
    try:
        num = int(dbsql3(datapath, "num").getdates1(day)[0][1])
        # print(num)
        # 初始化肯定获取不到
    except Exception as e:
        a = 0
        dbsql3(datapath, "num").add(a, a, str(day), a, a, a, a, a, a)
        num = 0
    ualist = []
    index1 = 0
    for log in f:
        index1 += 1
        if index1 > int(num):
            obj = re.compile(
                r'(?P<ip>.*?)- - \[(?P<time>.*?)\] "(?P<request>.*?)" (?P<status>.*?) (?P<bytes>.*?) "(?P<referer>.*?)" "(?P<ua>.*?)"')
            result = obj.match(log)
            # ip处理
            try:
                ip = result.group("ip").split(",")[0].strip()  # ip
            except Exception as e:
                ip = "ip未知"
            try:
                status = result.group("status")  # 状态码
            except Exception as e:
                status = "请求状态未知"
            try:
                time = datetime.datetime.strptime(result.group("time").replace(" +0800", ""),
                                                  "%d/%b/%Y:%H:%M:%S")  # 时间格式化
            except Exception as e:
                time = "请求时间未知"
            try:
                request = result.group("request").split()[1].split("?")[0]  # 请求url路径
            except Exception as e:
                request = "请求url路径未知"
            # print(request)
            try:
                ua = result.group("ua")
                # print(ua)
                for aget in ua:
                    if aget not in ualist:
                        ualist.append(aget)
                if "Windows" in ua:
                    u = "windows"
                elif "iPad" in ua:
                    u = "ipad"
                elif "Android" in ua:
                    u = "android"
                elif "Macintosh" in ua:
                    u = "mac"
                elif "iPhone" in ua:
                    u = "iphone"
                elif "Python" in ua:
                    u = "Python"
                elif "HttpClient" in ua:
                    u = "HttpClient"
                elif "Linux" or "Ubuntu" in ua:
                    u = "Linux"
                elif "curl" in ua:
                    u = "curl"
                elif "InternetMeasurement" in ua:
                    u = "InternetMeasurement"
                elif "CSSCheck" in ua:
                    u = "CSSCheck"
                elif "spider" in ua:
                    u = "spider"
                elif "WebKit" in ua:
                    u = "WebKit"
                elif "-" in ua:
                    u = "未知"
                else:
                    u = ua
            except Exception as e:
                u = "未知"
            try:
                # 获取省份优化
                for k in info.keys():
                    iplist.append(k)
                if ip not in iplist:
                    provs = get_ip_addr(ip)
                    country = provs["country"]
                    city = provs["city"]
                    info[ip] = provs
                else:
                    provs = info[ip]
                    country = provs["country"]
                    city = provs["city"]
            except Exception as e:
                country = "未知"
                city = "未知"
            try:
                referer = result.group("referer")
            except Exception as e:
                referer = "未知"

            if request == "/" or request == "/favicon.ico":
                # index1+=1
                # print("ip:" + ip, "时间：" + str(time), "状态码：" + status, "请求url路径" + request, "请求设备类型:" + u,
                #       "请求国家：" + country+"城市："+city)
                db1 = dbsql3(datapath, tbname)
                db1.add(ip, status, time, request, u, referer, country, city, type)

    db2 = dbsql3(datapath, "num")
    print(index1, day)
    db2.upstat(index1, day)





# 初始化数据
def scaner_file(logpath):
    info = {}
    iplist = []
    for logname in get_com_name(logpath):
        i = logname

        day = i.split(".log")[0].split("_")[2]
        # print(day)
        with open("%s" % (i), 'r') as f:
            f = f.readlines()
            i = i.split(".com")
            i = i[0] + ".com"
            upsql_3(info, iplist, f, i, day)
#获取时间差列表
def data_time(a,b):
    listtime=[]
    start_date = datetime.datetime.strptime(a, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(b, '%Y-%m-%d').date()
    delta = end_date - start_date
    date_list = [start_date + datetime.timedelta(days=x) for x in range(delta.days + 1)]
    for date in date_list:
        listtime.append(date.strftime("%Y-%m-%d"))
    # print(listtime)
    return listtime

#定时更新数据到数据库
def update_today():
    info = {}
    iplist = []
    # line= int(dbsql3(datapath, "num").getdates()[-1][1])
    for logname in get_com_name(logpath):
        for daylog in data_time(str(dbsql3(datapath, "num").getdates()[-1][2]),today):
            if daylog in logname:
                print(logname)
                type = logname.split("_")[0]
                logname = os.getcwd() + "/" + logname
                with open("%s" % (logname), 'r') as f:
                    f = f.readlines()
                    # if len(f)>line:
                    upsql_3(info, iplist, f, type, daylog)

def start_app():
    dbsql3(datapath, tbname)
    app = Flask(__name__)
    app.config['JSON_AS_ASCII'] = False


    def logtxt():
        datalogpath = os.path.join(os.getcwd(), "flasklog.txt")
        logging.basicConfig(filename=datalogpath, format='%(asctime)s  - %(levelname)s - %(message)s')


    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404


    @app.route('/', methods=['GET', 'POST'])
    def index():
        return render_template('testlayui.html')
        # return render_template('loginfolayui.html')

    #饼状图
    @app.route('/pie', methods=['GET'])
    def pie():
        table_name = tbname
        source = SourceData(table_name)
        # data.values=user_agent_list
        data = source.pie
        return render_template('pie.html', title=data.title, data=data.values, legend=data.legend, unit=data.unit)

    #词云
    @app.route('/wordcloud', methods=['GET'])
    def wordcloud():
        table_name = request.args.get('query')
        table_name = tbname
        source = SourceData(table_name)
        data = source.wordcloud
        return render_template('wordcloud.html', title=data.title, data=data.values, unit=data.unit)


    #中国
    @app.route('/china', methods=['GET'])
    def china():
        table_name = request.args.get('query')
        table_name = tbname
        source = SourceData(table_name)
        data = source.china
        return render_template('china.html', title=data.title, data=data.values, unit=data.unit)


    #世界
    @app.route('/china1', methods=['GET'])
    def china1():
        table_name = tbname
        source = SourceData(table_name)
        data = source.china
        return render_template('china1.html', title=data.title, data=data.values, unit=data.unit)

    #数据初始化更新接口
    @app.route('/updatelogdata', methods=['GET'])
    def updatelogdata():
        passwd = request.args.get('pwd')
        if passwd == "init":
            if len(dbsql3(datapath, "init").getdates()) == 0:
                a = 0
                dbsql3(datapath, "init").add(a, a, a, a, a, a, a, a, a)
                scaner_file(logpath)
                with open("init.log", "a+", encoding="utf-8") as f:
                    f.write("%s数据初始化成功" % (str(time.strftime("%Y-%m-%d %H:%M:%S"))) + "\n")
                return "数据初始化成功，注意数据初始化只请求一次。<a href='/'>返回</a>"
            else:
                return "已经数据初始化<a href='/'>返回</a>"
        elif passwd == "update":
            if len(dbsql3(datapath, "init").getdates()) == 0:
                a = 0
                dbsql3(datapath, "init").add(a, a, a, a, a, a, a, a, a)
                scaner_file(logpath)
                with open("init.log", "a+", encoding="utf-8") as f:
                    f.write("%s数据初始化成功" % (str(time.strftime("%Y-%m-%d %H:%M:%S"))) + "\n")
                return "数据初始化成功，注意数据初始化只请求一次。<a href='/'>返回</a>"
            else:
                update_today()
                return "数据更新成功<a href='/'>返回</a>"
        else:
            return render_template('404.html'), 404

    #获取数据api
    @app.route('/dataapi')
    def dataapi():
        def m5(x):
            text1 = x
            SALE = "dfewcfewcvfewfew"  # 设置盐值
            md_sale1 = hashlib.md5((text1 + SALE).encode())  # MD5加盐加密方法一：将盐拼接在原密码后
            md_sale2 = hashlib.md5(("fewfweffrwffew" + SALE).encode())  # MD5加盐加密方法一：将盐拼接在原密码后
            md5salepwd1 = md_sale1.hexdigest()
            md5salepwd2 = md_sale2.hexdigest()
            if md5salepwd1 == md5salepwd2:
                return True
            return False

        passwd = request.args.get("pwd")
        if m5(passwd):
            return dbsql3(datapath, tbname).getdates()
        return json.dumps(200)


    @app.route('/testlayuiapi', methods=['POST', 'GET'])
    def testlayuiapi():
        """
         请求的数据源，该函数模拟数据库中存储的数据，返回以下这种数据的列表：
        {'name': '香蕉', 'id': 1, 'price': '10'}
        {'name': '苹果', 'id': 2, 'price': '10'}
        """
        if request.method == 'POST':
            pass
        if request.method == 'GET':
            info = request.values
            limit = info.get('limit', 10)  # 每页显示的条数
            offset = info.get('offset', 0)  # 分片数，(页码-1)*limit，它表示一段数据的起点
            # print('get', limit)
            data = get_all_table_data1()
            # print(data)
        return jsonify({'total': len(data), 'rows': data[int(offset):(int(offset) + int(limit))]})
        # return render_template('test.html', itemsip=itemsip, itemsuser=itemsuser, itemsprovs=itemsprovs)
        # return jsonify({'total': len(data), 'rows': data[int(offset):(int(offset) + int(limit))]})
        # 注意total与rows是必须的两个参数，名字不能写错，total是数据的总长度，rows是每页要显示的数据,它是一个列表
        # 前端根本不需要指定total和rows这俩参数，他们已经封装在了bootstrap table里了
    #启动flask
    with app.app_context():
        # logtxt()
        # app.run(host='0.0.0.0', port=5355, debug=False)
        app.run(host='0.0.0.0', port=5355, debug=True)
if __name__ == "__main__":
        start_app()
