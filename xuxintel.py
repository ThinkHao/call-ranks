import os
import pymysql
import pymssql
import time
import datetime
import collections
from prettytable import PrettyTable
from flask import Flask

app = Flask(__name__)

# 呼出
out = []
# 呼入
inc = []
# 所有人名
names = []
# 有电话记录的人
made_calls = []
# 所有电话号码
nums = {}
# 过滤组长
boss = ['何传航', '李美华', '周荣华']

# 录音数据库配置
server="192.168.18.181"
user="sa"
password="88888"

# mysql数据库配置
config={
    "host":"127.0.0.1",
    "user":"root",
    "password":"root",
    "database":"db_telcount"
}

# 输出表格初始化
x = PrettyTable(["名字", "呼出"])

# 获取当前时间
def get_now_time():
    #print('正在获取当前时间...')
    now_time = datetime.datetime.now().strftime('%Y-%m-%d')
    now_str = str(now_time)
    #print('当前时间为{}'.format(now_str))
    return now_str

# 连接数据库查询通话记录
def con_sql(server, user, password):
    print('[info]正在向数据库请求数据...')
    conn = pymssql.connect(server, user, password, database="luyin")
    cursor = conn.cursor()
    sql_cmd = "select * from Record where Type='{}' or Type='{}'".format('OUT', 'INC')
    cursor.execute(sql_cmd)
    rows = cursor.fetchall()
    print('[info]请求完毕，共{}条数据.'.format(len(rows)))
    conn.close()
    return rows

# 解析获取到的通话记录，获取所需记录
def parse_rows(raw_datas,container1=None):
    nowtime = get_now_time()
    print('[info]正在解析数据...')
    if container1 == None:
        container1 = []
    if raw_datas:
        for raw_data in raw_datas:
            if raw_data[16] != None:
                dict = {
                    'time': str(raw_data[1]).split(' '),
                    'telnum': raw_data[4],
                    'user': raw_data[16],
                    'type': raw_data[12]
                }
                if dict['time'][0] == nowtime:
                    container1.append(dict)
        print('[info]完成解析,已获取今日{}条数据.'.format(len(container1)))
        return container1
    else:
        return None

# 获取销售电话排行
def get_call_rank(datas,number):
    now_time_str = get_now_time()
    if len(datas) != 0:
        for data in datas:
            if data['type'] == 'OUT':
                out.append(data['user'])
            if data['type'] == 'INC':
                inc.append(data['user'])
        objs_out_sort = collections.Counter(out).most_common(number)
        del inc[:]
        del out[:]
        return objs_out_sort
    else:
        print(u'暂无数据')
        return None

# 获取清洗后的数据
def get_filtered_data(data,container=None):
    if container == None:
        container = []
    now_time_str = get_now_time()
    if len(data) != 0:
        if data['type'] == 'OUT' and data['time'][0] == now_time_str and data['user'] != None:
            for d in data:
                container.append(d)
    return container

# 把人名和呼出次数写入数据库
def save_to_mysql(config, name, out):
    db = pymysql.connect(**config)
    cursor = db.cursor()
    sql_cmd = "INSERT INTO `t_telcount`( `name`, `out`) VALUES ('{}',{}) on duplicate key update `name`='{}',`out`={}".format(name, out, name, out)
    cursor.execute(sql_cmd)
    db.commit()  # 提交数据
    cursor.close()
    db.close()

# 把人名和电话号码存入数据库
def tel_name_mysql(config, name, telnum):
    db = pymysql.connect(**config)
    cursor = db.cursor()
    sql_cmd = "INSERT INTO `t_telname`( `name`, `telnum`) VALUES ('{}',{}) on duplicate key update `name`='{}',`telnum`={}".format(name, telnum, name, telnum)
    cursor.execute(sql_cmd)
    db.commit()  # 提交数据
    cursor.close()
    db.close()

@app.route('/')
def main():
    i = 0
    while True:
        raw_results = con_sql(server, user, password)
        parsed_datas = parse_rows(raw_results)
        ranks = get_call_rank(parsed_datas,60)
        for rank in ranks:
            x.add_row([rank[0], rank[1]])
            save_to_mysql(config, rank[0], rank[1])
        for parsed_data in parsed_datas:
            tel_name_mysql(config, parsed_data['user'], parsed_data['telnum'])
        i += 1
        print(x)
        print('Success! 第{}次入库!'.format(i))
        x.clear_rows()
        time.sleep(30)

if __name__ == '__main__':
    main()
