import os
import pymysql
import pymssql
import time
import datetime
import collections
from prettytable import PrettyTable


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
x = PrettyTable(["名字", "呼出", "呼入"])

# 数据过滤后存放的列表
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

def save_to_mysql(config, name, out, inc):
    db = pymysql.connect(**config)
    cursor = db.cursor()
    sql_cmd = "INSERT INTO `t_telcount`( `name`, `out`, `inc`) VALUES ('{}',{},{}) on duplicate key update `name`='{}',`out`={},`inc`={}".format(name, out, inc, name, out, inc)
    cursor.execute(sql_cmd)
    db.commit()  # 提交数据
    cursor.close()
    db.close()

# 获取当前时间
def get_now_time():
    now_time = datetime.datetime.now().strftime('%Y-%m-%d')
    now_str = str(now_time)
    return now_str

# 连接数据库查询通话记录
def con_sql(server, user, password):
    conn = pymssql.connect(server, user, password, database="luyin")
    cursor = conn.cursor()
    sql_cmd = "select * from Record where Type='{}' or Type='{}'".format('OUT', 'INC')
    cursor.execute(sql_cmd)
    rows = cursor.fetchall()
    conn.close()
    return rows

# 解析获取到的通话记录，获取所需字段
def parse_rows(results):
    for result in results:
        if result:
            dict = {
                'time': str(result[1]).split(' '),
                'telnum': result[4],
                'user': result[16],
                'type': result[12]
            }
            return dict
        else:
            return None

# 对获取到的字段进行过滤，只保留今天的通话数据，剔除无名字的条目,并且排序
def filter_time_name(row, number):
    now_time_str = get_now_time()
    if len(row) != 0:
        if row['type'] == 'OUT':
            if row['time'][0] == now_time_str:
                if row['user'] != None:
                    out.append(row['user'])
                    objs_out_sort = collections.Counter(out).most_common(number)
                    return objs_out_sort
        if row['type'] == 'INC':
            if row['time'][0] == now_time_str:
                if row['user'] != None:
                    inc.append(row['user'])
                    objs_inc_sort = collections.Counter(inc).most_common(number)
                    return objs_inc_sort
    else:
        print(u'暂无数据')
        return None

# 获取所有成员
def get_all_members(server, username, password):
    results = con_sql(server, username, password)
    if results:
        rows = parse_rows(results)
        for row in rows:
            names.append(row)
    return set(names)

def print_and_save(filtered_row):
    if filtered_row != None:
        if len(out) == 0 & len(inc) == 0:
            print(u'今日还没有电话记录，加油~')
        elif len(out) != 0:
            for i in filtered_row:
                if i[0] not in boss:
                    x.add_row([i[0], i[1], inc.count(i[0])])
                    save_to_mysql(config, i[0], i[1], inc.count(i[0]))
            return x
        elif len(inc) != 0:
            for i in filtered_row:
                if i[0] not in boss:
                    x.add_row([i[0], inc.count(i[0]), i[1]])
                    save_to_mysql(config, i[0], inc.count(i[0]), i[1])
            return x
    else:
        print(u"没有内容可以显示")
        return None

def add_telnum(config, results):
    db = pymysql.connect(**config)
    cursor = db.cursor()
    for result in results:
        telnum = result[4]
        name = result[16]
        if name != None:
            sql_cmd = "INSERT INTO `t_telcount`( `telnum`) VALUES ({}) FROM DUAL WHERE EXISTS (select 1 from `t_telnum` where `name`={})".format(telnum, name)
            cursor.execute(sql_cmd)
    db.commit()  # 提交数据
    cursor.close()
    db.close()



def main():
    # num用于计算输出次数
    num = 0
    while True:
        members = get_all_members(server, user, password)
        number = len(members)
        results = con_sql(server, user, password)
        #for result in results:
        row = parse_rows(results)
        filtered_row = filter_time_name(row, number)
        table = print_and_save(filtered_row)
        if table != None:
            print(table)
    #add_telnum(config, results)
            table.clear_rows()
        del out[:]
        del inc[:]
        # 每次循环结束，计数加1
        num += 1
        print('=' * 15, '第' + str(num) + '轮', '=' * 15)
        time.sleep(30)


if __name__ == '__main__':
    main()