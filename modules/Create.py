import re
import csv
import os
from . import Entity

"""
    CREATE TABLE语句
"""
meta_path = "database/metas/"   # 存放 元数据表 位置
data_path = "database/datas/"   # 存放 数据表 位置

def isPrimaryKey(item):
    # 如果有PRIMARY KEY字段，则一定为主键
    if 'PRIMARY KEY' in item:
        return True
    else:
        return False


def isNotNull(item):
    # 如果有NOT NULL字段、PRIMARY KEY字段，则非空
    if 'NOT NULL' in item or isPrimaryKey(item):
        return True
    else:
        return False


def createTable(sql):
    # 正则匹配，忽略大小写
    searchObj = re.search(r'^create table (.*)[ ]?\(.*\);$', sql, re.I)
    if searchObj:
        # 取create table 后第一个参数为tableName
        tableName = searchObj.group(1).rstrip()

        # 验证没有同名表已存在
        if not os.path.exists(meta_path + tableName + '.csv'):
            # 创建元数据表 MetaTable
            metaTable = Entity.MetaTable(tableName)

            # 解析字段
            index = sql.find('(')
            tableKeys = sql[index+1: -2]
            tableKeys = tableKeys.split(', ')
            if tableKeys == ['']:
                print("表属性不能为空")
                print("TABLE Field CAN NOT BE NULL!\n")
                return

            primary_num = 0
            for item in tableKeys:
                item.lstrip()
                item.rstrip()
                field = item.split(' ')[0]      # 字段名
                type = item.split(' ')[1]       # 类型
                is_primary = isPrimaryKey(item) # 是否为主键
                if is_primary:
                    primary_num = primary_num + 1
                is_not_null = isNotNull(item)   # 是否非空

                if primary_num > 1:
                    print("只能有一个主键")
                    print("PRIMARY KEY SHOULD BE ONLY ONE\n")
                    return

                # 创建元数据 Meta
                meta = Entity.Meta(field=field, type=type, isPrimary=is_primary, isNotNull=is_not_null)
                metaTable.appendMeta(meta)      # 保存到metaTable中

            # 写入元数据表
            with open(meta_path+tableName+'.csv', 'w+', newline='', encoding='utf-8') as f:
                file = csv.writer(f)
                for meta in metaTable.getMetas():
                    file.writerow(meta.getMeta())

            print("创建表" + tableName +"成功")
            print("CREATE TABLE " + tableName + " SUCCESSFULLY\n")

        else:   # 已有同名表
            print("表\'" + tableName + "\'已存在! 请使用其他名称!")
            print("TABLE \'" + tableName + "\' HAS EXIST!")
            print("PLEASE USE OTHER TABLE NAME\n")

    else:       # 正则匹配失败
        print("SQL 语法错误")
        print("SQL SYNTAX ERROR\n")

# create table test(id string PRIMARY KEY, name string, age int);   # 若表已存在，则会提示
# create table student (id string PRIMARY KEY, name string NOT NULL, major string, age int);