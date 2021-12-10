import re
import os
import csv
from . import Entity
from . import utils

meta_path = "database/metas/"   # 存放 元数据表 位置
data_path = "database/datas/"   # 存放 数据表 位置

def insertData(sql):
    # 正则匹配，忽略大小写
    searchObj = re.search(r'^insert into (.*)[ ]?(\(.*\))[ ]?values[ ]?(.*);$', sql, re.I)
    if searchObj:
        tableName = searchObj.group(1).rstrip()

        # 取出字段名和值
        columns = (searchObj.group(2)[1:-1]).split(',')    # [1:-1]去掉()
        values = (searchObj.group(3)[1:-1]).split(',')
        columns = [column.strip() for column in columns]
        values = [value.strip() for value in values]
        # print("columns: ", columns)
        # print("values: ", values)

        if os.path.exists(meta_path + tableName + ".csv"):
            # 检查字段是否合法
            is_column, column = utils.checkColumn(tableName, columns)
            if not is_column:
                print("给定字段\'" + column + "\'不合法\n")
                return
            # 检查非空约束
            not_null = utils.getNotNull(tableName)
            if not_null != []:  # 有字段是NOT NULL的
                # print("not_null: ", not_null) # 查看非空字段列表
                for field in not_null:
                    if field not in columns:    # 不满足非空约束
                        # print("field: ", field)
                        print(field + " 是非空的，需要提供值")
                        print(field + " IS NOT NULL, VALUE NEEDED\n")
                        return

            # 主键检查
            primary_key, index = utils.getPrimaryKey(tableName)
            # print("primary key: ", primary_key)
            # print("index: ", index)
            if os.path.exists(data_path + tableName + ".csv"):
                i = -1      #
                for column in columns:
                    i = i + 1
                    if column == primary_key:
                        break
                # print("columns[", i, "]: ", columns[i])
                # print("values[", i, "]: ", values[i])

                # 从data文件中读出主键对应的所有值
                with open(data_path + tableName + ".csv", 'r', encoding='utf-8') as f:
                    file = csv.reader(f)
                    for item in file:
                        if values[i] == item[index]:
                            print("主键有重复")
                            print("PRIMARY KEY HAS REPEAT\n")
                            return

            # 写入数据表
            datas = zip(columns, values)
            row = Entity.Row(metaTable=Entity.MetaTable.load(tableName), datas=datas)
            with open(data_path + tableName + '.csv', 'a+', newline='', encoding='utf-8') as f:
                file = csv.writer(f)
                file.writerow(row.getRow())
            print("插入表" + tableName +"成功")
            print("INSERT INTO " + tableName + " SUCCESSFULLY\n")

        else:   # 该表不存在
            print("TABLE \'" + tableName + "\' DO NOT EXIST\n")
    else:
        print("SQL语法错误\n")


# 数据初始化
# insert into test(id, name, age) values (1111, 张三, 21);
# insert into test(id, name, age) values (1112, 李四, 20);
# insert into test(id, name) values (1113, 王五);

# insert into test(id, name, age) values (1111, 张三, 21);     # 检查：主键约束，id重复
# insert into test(name, age) values (张三, 21);   # 检查：非空约束，id为PRIMARY KEY

# insert into student(id, name, major, age) values(20210001, 李四, 计算机, 18);
# insert into student(id, name, major) values(20210002, 王五, 计算机);