import csv
import os
import re
import pandas as pd
from . import utils

meta_path = "database/metas/"   # 存放 元数据表 位置
data_path = "database/datas/"   # 存放 数据表 位置

def updateData(sql):
    if 'where' in sql:
        searchObj = re.search(r'^update (.*) set (.*) where (.*);$', sql, re.I)
        if searchObj:
            tableName = searchObj.group(1).strip()
            columns_values = searchObj.group(2).split(', ')
            conditions = searchObj.group(3)

            columns_values = [column_value.strip() for column_value in columns_values]
            # 判断表是否存在
            is_table_exist = utils.isTableExist(tableName)
            if is_table_exist:
                updateItem(tableName, columns_values, conditions)
            else:
                print("表\'" + tableName + "\'不存在\n")
                return
        else:
            print("SQL解析错误")
            return
    else:
        print("SQL解析错误，请使用where子句")
        return


def updateItem(tableName, columns_values, conditions):
    # 根据where子句，更新set子句中的值
    if '=' in conditions:       # conditions形如 id = 111
        # conditions[0]是过滤字段名，conditions[1]是过滤值
        conditions = conditions.split('=')
        conditions = [condition.strip() for condition in conditions]

        for column_value in columns_values:     # column_value形如 name = 张三
            # column为待set字段，column为待set值
            column = column_value.split('=')[0].strip()
            value = column_value.split('=')[1].strip()

            # 检查主键 不可重复
            primary_key, primary_index = utils.getPrimaryKey(tableName)
            if primary_key and primary_key == column:
                is_repeat = utils.checkRepeat(tableName, primary_index, value)
                if is_repeat:
                    print("\'" + primary_key + "\'为主键，且已有值\'" + value + "\'\n")
                    return

            # 查找conditions[0]索引、对应conditions[1]值的行，更新值
            condition_index = utils.getIndexList(tableName, conditions[0])
            # 检查是否有conditions[0]字段
            if condition_index == []:
                print("表" + tableName +"中没有列" + conditions[0], end='\n\n')
                return
            else:
                condition_index = condition_index[0]
            dataList = utils.getDataList(tableName)
            value_indexs = utils.findMatchRow(dataList, conditions[1], condition_index, '=')
            if value_indexs == []:
                print("未找到" + conditions[0] + "等于" + conditions[1] + "的数据行")
                return

            # 修改行，写入csv文件
            column_index = utils.getIndexList(tableName, column)
            # 检查是否有column字段
            if column_index == []:
                print("表" + tableName + "中没有列" + column, end='\n\n')
                return
            else:
                column_index = column_index[0]
            dataList = pd.read_csv(data_path + tableName + '.csv', encoding='utf-8', header=None)
            for value_index in value_indexs:
                dataList.loc[value_index, column_index] = value
            dataList.to_csv(data_path + tableName + '.csv', index=0, encoding='utf-8', header=None)
            print("修改成功\n")
            return

    else:
        print("功能暂未实现，目前update只能使用=匹配，其他匹配实现逻辑相同\n")
        return

# update test set age=20 where id=1113;
# update test set aaa=bbb where id=1113;    # 检查：没有aaa字段
# update aaa set a=123 where b=321;         # 检查：没有aaa表

# update student set age=18 where id=20210001;  # 可以执行
# update student set age=18 where id=1;     # 检查：没有id=1的数据
# update student set age=18 where aaa=1;    # 检查：没有aaa的字段
# update student set bbb=18 where id=20210001;  # 检查：没有bbb的字段
# update student set id=20210001 where id=20210001; # 检查：主键不可重复