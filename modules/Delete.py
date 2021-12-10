import csv
import os
import re
import pandas as pd
from . import utils

meta_path = "database/metas/"   # 存放 元数据表 位置
data_path = "database/datas/"   # 存放 数据表 位置

def deleteData(sql):
    # 无where子句的解析
    if 'where' not in sql:
        searchObj = re.search(r'^delete from (.*);$', sql, re.I)
        if searchObj:
            tableName = searchObj.group(1).strip()
            is_table_exist = utils.isTableExist(tableName)
            if is_table_exist:
                deleteAllData(tableName)
                print("表" + tableName + "已删除\n")
                return
            else:
                print("表" + tableName + "不存在或无数据\n")
                return
        else:
            print("SQL解析失败")
            return

    # 有where子句的解析
    else:
        searchObj = re.search(r'^delete from (.*) where (.*);$', sql, re.I)
        if searchObj:
            tableName = searchObj.group(1).strip()
            conditions = searchObj.group(2).strip()
            is_table_exist = utils.isTableExist(tableName)
            if is_table_exist:
                deleteCondition(tableName, conditions)
                return
            else:
                print("表" + tableName + "不存在或无数据\n")
                return
        else:
            print("SQL解析失败")
            return


def deleteAllData(tableName):
    os.remove(data_path + tableName + ".csv")

def deleteCondition(tableName, conditions):
    if 'and' not in conditions and 'or' not in conditions:
        if '>=' in conditions:
            deleteFilter(tableName, conditions, '>=')
        elif '<=' in conditions:
            deleteFilter(tableName, conditions, '<=')
        elif '!=' in conditions:
            deleteFilter(tableName, conditions, '!=')
        elif '>' in conditions:
            deleteFilter(tableName, conditions, '>')
        elif '<' in conditions:
            deleteFilter(tableName, conditions, '<')
        elif '=' in conditions:
            deleteFilter(tableName, conditions, '=')
        else:
            print("SQL解析错误")
            return
    else:
        print("功能暂未完成")


def deleteFilter(tableName, conditions, param):
    dataList = utils.getDataList(tableName)

    # 过滤不满足where子句的行
    rows = []
    if param == '!=':
        filters = conditions.split('!=')
        filters[0] = filters[0].strip()
        filters[1] = filters[1].strip()
        filter_index = utils.getIndexList(tableName, filters[0])[0]
        # 找到满足条件的行号
        rows = utils.findMatchRow(dataList, filters[1], filter_index, '!=')
    elif param == '>=':
        filters = conditions.split('>=')
        filters[0] = filters[0].strip()
        filters[1] = filters[1].strip()
        filter_index = utils.getIndexList(tableName, filters[0])[0]
        rows = utils.findMatchRow(dataList, filters[1], filter_index, '>=')
    elif param == '<=':
        filters = conditions.split('<=')
        filters[0] = filters[0].strip()
        filters[1] = filters[1].strip()
        filter_index = utils.getIndexList(tableName, filters[0])[0]
        rows = utils.findMatchRow(dataList, filters[1], filter_index, '<=')
    elif param == '>':
        filters = conditions.split('>')
        filters[0] = filters[0].strip()
        filters[1] = filters[1].strip()
        filter_index = utils.getIndexList(tableName, filters[0])[0]
        rows = utils.findMatchRow(dataList, filters[1], filter_index, ">")
    elif param == '<':
        filters = conditions.split('<')
        filters[0] = filters[0].strip()
        filters[1] = filters[1].strip()
        filter_index = utils.getIndexList(tableName, filters[0])[0]
        rows = utils.findMatchRow(dataList, filters[1], filter_index, "<")
    elif param == '=':
        filters = conditions.split('=')
        filters[0] = filters[0].strip()
        filters[1] = filters[1].strip()
        filter_index = utils.getIndexList(tableName, filters[0])
        if filter_index == []:
            print("表" + tableName + "中没有列" + filters[0], end='\n\n')
            return
        else:
            rows = utils.findMatchRow(dataList, filters[1], filter_index[0], "=")
    else:
        print("SQL解析错误")
        return
    if len(rows) == 0:
        print("没有满足条件的行\n")
        return
    # 删除行，写入csv文件
    dataList = pd.read_csv(data_path + tableName + '.csv', encoding='utf-8', header=None)
    new_dataList = dataList.drop(rows)
    new_dataList.to_csv(data_path + tableName + '.csv', index=0, encoding='utf-8', header=None)
    print("删除\'" + conditions + "\' 成功\n")
    return

# delete from test where age = 1;   # 检查：不存在匹配的数据
# delete from test where aaa=bbb;   # 检查：不存在的字段
# delete from aaa;      # 检查：不存在的表
# delete from test where id = 1112;
# delete from test;

# delete from student where id = 20210002;
# delete from aaa;      # 检查delete不存在的表