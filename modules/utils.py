import os
import csv

meta_path = "database/metas/"   # 存放 元数据表 位置
data_path = "database/datas/"   # 存放 数据表 位置

# 从数据表中获取所有数据
def getDataList(tableName):
    with open(data_path + tableName + '.csv', 'r', encoding='utf-8') as f:
        dataList = []
        file = csv.reader(f)
        for item in file:
            dataList.append(item)

    return dataList


# 从metaTable中获取 非空约束 字段
def getNotNull(tableName):
    with open(meta_path + tableName + ".csv", 'r', encoding='utf-8') as f:
        file = csv.reader(f)
        not_null = []
        # 如果 isNotNull 为True，则返回字段名，进行非空约束
        for item in file:
            # print("item: ", item)
            if item[3] == str(True):
                not_null.append(item[0])

    return not_null


# 从metaTable中获取 主键 的字段、索引
def getPrimaryKey(tableName):
    with open(meta_path + tableName + ".csv", 'r', encoding='utf-8') as f:
        index = -1              # 主键在metaTable中的索引
        file = csv.reader(f)
        for item in file:
            index = index + 1
            if item[2] == str(True):
                return item[0], index

    return None


# 检查table是否存在
def isTableExist(tableName):
    if os.path.exists(meta_path + tableName + ".csv"):
        if os.path.exists(data_path + tableName + ".csv"):
            return True
    else:
        return False


# 检查字段名是否存在
def checkColumn(tableName, columns):
    fields = getColumns(tableName)
    # 检查字段是否存在
    for column in columns:
        if column not in fields:
            return False, column

    return True, ''


# 返回元数据表中的field
def getColumns(tableName):
    # 查找元数据表 返回Fields
    fields = []
    with open(meta_path + tableName + ".csv", 'r', encoding='utf-8') as f:
        file = csv.reader(f)
        for item in file:
                fields.append(item[0])

    return fields


# 根据columns，找到其在元数据表中对应的下标
def getIndexList(tableName, columns):
    indexList = []
    with open(meta_path + tableName + ".csv", 'r', encoding='utf-8') as f:
        file = csv.reader(f)
        index = -1
        for item in file:
            index = index + 1
            if item[0] in columns:
                indexList.append(index)
    return indexList


# 检查主键更新值有无重复
def checkRepeat(tableName, primary_index, value):
    with open(data_path + tableName + ".csv", 'r', encoding='utf-8') as f:
        file = csv.reader(f)
        for item in file:
            if item[primary_index] == value:
                return True     # 有重复

    return False


# 找到满足过滤条件的行数
def findMatchRow(dataList, filter_value, filter_index, param):
    row = -1
    rows = []
    for data in dataList:
        row = row + 1
        if param == '!=':
            if data[filter_index] != filter_value:
                rows.append(row)
        elif param == '>=':
            if data[filter_index] >= filter_value:
                rows.append(row)
        elif param == '<=':
            if data[filter_index] <= filter_value:
                rows.append(row)
        elif param == '>':
            if data[filter_index] > filter_value:
                rows.append(row)
        elif param == '<':
            if data[filter_index] < filter_value:
                rows.append(row)
        elif param == '=':
            if data[filter_index] == filter_value:
                rows.append(row)

    return rows