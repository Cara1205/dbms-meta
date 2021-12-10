import csv
import re
from . import utils

meta_path = "database/metas/"   # 存放 元数据表 位置
data_path = "database/datas/"   # 存放 数据表 位置

def selectData(sql):
    # 无wherr子句的解析
    if 'where' not in sql:
        searchObj = re.search(r'^select (.*) from (.*);$', sql, re.I)
        if searchObj:
            tableName = searchObj.group(2).rstrip()
            columns = searchObj.group(1).split(', ')
            columns = [column.lstrip().rstrip() for column in columns]

            # 判断table是否存在
            is_table_exist = utils.isTableExist(tableName)
            if not is_table_exist:
                print("表" + tableName + "不存在或无数据")
                print("TABLE \'" + tableName + "\' NOT EXISTS\n")
                return
            else:
                dataList, columns = SimpleSelect(tableName, columns)
                if len(dataList) == 0:
                    print('0 rows affected\n')
                    return
                else:
                    printTable(columns, dataList)
                    print( str(len(dataList)) + ' rows affected\n')
                    return

        else:  # 正则匹配失败
            print("SQL 解析失败")
            return

    # 有where子句的解析
    else:
        searchObj = re.search(r'^select (.*) from (.*) where (.*);$', sql, re.I)
        if searchObj:
            tableName = searchObj.group(2).strip()
            columns = searchObj.group(1).split(', ')
            conditions = searchObj.group(3).strip()

            # 判断table是否存在
            is_table_exist = utils.isTableExist(tableName)
            if not is_table_exist:
                print("表" + tableName + "不存在或无数据")
                print("TABLE \'" + tableName + "\' NOT EXISTS\n")
                return
            else:
                if 'and' not in conditions and 'or' not in conditions:
                    # 根据where子句内容select
                    dataList, columns = SimpleSelectCondition(tableName, columns, conditions)
                    if len(dataList) == 0:
                        print('0 rows affected\n')
                    else:
                        printTable(columns, dataList)
                        print(str(len(dataList)) + ' rows affected\n')
                elif 'and' in conditions:
                    conditions = conditions.split('and ')
                    conditions[0] = conditions[0].strip()
                    conditions[1] = conditions[1].strip()
                    dataList1 = SimpleSelectCondition(tableName, columns, conditions[0])
                    dataList2 = SimpleSelectCondition(tableName, columns, conditions[1])
                    # 取交
                    dataList = [data for data in dataList1 if data in dataList2]
                    if len(dataList) == 0:
                        print('0 rows affected\n')
                    else:
                        printTable(columns, dataList)
                        print(str(len(dataList)) + ' rows affected\n')
                elif 'or' in conditions:
                    conditions = conditions.split('and ')
                    conditions[0] = conditions[0].strip()
                    conditions[1] = conditions[1].strip()
                    dataList1 = SimpleSelectCondition(tableName, columns, conditions[0])
                    dataList2 = SimpleSelectCondition(tableName, columns, conditions[1])
                    # 取并
                    dataList = [data for data in dataList1] + [data for data in dataList2 if data not in dataList1]
                    if len(dataList) == 0:
                        print('0 rows affected\n')
                        return
                    else:
                        printTable(columns, dataList)
                        print(str(len(dataList)) + ' rows affected\n')
                        return
                else:
                    print("功能暂未实现\n")
                    return


def SimpleSelect(tableName, columns):
    dataList = []   # 存放查询的数据
    if '*' in columns:
        dataList = utils.getDataList(tableName)
        columns = utils.getColumns(tableName) # 当columns为*时，返回为字段名
        return dataList, columns

    else:
        # 检查是否有不存在的字段名
        colomn_available, column = utils.checkColumn(tableName, columns)
        if not colomn_available:
            print("列 \'" + column + "\' 不存在")
            print("COLUMN \'" + column + "\' NOT EXIST\n")
            return [], []

        # 从元数据表中找到 所需columns的下标
        indexList = utils.getIndexList(tableName, columns)

        # 从数据表中读取数据
        with open(data_path + tableName + ".csv", 'r', encoding='utf-8') as f:
            file = csv.reader(f)
            for item in file:
                data = []
                for i in indexList:
                    data.append(item[i])
                dataList.append(data)

        return dataList, columns


def printTable(columns, dataList):
    for column in columns:
        print("%8s " %(column), end='')
    print()
    for data in dataList:
        for item in data:
            print("%8s" %(item), end='')
        print()
    print()


def SimpleSelectCondition(tableName, columns, conditions):
    dataList = []

    if '*' in columns:
        columns = utils.getColumns(tableName)
    else:
        # 检查是否有不存在的字段名
        colomn_available, column = utils.checkColumn(tableName, columns)
        if not colomn_available:
            print("列 \'" + column + "\' 不存在")
            print("COLUMN \'" + column + "\' NOT EXIST\n")
            return [], []

    # 读入所有数据
    tmpDataList = utils.getDataList(tableName)

    if '!=' in conditions:
        filters = conditions.split('!=')
        filters[0] = filters[0].strip()
        filters[1] = filters[1].strip()
        tmpDataList = getFilterData(tableName, tmpDataList, filters, '!=')
    elif '>=' in conditions:
        filters = conditions.split('>=')
        filters[0] = filters[0].strip()
        filters[1] = filters[1].strip()
        tmpDataList = getFilterData(tableName, tmpDataList, filters, '>=')
    elif '<=' in conditions:
        filters = conditions.split('<=')
        filters[0] = filters[0].strip()
        filters[1] = filters[1].strip()
        tmpDataList = getFilterData(tableName, tmpDataList, filters, '<=')
    elif '>' in conditions:
        filters = conditions.split('>')
        filters[0] = filters[0].strip()
        filters[1] = filters[1].strip()
        tmpDataList = getFilterData(tableName, tmpDataList, filters, '>')
    elif '<' in conditions:
        filters = conditions.split('<')
        filters[0] = filters[0].strip()
        filters[1] = filters[1].strip()
        tmpDataList = getFilterData(tableName, tmpDataList, filters, '<')
    elif '=' in conditions:
        filters = conditions.split('=')
        filters[0] = filters[0].strip()
        filters[1] = filters[1].strip()
        tmpDataList = getFilterData(tableName, tmpDataList, filters, '=')
    elif conditions[0] == str(False):
        tmpDataList = []
    elif conditions[0] == str(True):
        tmpDataList = tmpDataList
    else:
        print("SQL语句解析失败")
        return

    # 返回column对应列的数据
    indexList = utils.getIndexList(tableName, columns)
    for data in tmpDataList:
        row = []
        for i in indexList:
            row.append(data[i])
        dataList.append(row)

    return dataList, columns


def getFilterData(tableName, dataList, filters, param):
    filter_data = []
    if param == '!=':
        filter_index = utils.getIndexList(tableName, filters[0])
        # print("dataList: ", dataList)
        for data in dataList:
            if not data[filter_index[0]] == filters[1]:
                filter_data.append(data)
        return filter_data
    elif param == '>=':
        filter_index = utils.getIndexList(tableName, filters[0])
        for data in dataList:
            if data[filter_index[0]] >= filters[1]:
                filter_data.append(data)
        return filter_data
    elif param == '<=':
        filter_index = utils.getIndexList(tableName, filters[0])
        for data in dataList:
            if data[filter_index[0]] <= filters[1]:
                filter_data.append(data)
        return filter_data
    elif param == '>':
        filter_index = utils.getIndexList(tableName, filters[0])
        for data in dataList:
            if data[filter_index[0]] > filters[1]:
                filter_data.append(data)
        return filter_data
    elif param == '<':
        filter_index = utils.getIndexList(tableName, filters[0])
        for data in dataList:
            if data[filter_index[0]] < filters[1]:
                filter_data.append(data)
        return filter_data
    elif param == '=':
        filter_index = utils.getIndexList(tableName, filters[0])
        for data in dataList:
            if data[filter_index[0]] == filters[1]:
                filter_data.append(data)
        return filter_data
    else:
        return dataList


# select * from test;
# select * from test where id != 1111;
# select id, name from test where id = 1111;
# select * from test where age > 18;

# select * from aaa;    # 检查：不存在的表
# select aaa from test; # 检查：不存在的字段

# select * from student;
# select id, name, major from student where id = 20210001;
# select aaa from student;