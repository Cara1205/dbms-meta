import csv
"""
    元数据     Meta
    元数据表   MetaTable
    数据行     Row
    数据表     DataTable
"""

meta_path = "database/metas/"   # 存放 元数据表 位置
data_path = "database/datas/"   # 存放 数据表 位置

class Meta:
    def __init__(self, field, type, isPrimary=False, isNotNull=False, default=None):
        self.Field = field
        self.Type = type
        self.isPrimary = isPrimary
        self.isNotNull = isNotNull
        self.Default = default
        if default == None:
            self.fillZeroValue()

    def getMeta(self):
        meta = [
            self.Field,
            self.Type,
            self.isPrimary,
            self.isNotNull,
            self.Default
        ]
        return meta

    def fillZeroValue(self):
        if 'float' in self.Type:
            self.Default = 0.0
        elif 'int' in self.Type:
            self.Default = 0
        else:
            self.Default = ''


class MetaTable:
    def __init__(self, tableName, metas=None):
        self.tableName = tableName
        if metas == None:
            self.metas = []
        else:
            self.metas = metas

    def getMetas(self):
        return self.metas

    def appendMeta(self, meta):
        self.metas.append(meta)

    @staticmethod
    def load(tableName):
        # 从文件中加载metaTable
        metas = []
        with open(meta_path + tableName + ".csv", 'r', encoding='utf-8') as f:
            file = csv.reader(f)
            for item in file:
                meta = Meta(*item)
                metas.append(meta)

        return MetaTable(tableName, metas)


class Row:
    def __init__(self, metaTable, datas):
        dict = {}
        for data in datas:
            # data为zip在一起的元组，data[0]为字段名，data[1]为值
            dict[data[0]] = data[1]

        # 补齐全部字段的值
        self.datas = []
        metas = metaTable.getMetas()
        for meta in metas:
            if meta.Field in dict.keys():
                # 如果insert语句提供了，则使用给的值
                self.datas.append(dict[meta.Field])
            else:   # 否则填充零值
                self.datas.append(meta.Default)
        # print(self.datas)

    def getRow(self):
        return self.datas


class DataTable:
    def __init__(self, tableName, rows=None):
        self.tableName = tableName
        if rows == None:
            self.rows = []
        else:
            self.rows = rows


    @staticmethod
    def load(tableName):
        # 从文件中加载dataTable
        rows = []
        with open(data_path + tableName + ".csv", 'r', encoding='utf-8') as f:
            file = csv.reader(f)
            for item in file:
                row = Row(*item)
                rows.append(row)

        print("datas: ", rows)
        return DataTable(tableName, rows)