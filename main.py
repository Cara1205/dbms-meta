from modules import Create
from modules import Insert
from modules import Select
from modules import Update
from modules import Delete

if __name__ == '__main__':
    create_sql = 'create table '
    insert_sql = 'insert into '
    select_sql = 'select '
    updata_sql = 'update '
    delete_sql = 'delete from'
    # help_sql = 'help table '
    print("20184393 张馨月\t20184392 王心怡\t数据库Project")
    print("--------  使用说明  --------")
    print("· create table\t\t创建新表，可设置主键、非空约束\n"
          "· insert into\t\t插入数据\n"
          "· select\t\t查询数据\n"
          "· update\t\t更新数据\n"
          "· delete from\t\t删除数据\n")
    print("注：使用方法与MySQL大体一致。（必须带;号）\n具体语句可查看每个模块的py文件末尾 # 后的示例代码")


    while True:
        sql = input('>')
        if sql.lstrip().startswith(create_sql):
            Create.createTable(sql)
        elif sql.lstrip().startswith(insert_sql):
            Insert.insertData(sql)
        elif sql.lstrip().startswith(select_sql):
            Select.selectData(sql)
        elif sql.lstrip().startswith(updata_sql):
            Update.updateData(sql)
        elif sql.lstrip().startswith(delete_sql):
            Delete.deleteData(sql)
        else:
            print("SQL解析错误，暂时没有这条语句\n")