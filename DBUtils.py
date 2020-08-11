import sqlite3

import pymssql


class SQLObject(object):

    def __init__(self) -> None:
        super().__init__()

    def GetConnect(self):
        pass

    def CreateTable(self, table_name, col_prop_list):
        pass

    def ExecScript(self, sql_script):
        pass
    
    def ExecInsert(self, table_name, col_list, value_list):
        pass

    def ExecQuery(self, col_list, table_name, condition=None):
        pass

    def ExecUpdate(self, table_name, modify_content, condition):
        pass

    def ExecDelete(self, table_name, condition):
        pass

    def GetClose(self):
        pass

    def getRowNColCount(self, result):
        pass


class MSSQL(SQLObject):

    def __init__(self, host, user, pwd, db, charset="utf8"):

        """
        初始化数据库连接
        :param host: 服务器地址
        :param user: 用户名
        :param pwd: 密码
        :param db: 数据库名
        :param charset: 字符编码
        """

        super(MSSQL, self).__init__()
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.charset = charset
        self.conn = pymssql.connect(host=self.host, user=self.user, password=self.pwd, database=self.db,
                                    charset=self.charset)

    def GetConnect(self):
        if not self.db:
            raise (NameError, "没有设置数据库信息")
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return cur

    def CreateTable(self, table_name, col_prop_list):
        """
        创建表
        :param table_name: 表名
        :param col_prop_list: 字段名及属性
        :return:
        """
        cur = self.GetConnect()
        sql = f'create table if not exists {table_name} ({", ".join(col_prop_list)})'
        cur.execute(sql)
        self.conn.commit()
        # self.conn.close()
       
    def ExecScript(self, sql_script):
        """
        执行sql语句
        :param sql_script: sql语句
        :return res_list: 结果列表
        """
        cur = self.GetConnect()
        cur.execute(sql_script)
        res_list = cur.fetchall()
        return res_list
    
    def ExecQuery(self, col_list, table_name, condition=None):
        """
        查询数据
        :param col_list: 字段名
        :param table_name: 表名
        :param condition: 查询条件
        :return:
        """
        cur = self.GetConnect()
        if isinstance(col_list, list):
            s = ','.join(col_list)
        else:
            s = col_list
        sql = f'select {s} from {table_name} {condition}'
        cur.execute(sql)
        res_list = cur.fetchall()
        # 查询完毕后必须关闭连接
        # self.conn.close()
        return res_list

    def ExecInsert(self, table_name, col_list, value_list):
        """
        新增数据
        :param table_name: 表名
        :param col_list: 字段名
        :param value_list: 字段名对应的值
        :return:
        """
        cur = self.GetConnect()
        sql = f'''insert into {table_name} ({",".join(col_list)}) VALUES ('{"','".join(value_list)}')'''
        cur.execute(sql)
        self.conn.commit()
        # self.conn.close()

    def ExecUpdate(self, table_name, modify_content, condition):
        """
        修改数据
        :param table_name: 表名
        :param modify_content: 修改内容
        :param condition: 查询条件
        :return:
        """
        cur = self.GetConnect()
        sql = f'update {table_name} set {modify_content} {condition}'
        cur.execute(sql)
        self.conn.commit()
        # self.conn.close()

    def ExecDelete(self, table_name, condition):
        """
        删除数据
        :param table_name: 表名
        :param condition: 查询条件
        :return:
        """
        cur = self.GetConnect()
        sql = f'delete from {table_name} where {condition}'
        cur.execute(sql)
        self.conn.commit()
        # self.conn.close()

    def GetClose(self):
        """
        关闭连接
        :return:
        """
        self.conn.close()

    def getRowNColCount(self, result):
        """
        获取行数和列数
        :param result: 数据库查询的结果
        :return: 行数，列数
        """
        row = len(result)
        col = len(result[0])
        return row, col


class SQLITE(SQLObject):

    def __init__(self, db):
        """
        初始化数据库连接
        :param db: 数据库名
        """
        super(SQLITE, self).__init__()
        self.db = db
        self.conn = sqlite3.connect(database=self.db)

    def GetConnect(self):
        if not self.db:
            raise (NameError, "没有设置数据库信息")
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return cur

    def CreateTable(self, table_name, col_prop_list):
        """
        创建表
        :param table_name: 表名
        :param col_prop_list: 字段名及属性
        :return:
        """
        cur = self.GetConnect()
        sql = f'create table {table_name} ({",".join(col_prop_list)})'
        cur.execute(sql)
        self.conn.commit()
        # self.conn.close()
    
    def ExecScript(self, sql_script):
        """
        执行sql语句
        :param sql_script: sql语句
        :return res_list: 结果列表
        """
        cur = self.GetConnect()
        cur.execute(sql_script)
        res_list = cur.fetchall()
        return res_list

    def ExecQuery(self, col_list, table_name, condition: str = None):
        """
        查询数据
        :param col_list: 字段名
        :param table_name: 表名
        :param condition: 查询条件
        :return:
        """
        cur = self.GetConnect()
        if isinstance(col_list, list):
            s = ','.join(col_list)
        else:
            s = col_list
        sql = f'select {s} from {table_name} {condition}'
        result_list = cur.execute(sql).fetchall()
        # self.conn.close()
        return result_list

    def ExecInsert(self, table_name, col_list, value_list):
        """
        新增数据
        :param table_name: 表名
        :param col_list: 字段名
        :param value_list: 字段名对应的值
        :return:
        """
        cur = self.GetConnect()
        sql = f'''insert into {table_name} ({",".join(col_list)}) VALUES ('{"','".join(value_list)}')'''
        cur.execute(sql)
        self.conn.commit()
        # self.conn.close()

    def ExecUpdate(self, table_name, modify_content, condition: str = None):
        """
        更新数据
        :param table_name: 表名
        :param modify_content: 修改内容
        :param condition: 查询条件
        :return:
        """
        cur = self.GetConnect()
        sql = f'update {table_name} set {modify_content} {condition}'
        cur.execute(sql)
        self.conn.commit()
        # self.conn.close()

    def ExecDelete(self, table_name, condition: str = None):
        """
        删除数据
        :param table_name: 表名
        :param condition: 查询条件
        :return:
        """
        cur = self.GetConnect()
        sql = f'delete from {table_name} where {condition}'
        cur.execute(sql)
        self.conn.commit()
        # self.conn.close()

    def GetClose(self):
        """
        关闭连接
        :return:
        """
        self.conn.close()

    def getRowNColCount(self, result):
        """
        获取行数和列数
        :param result: 数据库查询的结果
        :return: 行数，列数
        """
        row = len(result)
        col = len(result[0])
        return row, col
