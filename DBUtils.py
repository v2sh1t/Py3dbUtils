import sqlite3

import pymssql


class SQLObject(object):

    def GetConnect(self):
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
        """
        获取数据库连接
        :return: cursor & connection
        """
        if not self.db:
            raise (NameError, "没有设置数据库信息")
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return cur, self.conn

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
            return cur, self.conn

    def getRowNColCount(self, result):
        """
        获取行数和列数
        :param result: 数据库查询的结果
        :return: 行数，列数
        """
        row = len(result)
        col = len(result[0])
        return row, col
