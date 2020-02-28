
from Bka.mysqlset import xiemysql
import pymssql
import pymysql.cursors
import pymysql
import datetime


setsmysql = xiemysql()

class xiemysql:
    def __init__(self):
        self.host =setsmysql.host[0]
        self.user =setsmysql. user[0]
        self.password =setsmysql.password[0]
        self.dbs = setsmysql.db[0]
        self.charset = setsmysql.charset[0]

    def connections(self):
        connection = pymssql.connect(host=self.host,

                                     user=self.user,
                                     password =self.password,
                                     database= self.dbs,
                                     charset=self.charset,
                                   )
        return connection