import os
from sqlalchemy  import create_engine
import logging
import pandas as pd

import config
class PostgreDB :

    _db_url = 'postgres://postgres:postgres@6bills.iptime.org/postgres'
    _db_connection = None

    def __init__(self):
        # 'postgres://tkrapi:tkrapi@127.0.0.1/tkrapi'
        db_host = config.DATABASE_CONFIG['host']
        db_user = config.DATABASE_CONFIG['user']
        db_pwd = config.DATABASE_CONFIG['password']
        db_name = config.DATABASE_CONFIG['name']
        db_url = 'postgres://' + db_user + ":" + db_pwd + "@" + db_host +"/" + db_name

        self._db_url = db_url
        self._db_connection = create_engine(self._db_url).connect()

    # @staticmethod
    def get_conn(self) :
        return self._db_connection

    def query(self, sql, param=[]):
        result = self.get_conn().execute(sql, param)
        # idx = 0
        # for row in result:
        #     logging.debug('[%i] : %s', idx, row)
        # return result
        return result

    def __del__(self):
        self._db_connection.close()

    def script_execution(self, file_name):
        with open(file_name, 'r') as f :
            sqlScript = []
            sqlScript = f.read()
            for statement in sqlScript.split(';'):
                self.get_conn().execute(statement)
                # with self.get_conn().cursor() as cur:
                #     cur.execute(statement)

        # with open(file_name, 'r') as f :
        #     sql_script = f.read()
        #     self.get_conn().execute(sql_script)
        #f.closed

if __name__ == "__main__":
    db = PostgreDB()

    # sql_s = "insert into test_table(code, name)values('doni','doni')"
    # db.query(sql_s)

    sql = '''SELECT O.*, S.NAME, S.TYPE 
      FROM (
            select t1.code, t1.date, t1.open, t1.high, t1.low,t1.close,t1.volume 
                    ,(t1.close-t2.close) diff, t2.close last_close
                    ,(t1.close-t2.close)/t2.close*100 as per
            from (SELECT *, ROW_NUMBER() OVER (ORDER BY code, date ASC) as no FROM ohlc) t1,
                (SELECT *, ROW_NUMBER() OVER (ORDER BY code, date ASC) as no FROM ohlc) t2
            where t1.code = t2.code
            and t1.no = t2.no+1
           )O
      LEFT OUTER JOIN 
            STOCK_COMPANY S
        ON O.CODE = S.CODE
     WHERE TO_DATE(DATE, 'YYYYMMDD') >= TO_DATE('20180510', 'YYYYMMDD')
       AND TO_DATE(DATE, 'YYYYMMDD') <= TO_DATE('20180525', 'YYYYMMDD')
       AND O.PER >= 15
       AND O.PER <= 30
    ORDER BY CODE, DATE
    '''

    conn = db.get_conn()
    #result = pd.read_sql("select count(*) from stock_company", conn)
    result = pd.read_sql(sql, conn)
    print(result)