
import pandas as pd
from Bka.mysqls import xiemysql

xiemy = xiemysql()
conn = xiemy.connections()



class shouyuqishu:
    def __init__(self,con = conn):
        self.con = con
    def shouyu(self,sql):
        df = pd.read_sql(sql, con=self.con)
        return df
    ###融资金额、首付比、实收首付金额、尾付金额比例
    def rongzi(self,sql):
        df1 = pd.read_sql(sql, con=self.con)
        return df1
    ## --剩余金额
    def shengyu(self,sql):
        df2 = pd.read_sql(sql, con=self.con)
        return df2
    ### 是否有信用卡
    def xinyongka(self,sql):
        df3 = pd.read_sql(sql, con=self.con)
        return df3
    ### 门店所在省
    def shengfen(self,sql):
        df4 = pd.read_sql(sql, con=self.con)
        return df4


#
# sql2 = "SELECT * FROM( SELECT	*, ROW_NUMBER () OVER ( PARTITION BY repayment_num ORDER BY instalment ) as row \
# FROM ( SELECT c.cus_name,c.tel_phone,c.id_number,a.repayment_num,a.order_num,a.instalment,convert(datetime,a.repayment_date,120) yinghuan,\
# convert(datetime,a.repayment_date2,120) shihuan,b.day_start as 交车日期,CASE b.status WHEN  '0001' THEN '还款中' WHEN  '0002' THEN'正常结清' WHEN  '0003'  THEN' 提起结清' WHEN   '0004' THEN  '处置结清' WHEN  '0005' THEN '核销结清' \
# WHEN  '0006' THEN '逾期' WHEN  '0007' THEN '违约终止' WHEN  '0008' THEN '合同损失' WHEN  '0009' THEN '转让终止' WHEN  '0010' THEN '置换终止' \
# WHEN  '0011' THEN	'期满退车' 	WHEN  '0012' THEN	'损失终止' END AS '合同状态'   \
# FROM [dbo].[settlement_t_n_repayment_plan] a LEFT JOIN [dbo].[settlement_t_n_contract] b           \
# ON a.repayment_num = b.repayment_num  INNER JOIN [dbo].[order_detail_original] as c ON b.repayment_num = c.paylist_code \
# WHERE a.repayment_state in ('0003','0004','0005') \
# AND a.delete_status = 'N'  \
# ) r) a WHERE a.row = 1"
#
#
# df = pd.read_sql(sql2,con = conn)
# print(df)