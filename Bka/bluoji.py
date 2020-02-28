import os
import pickle
from pickle import load
import pandas as pd
import numpy as np
from Bka.huoqushuju import shouyuqishu
from Bka.mysqls import xiemysql
from Bka.middles import Pbc

xiemy = xiemysql()
conn = xiemy.connections()
#
# df_all = shouyuqishu(conn)
#
sql = ("SELECT top 10* FROM(  SELECT	*,	ROW_NUMBER () OVER ( PARTITION BY order_num ORDER BY instalment ) as row FROM \
(SELECT order_num,instalment,convert(datetime,repayment_date,120) yinghuan,   \
 convert(datetime,repayment_date2,120) shihuan \
FROM [dbo].[settlement_t_n_repayment_plan] \
WHERE repayment_state in ('0003','0004','0005') AND delete_status = 'N') r) a  WHERE a.row = 1 ")

### 首逾期数
sql2 = "SELECT * FROM( SELECT	*, ROW_NUMBER () OVER ( PARTITION BY repayment_num ORDER BY instalment ) as row \
FROM ( SELECT c.cus_name,c.tel_phone,c.id_number,a.repayment_num,a.order_num,a.instalment,convert(datetime,a.repayment_date,120) yinghuan,\
convert(datetime,a.repayment_date2,120) shihuan,b.day_start as 交车日期,CASE b.status WHEN  '0001' THEN '还款中' WHEN  '0002' THEN'正常结清' WHEN  '0003'  THEN' 提起结清' WHEN   '0004' THEN  '处置结清' WHEN  '0005' THEN '核销结清' \
WHEN  '0006' THEN '逾期' WHEN  '0007' THEN '违约终止' WHEN  '0008' THEN '合同损失' WHEN  '0009' THEN '转让终止' WHEN  '0010' THEN '置换终止' \
WHEN  '0011' THEN	'期满退车' 	WHEN  '0012' THEN	'损失终止' END AS '合同状态'   \
FROM [dbo].[settlement_t_n_repayment_plan] a LEFT JOIN [dbo].[settlement_t_n_contract] b           \
ON a.repayment_num = b.repayment_num  INNER JOIN [dbo].[order_detail_original] as c ON b.repayment_num = c.paylist_code \
WHERE a.repayment_state in ('0003','0004','0005') \
AND a.delete_status = 'N' AND b.status IN ('0001','0006','0008')  \
) r) a WHERE a.row = 1"

# 融资金额、首付比、实收首付金额、尾付金额比例
sql3 =  "SELECT T1.*,T2.contract_num,T2.合同状态,T3.* FROM     \
(SELECT                                                \
      [order_num]             \
      ,[financing_amount]        \
      ,[indeed_down_payment_amount]  \
      ,[indeed_final_payment_amount]  \
      ,[final_payment_scale]  \
  FROM [HSBIData].[dbo].[settlement_t_n_contract_financial]  \
  where delete_status = 'N') T1   \
  LEFT JOIN    \
  (SELECT  order_num,   \
  [contract_num]   \
      ,[status]   \
          ,case when status = '0001' then '还款中'  \
                        when status = '0002' then '正常结清'  \
                        when status = '0003' then '提前结清'  \
                        when status = '0004' then '处置结清'  \
                        when status = '0005' then '核销结清'  \
                        when status = '0006' then '逾期'   \
                        when status = '0007' then '违约终止'  \
                        when status = '0008' then '合同损失'  \
                        when status = '0009' then '转让终止'  \
                        when status = '0010' then '置换终止'  \
                        when status = '0011' then '期满退车'  \
                        when status = '0012' then '损失终止'  \
                end as 合同状态   \
  FROM [HSBIData].[dbo].[settlement_t_n_contract]   \
  WHERE delete_status = 'N'  ) T2  \
  ON T2.order_num= T1.order_num   \
  LEFT JOIN  \
  (SELECT   \
      [lease_code]       \
      ,[paylist_code]   \
      ,[tel_phone]  \
      ,[id_number]  \
  FROM [HSBIData].[dbo].[order_detail_original]) T3  \
  ON T3.lease_code = T2.contract_num  \
 WHERE T2.status IN  ('0001','0006','0008') "

## 剩余金额

sql4 =  "SELECT * FROM ( SELECT 	*,	ROW_NUMBER () OVER ( PARTITION BY repayment_num ORDER BY repayment_num ) as row  \
FROM ( SELECT d.cus_name, d.tel_phone, d.id_number,c.repayment_num,a.order_num,b.financing_amount,a.actual_month_repay, \
b.financing_amount-a.actual_month_repay as 剩余金额,c.day_start as 交车日期,CASE c.status WHEN  '0001' THEN  \
'还款中' WHEN  '0002' THEN '正常结清' WHEN  '0003' THEN '提起结清'  WHEN  '0004' THEN '处置结清' WHEN  '0005' THEN  \
	'核销结清' 	WHEN  '0006' THEN '逾期'  WHEN  '0007' THEN '违约终止'  WHEN  '0008' THEN'合同损失'    \
WHEN  '0009' THEN '转让终止' WHEN  '0010' THEN      \
'置换终止' WHEN  '0011' THEN '期满退车' WHEN  '0012' THEN '损失终止'  \
END AS '合同状态' FROM ( SELECT order_num,sum(indeed_repayment_month) as actual_month_repay  \
FROM [dbo].[settlement_t_n_repayment_plan] WHERE delete_status = 'N'  GROUP BY \
order_num ) a LEFT JOIN [dbo].[settlement_t_n_contract_financial] b ON a.order_num = b.order_num  \
LEFT JOIN [dbo].[settlement_t_n_contract] c ON b.order_num = c.order_num  \
LEFT JOIN [dbo].[order_detail_original] d ON c.order_num = d.fin_order_no WHERE  \
 b.delete_status = 'N' AND c.status IN ('0001','0006','0008') ) a ) b WHERE b.row = 1"

## 门店所在省
sql5 =  "SELECT c.cus_name, c.tel_phone, c.id_number, b.repayment_num, b.order_num, a.area, \
 a.province,a.city,b.day_start as 交车日期,CASE b.status WHEN  '0001' THEN '还款中'  \
WHEN  '0002' THEN '正常结清' WHEN  '0003' THEN '提起结清' WHEN  '0004' THEN   \
'处置结清' WHEN  '0005' THEN '核销结清' WHEN  '0006' THEN '逾期' WHEN  '0007' THEN \
'违约终止' WHEN  '0008' THEN '合同损失' WHEN  '0009' THEN '转让终止' WHEN  '0010' THEN \
'置换终止' WHEN  '0011' THEN '期满退车' WHEN  '0012' THEN '损失终止'   \
END AS '合同状态' FROM [HSBIData].[dbo].[settlement_t_n_contract] b INNER JOIN\
[HSBIData].[dbo].[order_detail_original] c ON b.order_num = c.fin_order_no   \
  LEFT JOIN	 [H_RISKMG_DB].[dbo].[Data_store_info_new] a ON          \
	a.before_store = b.shop_name WHERE b.delete_status = 'N' AND b.status IN ('0001','0006','0008') "

### 获取pboc的数据
slq6 = "SELECT c.client_response_info FROM credit_gateway_t_hshc_credit_crecord b LEFT JOIN credit_gateway_t_hshc_credit_crecord_detail c \
ON b.record_no = c.record_no WHERE b.service_name = 'credit_single_query' AND b.is_violent=1  AND b.status = 1 "




# df1 = df_all.shouyu(sql)
# print(df1)
class xgb1:
    def __init__(self,curname = os.path.dirname(os.path.abspath(__file__))):
        self.cur_dir= curname
    def xgb(self):
        self.model_path = os.path.join(self.cur_dir, "alg.pickle")
        self.xgb = load(open(self.model_path, "rb"))

        return self.xgb
    def xgb1(self):
        model_path = os.path.join(self.cur_dir, "alg1.pickle")
        xgb = load(open(model_path, "rb"))

        return xgb
    def dfmerge_yw(self):
        # xiemy = xiemysql()
        # conn = xiemy.connections()

        ## 首逾期数
        df2 = pd.read_sql(sql2, con=conn)

        # 融资金额、首付比、实收首付金额、尾付金额比例
        df3 = pd.read_sql(sql3, con=conn)

        ## 剩余金额
        df4 = pd.read_sql(sql4, con=conn)

        ## 门店所在省
        df5 = pd.read_sql(sql5, con=conn)

        q3 = pd.merge(df3,df4,on = ['order_num','tel_phone', 'id_number','合同状态','financing_amount'],how = 'left')

        q4 = pd.merge(q3, df5, on=['cus_name', 'tel_phone', 'id_number', 'repayment_num', 'order_num', '交车日期', '合同状态'],
                      how='left')
        ## 合并首逾
        q5 = pd.merge(q4, df2,
                      on=['cus_name', 'tel_phone', 'id_number', 'repayment_num', 'order_num', '交车日期', '合同状态', 'row'],
                      how='left')
        return df2,q4
    def dfmerge_pboc(self):
        # xiemy = xiemysql()
        # conn = xiemy.connections()
        pboc_df = pd.read_sql(slq6,con = conn)
        data = list(pboc_df['client_response_info'])
        pboc = Pbc(data)
        df1 = pboc.debat()
        df2 = pboc.peopleinfo()
        df3 = pboc.work()
        pboc_one = pd.merge(df1,df2, on=['queryCredNum', 'queryName'], how='left')

        pboc_two = pd.merge(pboc_one, df3, on=['queryCredNum', 'queryName'], how='left')
        pboc_two.rename(columns={'queryCredNum': 'id_number', 'queryName': 'cus_name'}, inplace=True)
        return pboc_two

    def dizhi(self,df):
        if pd.isnull(df['province']):
            dz = 0
        else:
            if pd.notnull(df['resideAddr']) and pd.notnull(df['residenceAddress']):
                if str(df['province'])[0:2] == str(df['resideAddr'])[0:2] and str(df['province'])[0:2] == str(
                        df['residenceAddress'])[0:2]:
                    dz = 1
                elif str(df['province'])[0:2] != str(df['resideAddr'])[0:2] and str(df['province'])[0:2] == str(
                        df['residenceAddress'])[0:2]:
                    dz = 2
                elif str(df['province'])[0:2] == str(df['resideAddr'])[0:2] and str(df['province'])[0:2] != str(
                        df['residenceAddress'])[0:2]:
                    dz = 3
                elif str(df['province'])[0:2] != str(df['resideAddr'])[0:2] and str(df['province'])[0:2] != str(
                        df['residenceAddress'])[0:2] and \
                        str(df['resideAddr'])[0:2] == str(df['residenceAddress'])[0:2]:
                    dz = 4
                elif str(df['province'])[0:2] != str(df['resideAddr'])[0:2] and str(df['province'])[0:2] != str(
                        df['residenceAddress'])[0:2] and \
                        str(df['resideAddr'])[0:2] != str(df['residenceAddress'])[0:2]:
                    dz = 5
                else:
                    dz = 6
            elif pd.notnull(df['resideAddr']) and pd.isnull(df['residenceAddress']):
                if str(df['province'])[0:2] == str(df['resideAddr']):
                    dz = 7
                else:
                    dz = 8
            elif pd.isnull(df['resideAddr']) and pd.notnull(df['residenceAddress']):
                if str(df['province'])[0:2] == str(df['residenceAddress'])[0:2]:
                    dz = 9
                else:
                    dz = 10
            else:
                dz = 11
        return dz




