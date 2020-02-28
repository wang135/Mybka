import numpy as np
import pandas as pd
from Bka.bluoji import xgb1
from Bka.middles import Score
import matplotlib.pyplot as plt

from Bka.middles import Score
from Bka.bluoji import xgb1

import matplotlib.pyplot as plt
fig, ax = plt.subplots()
df_all = xgb1()
score = Score()
import pandas as pd
fig, ax = plt.subplots()
df_all = xgb1()
#
df2,q4 = df_all.dfmerge_yw()
# print(df_yw)
df_pboc = df_all.dfmerge_pboc()
# all_hb = pd.merge(df_pboc,df_yw,on=['id_number','cus_name'])
all_hb = pd.merge(df_pboc,q4,on=['id_number','cus_name'],how ='right')
print(all_hb)

all_hb = all_hb.drop_duplicates('id_number')
all_hb['dz'] = all_hb.apply(lambda x:df_all.dizhi(x),axis=1)
all_hb['首付比'] =all_hb['indeed_down_payment_amount']/ all_hb['financing_amount']
all_hbss = all_hb[['id_number','cus_name','financing_amount','首付比','indeed_down_payment_amount','indeed_final_payment_amount','剩余金额','是否有信贷','dz']]

all_hbss.rename(columns={'financing_amount': '融资金额','indeed_down_payment_amount':'实收首付金额',
                         'indeed_final_payment_amount':'尾付金额比例','是否有信贷': '是否有贷款或信用卡'
                         }, inplace=True)

# print(all_hbss)
# df_xgb = all_hbss[['融资金额', '首付比', '实收首付金额', '尾付金额比例', '剩余金额', '是否有贷款或信用卡', 'dz']]
df = score.predict(all_hbss)

## 把首逾加进去
rowws = df2[["cus_name","id_number","row"]]
alls = pd.merge(df,rowws,on = ["cus_name","id_number"],how='left')

df1 = alls.replace([np.inf, -np.inf], np.nan)
df1['row'] = df1['row'].fillna(0)


## 写入sql
import pymssql
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('mssql+pymssql://riskuser:1qaz@WSX@170000:1433/H_riskMG_DB')

conn = engine.connect()
resselect=engine.execute("truncate table tick_data")
df1.to_sql('tick_data',conn,if_exists='append',index = False)

## 写入mysq
from sqlalchemy import create_engine
import pymysql
from sqlalchemy.types import VARCHAR
pymysql.install_as_MySQLdb()
engine = create_engine('mysql+pymysql://root:000000g@58edd9c77adb6.bj.cdb.myqcloud.com:5432/wangzhe?charset=utf8')
#resselect=engine.execute("truncate table tick_data")
conn = engine.connect()
#df1.to_sql('tick_data',conn,if_exists='append',index = False)
#print(df)
df1.to_excel(r"C:\Users\Administrator\Desktop\df8888.xlsx",encoding = 'gbk', index=False)

df1.to_csv(r"C:\Users\Administrator\Desktop\df8888.csv")


df1.to_sql('tick_data',conn,if_exists='append',index = False)












# all_hb = pd.read_excel(r"C:\Users\Administrator\Desktop\scoress\all_hb.xlsx")
# all_hb = all_hb.drop_duplicates('id_number')
# all_hb['dz'] = all_hb.apply(lambda x:df_all.dizhi(x),axis=1)
# all_hb['首付比'] =all_hb['indeed_down_payment_amount']/ all_hb['financing_amount']
# all_hbss = all_hb[['id_number','cus_name','financing_amount','首付比','indeed_down_payment_amount','indeed_final_payment_amount','剩余金额','是否有信贷','dz']]
#
# all_hbss.rename(columns={'financing_amount': '融资金额','indeed_down_payment_amount':'实收首付金额',
#                          'indeed_final_payment_amount':'尾付金额比例','是否有信贷': '是否有贷款或信用卡'
#                          }, inplace=True)
# # Index(['融资金额', '首付比', '实收首付金额', '尾付金额比例', '剩余金额', '是否有信贷', 'dz']
# print(all_hbss.columns)
# df_xgb = all_hbss[['融资金额', '首付比', '实收首付金额', '尾付金额比例', '剩余金额', '是否有贷款或信用卡', 'dz']]
# dtrain_predprob = df_all.xgb1().predict_proba(df_xgb)[:,1]
# all_hbss['p'] = dtrain_predprob
#
# ### 一种计算方案
# import math
# B = 90 / math.log(2)
# A = 800 - 90 * math.log(90) / math.log(2)
# all_hbss['odds'] =all_hbss['p']/(1-all_hbss['p'])
# # p_1 = 1/(1+np.exp(-odds))
# #         score = 655-95*np.log2((p_1/(1-p_1))/(0.05/0.95))
# all_hbss['score1'] = A -B *np.log(all_hbss['odds'])
#
# # df["p"] = p
# ### 二种计算方案
# s = (0.1 / 0.9) / (0.05 / 0.95)
# all_hbss['p_1'] = 1 / (1 + s * np.exp(-np.log(all_hbss['p'] / (1 -all_hbss['p']))))
# all_hbss['score'] = 652 - 95 * np.log((all_hbss['p_1'] / (1 -all_hbss['p_1'])) / (0.05 / 0.95))
# all_hbss.to_excel(r"C:\Users\Administrator\Desktop\scoress\all_hb_score2.xlsx",encoding = 'gbk', index=False)
# all_hbss['score'].value_counts().plot(kind = 'line',ax = ax)
# plt.show()
# # all_hb['score'][all_hb['score'] < 300] = 300
# # all_hb['score'][all_hb['score'] > 1000] = 1000
# print('qqqqqqqqqqqqqqqqqqqqqqqqqq',dtrain_predprob)
