from Bka.middles import Score
from Bka.bluoji import xgb1

import matplotlib.pyplot as plt
fig, ax = plt.subplots()
df_all = xgb1()
score = Score()
import pandas as pd

all_hb = pd.read_excel(r"C:\Users\Administrator\Desktop\scoress\all_hb.xlsx")
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

print(df)