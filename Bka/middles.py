#### 处理PBOC的数据模块


import pandas as pd
import json
class Pbc:
    def __init__(self,data):
        self.data = data

    ## 获取有无贷款，报告头
    def debat(self):
        result = []
        for ii in self.data:
            a = ''
            b = ''
            try:
                if 'ct' in json.loads(ii)['credit_result_json']['reportMessage'].keys():
                    infos = json.loads(ii)['credit_result_json']['reportMessage']['reportBasics']
                    a = 1
                    infos['是否有信贷'] = a
                    b = infos
                else:
                    infos = json.loads(ii)['credit_result_json']['reportMessage']['reportBasics']
                    a = 0

                    infos['是否有信贷'] = a
                    b = infos
                dicta = pd.DataFrame([b])
                print(dicta)
                result.append(dicta)
            except:
                if 'ct' in json.loads(json.loads(json.loads(ii)['credit_result_json'])['statusMsg'])[
                    'reportMessage'].keys():
                    infos = json.loads(json.loads(json.loads(ii)['credit_result_json'])['statusMsg'])['reportMessage'][
                        'reportBasics']
                    a = 1
                    infos['是否有信贷'] = a
                    b = infos
                else:
                    infos = json.loads(json.loads(json.loads(ii)['credit_result_json'])['statusMsg'])['reportMessage'][
                        'reportBasics']
                    a = 0

                    infos['是否有信贷'] = a
                    b = infos
                dicta = pd.DataFrame([b])
                print(dicta)
                result.append(dicta)
        result_dict = pd.concat(result, ignore_index=True)
        pboc_df = result_dict[['queryCredNum', 'queryName', '是否有信贷']]
        return pboc_df
    ## 个人基本信息表(户籍地）
    def peopleinfo(self):
        lista = []
        result1 = []
        for ii in self.data:

            try:
                if 'identityInfo' in json.loads(json.loads(json.loads(ii)['credit_result_json'])['statusMsg'])[
                    'reportMessage'].keys():
                    infos = json.loads(json.loads(json.loads(ii)['credit_result_json'])['statusMsg'])['reportMessage'][
                        'reportBasics']
                    identityInfo = \
                    json.loads(json.loads(json.loads(ii)['credit_result_json'])['statusMsg'])['reportMessage'][
                        'identityInfo']
                    infos.update(identityInfo)
                    dicta1 = pd.DataFrame([infos])
                    result1.append(dicta1)
            except:
                if 'identityInfo' in json.loads(ii)['credit_result_json']['reportMessage'].keys():
                    infos = json.loads(ii)['credit_result_json']['reportMessage']['reportBasics']
                    identityInfo = json.loads(ii)['credit_result_json']['reportMessage']['identityInfo']
                    infos.update(identityInfo)
                    dicta1 = pd.DataFrame([infos])
                    result1.append(dicta1)
        result_dict1 = pd.concat(result1, ignore_index=True)
        pboc_df1 = result_dict1[['queryCredNum', 'queryName', 'contactAddress', 'residenceAddress', 'maritalStatus', 'gender']]
        return pboc_df1
    ### 个人信息（工作地）
    def work(self):
        result2 = []
        for ii in self.data:

            try:
                if 'resideInfo' in json.loads(ii)['credit_result_json']['reportMessage'].keys():
                    infos = json.loads(ii)['credit_result_json']['reportMessage']['reportBasics']
                    resideInfo = json.loads(ii)['credit_result_json']['reportMessage']['resideInfo'][0]
                    infos.update(resideInfo)

                    dicta2 = pd.DataFrame([infos])
                    print('111', dicta2)
                    result2.append(dicta2)
            except:
                if 'resideInfo' in json.loads(json.loads(json.loads(ii)['credit_result_json'])['statusMsg'])[
                    'reportMessage'].keys():
                    infos = json.loads(json.loads(json.loads(ii)['credit_result_json'])['statusMsg'])['reportMessage'][
                        'reportBasics']
                    resideInfo = \
                    json.loads(json.loads(json.loads(ii)['credit_result_json'])['statusMsg'])['reportMessage'][
                        'resideInfo'][0]
                    infos.update(resideInfo)

                    dicta2 = pd.DataFrame([infos])
                    print('222', dicta2)
                    result2.append(dicta2)



        result_dict2 = pd.concat(result2, ignore_index=True)
        pboc_df2 = result_dict2[['queryCredNum', 'queryName', 'resideAddr']]
        return pboc_df2


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

## 获取score的函数
import numpy as np
import os
from pickle import load
class Score:
    def __init__(self, curname=os.path.dirname(os.path.abspath(__file__))):
        self.cur_dir = curname

    # @staticmethod
    def xgb(self):
        self.model_path = os.path.join(self.cur_dir, "alg1.pickle")
        self.xgb = load(open(self.model_path, "rb"))
        return self.xgb

    @staticmethod
    def get_score_a(p: pd.Series) -> pd.Series:
        s = (0.1 / 0.9) / (0.05 / 0.95)
        p_1 = 1 / (1 + s * np.exp(-np.log(p / (1 - p))))
        score = 652 - 95 * np.log((p_1 / (1 - p_1)) / (0.05 / 0.95))
        # score[score < 300] = 300
        # score[score > 1000] = 1000
        return score

    @staticmethod
    def get_score_b( p: pd.Series) -> pd.Series:
        import math
        B = 90 / math.log(2)
        A = 800 - 90 * math.log(90) / math.log(2)
        odds = p/ (1 - p)
        # p_1 = 1/(1+np.exp(-odds))
        #         score = 655-95*np.log2((p_1/(1-p_1))/(0.05/0.95))
        score1 = A - B * np.log(odds)
        return score1


    def derive(self, df: pd.DataFrame) -> pd.DataFrame:
        # df["cell_city"] = df["cell_city"].apply(lambda x: self.cell_city_map[x])
        # df["cell_province"] = df["cell_province"].apply(lambda x: self.cell_province_map[x])
        # df["frg_group_num"] = df["frg_group_num"].apply(lambda x: self.frg_group_num_map[x])
        # df["id_city"] = df["id_city"].apply(lambda x: self.id_city_map[x])
        return df
    def predict(self, df:pd.DataFrame)->pd.DataFrame:
        df = self.derive(df)
        feature = ['融资金额', '首付比', '实收首付金额', '尾付金额比例', '剩余金额', '是否有贷款或信用卡', 'dz']
        p = self.xgb().predict_proba(df.loc[:,feature])[:,1]
        df["p"] = p
        df["score"] = self.get_score_a(df["p"])
        df["score1"] = self.get_score_b(df["p"])
        return df
