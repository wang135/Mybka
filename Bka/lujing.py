import os
from pickle import load

class lujings:
    def __init__(self,curname = os.path.dirname(os.path.abspath(__file__))):
        self.cur_dir= curname
    def shenfen(self):
        self.model_path1 = os.path.join(self.cur_dir, "pick/cellphone.pickle")
        self.shenfen = load(open(self.model_path1, "rb"))
        return self.shenfen
    def xgb(self):
        self.model_path2 = os.path.join(self.cur_dir, "pick/xgb.pkl")
        self.xgb = load(open(self.model_path2, "rb"))
        return self.xgb
    def feature(self):
        self.model_path3 = os.path.join(self.cur_dir, "pick/features.pkl")
        self.feature= load(open(self.model_path3, "rb"))
        return self.feature
    def cell_city(self):
        self.model_path4 = os.path.join(self.cur_dir, "pick/cell_city.pickle")
        self.cell_city = load(open(self.model_path4, "rb"))
        return self.cell_city
    def id_city(self):
        self.model_path5 = os.path.join(self.cur_dir, "pick/id_city.pickle")
        self.id_city = load(open(self.model_path5, "rb"))
        return self.id_city
    def cell_province(self):
        self.model_path6 = os.path.join(self.cur_dir, "pick/cell_province.pickle")
        self.cell_province = load(open(self.model_path6, "rb"))
        return self.cell_province
    def id_yinshe(self):
        self.model_path7 = os.path.join(self.cur_dir, "pick/id_yinshe.pickle")
        self.id_yinshe = load(open(self.model_path7, "rb"))
        return self.id_yinshe
    def diqu(self):
        self.model_path8 = os.path.join(self.cur_dir, "pick/diqu.pickle")
        self.diqu = load(open(self.model_path8, "rb"))
        return self.diqu