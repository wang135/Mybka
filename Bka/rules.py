import numpy as np
import pandas as pd
from Bka.bluoji import xgb1
import matplotlib.pyplot as plt

fig, ax = plt.subplots()
df_all = xgb1()

df2, q4 = df_all.dfmerge_yw()
