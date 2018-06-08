import pandas as pd
import numpy as np
import math

tr_df_1 = pd.read_csv('tr_threshold_1.csv', header=None).astype(float)
tr_df_5 = pd.read_csv('tr_threshold_5.csv', header=None).astype(float)
tr_df_10 = pd.read_csv('tr_threshold_10.csv', header=None).astype(float)
tr_df_20 = pd.read_csv('tr_threshold_20.csv', header=None).astype(float)
tr_df_40 = pd.read_csv('tr_threshold_40.csv', header=None).astype(float)
tr_df_80 = pd.read_csv('tr_threshold_80.csv', header=None).astype(float)

te_df_1 = pd.read_csv('te_threshold_1.csv', header=None).astype(float)
te_df_5 = pd.read_csv('te_threshold_5.csv', header=None).astype(float)
te_df_10 = pd.read_csv('te_threshold_10.csv', header=None).astype(float)
te_df_20 = pd.read_csv('te_threshold_20.csv', header=None).astype(float)
te_df_40 = pd.read_csv('te_threshold_40.csv', header=None).astype(float)
te_df_80 = pd.read_csv('te_threshold_80.csv', header=None).astype(float)

np_tr_df_1 = np.array(tr_df_1)
np_tr_df_5 = np.array(tr_df_5)
np_tr_df_10 = np.array(tr_df_10)
np_tr_df_20 = np.array(tr_df_20)
np_tr_df_40 = np.array(tr_df_40)
np_tr_df_80 = np.array(tr_df_80)

np_te_df_1 = np.array(te_df_1)
np_te_df_5 = np.array(te_df_5)
np_te_df_10 = np.array(te_df_10)
np_te_df_20 = np.array(te_df_20)
np_te_df_40 = np.array(te_df_40)
np_te_df_80 = np.array(te_df_80)

# brier score = (1/n) * (p_k - o_k)^2 [from k=1 to n, where n is the total forecast]
n = 150 # (0-9) 10 neighbors and 15 different range of average column cells
probability_1 = np.round(((np_tr_df_1 - np_te_df_1)** 2) / n, 4)
probability_5 = np.round(((np_tr_df_5 - np_te_df_5)** 2) / n, 4)
probability_10 = np.round(((np_tr_df_10 - np_te_df_10)** 2) / n, 4)
probability_20 = np.round(((np_tr_df_20 - np_te_df_20)** 2) / n, 4)
probability_40 = np.round(((np_tr_df_40 - np_te_df_40)** 2) / n, 4)
probability_80 = np.round(((np_tr_df_80 - np_te_df_80)** 2) / n, 4)

# print(np.round(probability_1, 4))
np.savetxt('bs_1.csv', probability_1, delimiter=',', fmt='%10.5f')
np.savetxt('bs_5.csv', probability_5, delimiter=',', fmt='%10.5f')
np.savetxt('bs_10.csv', probability_10, delimiter=',', fmt='%10.5f')
np.savetxt('bs_20.csv', probability_20, delimiter=',', fmt='%10.5f')
np.savetxt('bs_40.csv', probability_40, delimiter=',', fmt='%10.5f')
np.savetxt('bs_80.csv', probability_80, delimiter=',', fmt='%10.5f')

