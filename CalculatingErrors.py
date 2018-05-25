import pandas as pd
import numpy as np

tr_df_1 = pd.read_csv('F:/dataset/rain_data/table/threshold_1.csv', header=None)
tr_df_5 = pd.read_csv('F:/dataset/rain_data/table/threshold_5.csv', header=None)
tr_df_10 = pd.read_csv('F:/dataset/rain_data/table/threshold_10.csv', header=None)
tr_df_20 = pd.read_csv('F:/dataset/rain_data/table/threshold_20.csv', header=None)
tr_df_40 = pd.read_csv('F:/dataset/rain_data/table/threshold_40.csv', header=None)
tr_df_80 = pd.read_csv('F:/dataset/rain_data/table/threshold_80.csv', header=None)

te_df_1 = pd.read_csv('F:/dataset/rain_data/table/te_threshold_1.csv', header=None)
te_df_5 = pd.read_csv('F:/dataset/rain_data/table/te_threshold_5.csv', header=None)
te_df_10 = pd.read_csv('F:/dataset/rain_data/table/te_threshold_10.csv', header=None)
te_df_20 = pd.read_csv('F:/dataset/rain_data/table/te_threshold_20.csv', header=None)
te_df_40 = pd.read_csv('F:/dataset/rain_data/table/te_threshold_40.csv', header=None)
te_df_80 = pd.read_csv('F:/dataset/rain_data/table/te_threshold_80.csv', header=None)

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

total_train = ((20*((15*10)+(9*20))) + (6*15*20))*1148*1681 /100
total_test = ((9*((15*10)+(9*20))) + (4*15*20))*1148*1681 /100

probability_1 = np.round(abs((np_tr_df_1/total_train) - (np_te_df_1/total_test)), 4)
probability_5 = np.round(abs((np_tr_df_5/total_train) - (np_te_df_5/total_test)), 4)
probability_10 = np.round(abs((np_tr_df_10/total_train) - (np_te_df_10/total_test)), 4)
probability_20 = np.round(abs((np_tr_df_20/total_train) - (np_te_df_20/total_test)), 4)
probability_40 = np.round(abs((np_tr_df_40/total_train) - (np_te_df_40/total_test)), 4)
probability_80 = np.round(abs((np_tr_df_80/total_train) - (np_te_df_80/total_test)), 4)

# print(np.round(probability_1, 4))
np.savetxt('F:/dataset/rain_data/table/error_1.csv', probability_1, delimiter=',', fmt='%10.5f')
np.savetxt('F:/dataset/rain_data/table/error_5.csv', probability_5, delimiter=',', fmt='%10.5f')
np.savetxt('F:/dataset/rain_data/table/error_10.csv', probability_10, delimiter=',', fmt='%10.5f')
np.savetxt('F:/dataset/rain_data/table/error_20.csv', probability_20, delimiter=',', fmt='%10.5f')
np.savetxt('F:/dataset/rain_data/table/error_40.csv', probability_40, delimiter=',', fmt='%10.5f')
np.savetxt('F:/dataset/rain_data/table/error_80.csv', probability_80, delimiter=',', fmt='%10.5f')

