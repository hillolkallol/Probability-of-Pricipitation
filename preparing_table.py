# from netCDF4 import Dataset
# import numpy as np
# import pandas as pd
#
# # divide training and testing data
# index70 = pd.read_csv('F:/dataset/rain_data/index70.csv', header=None)
# # print(index70.values[0][5])
#
# # read netcdf
# netcdf_entire_dataset = Dataset("F:/dataset/entire_dataset.nc", "r")
# rain_models = netcdf_entire_dataset.variables['rain_models']
#
# threshold_array = [1, 5, 10, 20, 40, 80]
#
# for threshold in threshold_array:
#     data_table = np.zeros((51, 10))
#     for i in index70:
#         for j in range(20):
#             for k in range(1, 39):
#                 single_file = rain_models[i, j, k, :, :]
#                 real_grid = rain_models[i, j, 0, :, :]
#
#                 a = np.array(single_file)
#                 a[a > 10000] = np.nan
#                 if np.isnan(a).all():
#                     print('nan')
#                 else:
#                     for grid_y in range(1, 1149):  # for every y
#                         for grid_x in range(1, 1682):  # for every x
#                             print(i, j, k, grid_y, grid_x)
#                             try:
#                                 taking_small_grid = single_file[grid_y - 1:grid_y + 2, grid_x - 1:grid_x + 2]
#                                 avg = int(np.average(taking_small_grid))
#                                 if avg > 50: avg = 50
#                                 # print(avg)
#                                 count_gt_threshold = len(taking_small_grid[np.where(taking_small_grid > threshold)])
#                                 # print(count_gt_threshold)
#
#                                 if real_grid[grid_y, grid_x] > single_file[grid_y, grid_x]:
#                                     data_table[avg, count_gt_threshold] += 1
#                                     print('counting..')
#                             except:
#                                 print('passing..')
#                                 pass
#
#     np.savetxt('threshold_'+str(threshold)+'.csv', data_table, delimiter=',')


















#
# from netCDF4 import Dataset
# import numpy as np
# import pandas as pd
# import threading
#
# def calculation(ind):
#     for i in index70[ind:ind+6]:
#         for j in range(20):
#             for k in range(1, 39):
#                 single_file = rain_models[i, j, k, :, :]
#                 real_grid = rain_models[i, j, 0, :, :]
#
#                 a = np.array(single_file)
#                 a[a > 10000] = np.nan
#                 if np.isnan(a).all():
#                     print('########################## nan #############################')
#                 else:
#                     # calculation(single_file, real_grid)
#                     # threading.Thread(target=calculation, args=(single_file, real_grid)).start()
#                     for grid_y in range(1, 1149):  # for every y
#                         for grid_x in range(1, 1682):  # for every x
#                             print(i, j, k, grid_y, grid_x)
#                             try:
#                                 taking_small_grid = single_file[grid_y - 1:grid_y + 2, grid_x - 1:grid_x + 2]
#                                 avg = int(np.average(taking_small_grid))
#                                 if avg > 100: avg = 100
#                                 # print(avg)
#                                 count_gt_threshold_t1 = len(taking_small_grid[np.where(taking_small_grid > 1)])
#                                 count_gt_threshold_t5 = len(taking_small_grid[np.where(taking_small_grid > 5)])
#                                 count_gt_threshold_t10 = len(taking_small_grid[np.where(taking_small_grid > 10)])
#                                 count_gt_threshold_t20 = len(taking_small_grid[np.where(taking_small_grid > 20)])
#                                 count_gt_threshold_t40 = len(taking_small_grid[np.where(taking_small_grid > 40)])
#                                 count_gt_threshold_t80 = len(taking_small_grid[np.where(taking_small_grid > 80)])
#                                 # print(count_gt_threshold)
#
#                                 if real_grid[grid_y, grid_x] > single_file[grid_y, grid_x]:
#                                     data_table_t1[avg, count_gt_threshold_t1] += 1
#                                     data_table_t5[avg, count_gt_threshold_t5] += 1
#                                     data_table_t10[avg, count_gt_threshold_t10] += 1
#                                     data_table_t20[avg, count_gt_threshold_t20] += 1
#                                     data_table_t40[avg, count_gt_threshold_t40] += 1
#                                     data_table_t80[avg, count_gt_threshold_t80] += 1
#                                     print('counting.................................')
#                             except:
#                                 print('passing..')
#                                 pass
#
#
# if __name__ == "__main__":
#     # divide training and testing data
#     index70 = pd.read_csv('F:/dataset/rain_data/index70.csv', header=None)
#     index70 = np.array(index70.values[0])
#     # print(len(index70))
#     # print(index70.values[0][5])
#
#     # read netcdf
#     netcdf_entire_dataset = Dataset("F:/dataset/entire_dataset.nc", "r")
#     rain_models = netcdf_entire_dataset.variables['rain_models']
#
#     data_table_t1 = np.zeros((101, 10))
#     data_table_t5 = np.zeros((101, 10))
#     data_table_t10 = np.zeros((101, 10))
#     data_table_t20 = np.zeros((101, 10))
#     data_table_t40 = np.zeros((101, 10))
#     data_table_t80 = np.zeros((101, 10))
#
#     t1 = threading.Thread(target=calculation, args=(0,))
#     t2 = threading.Thread(target=calculation, args=(6,))
#     t3 = threading.Thread(target=calculation, args=(12,))
#     t4 = threading.Thread(target=calculation, args=(18,))
#
#     t1.start()
#     t2.start()
#     t3.start()
#     t4.start()
#
#     np.savetxt('threshold_'+str(1)+'.csv', data_table_t1, delimiter=',', fmt='%10.5f')
#     np.savetxt('threshold_' + str(5) + '.csv', data_table_t5, delimiter=',', fmt='%10.5f')
#     np.savetxt('threshold_' + str(10) + '.csv', data_table_t10, delimiter=',', fmt='%10.5f')
#     np.savetxt('threshold_' + str(20) + '.csv', data_table_t20, delimiter=',', fmt='%10.5f')
#     np.savetxt('threshold_' + str(40) + '.csv', data_table_t40, delimiter=',', fmt='%10.5f')
#     np.savetxt('threshold_' + str(80) + '.csv', data_table_t80, delimiter=',', fmt='%10.5f')
#












from netCDF4 import Dataset
import numpy as np
import pandas as pd
import multiprocessing

# divide training and testing data
index70 = pd.read_csv('F:/dataset/rain_data/index70.csv', header=None)
index70 = np.array(index70.values[0])
# print(len(index70))
# print(index70.values[0][5])

# read netcdf
netcdf_entire_dataset = Dataset("F:/dataset/entire_dataset.nc", "r")
rain_models = netcdf_entire_dataset.variables['rain_models']
# data_table_t1 = np.zeros((101, 10))

def calculation1(data_table_1, ind, lock):
    data_table_tt1 = np.frombuffer(data_table_1.get_obj())
    data_table_t1 = data_table_tt1.reshape((101, 10))
    for i in index70[ind:ind+1]:
        for j in range(1):
            for k in range(1, 2):
                single_file = rain_models[i, j, k, :, :]
                real_grid = rain_models[i, j, 0, :, :]

                a = np.array(single_file)
                a[a > 10000] = np.nan
                if np.isnan(a).all():
                    print('########################## nan #############################')
                else:
                    # calculation(single_file, real_grid)
                    # threading.Thread(target=calculation, args=(single_file, real_grid)).start()
                    for grid_y in range(1, 1149):  # for every y
                        for grid_x in range(1, 1682):  # for every x
                            # print(i, j, k, grid_y, grid_x)
                            try:
                                taking_small_grid = single_file[grid_y - 1:grid_y + 2, grid_x - 1:grid_x + 2]
                                avg = int(np.average(taking_small_grid))
                                if avg > 100: avg = 100
                                # print(avg)
                                count_gt_threshold_t1 = len(taking_small_grid[np.where(taking_small_grid > 1)])
                                # count_gt_threshold_t5 = len(taking_small_grid[np.where(taking_small_grid > 5)])
                                # count_gt_threshold_t10 = len(taking_small_grid[np.where(taking_small_grid > 10)])
                                # count_gt_threshold_t20 = len(taking_small_grid[np.where(taking_small_grid > 20)])
                                # count_gt_threshold_t40 = len(taking_small_grid[np.where(taking_small_grid > 40)])
                                # count_gt_threshold_t80 = len(taking_small_grid[np.where(taking_small_grid > 80)])
                                # print(count_gt_threshold)

                                if real_grid[grid_y, grid_x] > single_file[grid_y, grid_x]:

                                    lock.acquire()
                                    data_table_t1[avg, count_gt_threshold_t1] += 1
                                    # data_table_t5[avg, count_gt_threshold_t5] += 1
                                    # data_table_t10[avg, count_gt_threshold_t10] += 1
                                    # data_table_t20[avg, count_gt_threshold_t20] += 1
                                    # data_table_t40[avg, count_gt_threshold_t40] += 1
                                    # data_table_t80[avg, count_gt_threshold_t80] += 1
                                    #print('counting.................................')
                                    name = multiprocessing.current_process().name
                                    print(name, np.amax(data_table_t1))
                                    # printMax()
                                    lock.release()
                            except:
                                print('passing..')
                                pass


# def calculation2(data_table_1):
#     data_table_tt1 = np.frombuffer(data_table_1.get_obj())
#     data_table_t1 = data_table_tt1.reshape((101, 10))
#     for i in index70[12:13]:
#         for j in range(1):
#             for k in range(1, 2):
#                 single_file = rain_models[i, j, k, :, :]
#                 real_grid = rain_models[i, j, 0, :, :]
#
#                 a = np.array(single_file)
#                 a[a > 10000] = np.nan
#                 if np.isnan(a).all():
#                     print('########################## nan #############################')
#                 else:
#                     # calculation(single_file, real_grid)
#                     # threading.Thread(target=calculation, args=(single_file, real_grid)).start()
#                     for grid_y in range(1, 1149):  # for every y
#                         for grid_x in range(1, 1682):  # for every x
#                             # print(i, j, k, grid_y, grid_x)
#
#                             taking_small_grid = single_file[grid_y - 1:grid_y + 2, grid_x - 1:grid_x + 2]
#                             avg = int(np.average(taking_small_grid))
#                             if avg > 100: avg = 100
#                             # print(avg)
#                             count_gt_threshold_t1 = len(taking_small_grid[np.where(taking_small_grid > 1)])
#                             # count_gt_threshold_t5 = len(taking_small_grid[np.where(taking_small_grid > 5)])
#                             # count_gt_threshold_t10 = len(taking_small_grid[np.where(taking_small_grid > 10)])
#                             # count_gt_threshold_t20 = len(taking_small_grid[np.where(taking_small_grid > 20)])
#                             # count_gt_threshold_t40 = len(taking_small_grid[np.where(taking_small_grid > 40)])
#                             # count_gt_threshold_t80 = len(taking_small_grid[np.where(taking_small_grid > 80)])
#                             # print(count_gt_threshold)
#
#                             if real_grid[grid_y, grid_x] > single_file[grid_y, grid_x]:
#                                 data_table_t1[avg, count_gt_threshold_t1] += 1
#                                 # data_table_t5[avg, count_gt_threshold_t5] += 1
#                                 # data_table_t10[avg, count_gt_threshold_t10] += 1
#                                 # data_table_t20[avg, count_gt_threshold_t20] += 1
#                                 # data_table_t40[avg, count_gt_threshold_t40] += 1
#                                 # data_table_t80[avg, count_gt_threshold_t80] += 1
#                                 #print('counting.................................')
#                                 name = multiprocessing.current_process().name
#                                 print(name, np.amax(data_table_t1))
#                                 # printMax()

if __name__ == "__main__":

    data_table1 = np.zeros((101, 10))
    # data_table_t5 = np.zeros((101, 10))
    # data_table_t10 = np.zeros((101, 10))
    # data_table_t20 = np.zeros((101, 10))
    # data_table_t40 = np.zeros((101, 10))
    # data_table_t80 = np.zeros((101, 10))

    data_table_1 = multiprocessing.Array('d', np.zeros((101, 10)).flat)
    lock = multiprocessing.Lock()

    t1 = multiprocessing.Process(name='First', target=calculation1, args=(data_table_1, 0, lock))
    t2 = multiprocessing.Process(name='Second', target=calculation1, args=(data_table_1, 1, lock))

    t1.daemon = True
    t2.daemon = True

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    data_table_tt1 = np.frombuffer(data_table_1.get_obj())
    data_table_t1 = data_table_tt1.reshape((101, 10))

    print('max', np.sum(data_table_t1))
    np.savetxt('threshold_' + str(1) + '.csv', data_table_t1, delimiter=',', fmt='%10.5f')
    # np.savetxt('threshold_' + str(5) + '.csv', data_table_t5, delimiter=',', fmt='%10.5f')
    # np.savetxt('threshold_' + str(10) + '.csv', data_table_t10, delimiter=',', fmt='%10.5f')
    # np.savetxt('threshold_' + str(20) + '.csv', data_table_t20, delimiter=',', fmt='%10.5f')
    # np.savetxt('threshold_' + str(40) + '.csv', data_table_t40, delimiter=',', fmt='%10.5f')
    # np.savetxt('threshold_' + str(80) + '.csv', data_table_t80, delimiter=',', fmt='%10.5f')
