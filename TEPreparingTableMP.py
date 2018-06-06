from netCDF4 import Dataset
import numpy as np
import pandas as pd
import multiprocessing

np.seterr(divide='ignore', invalid='ignore')
# divide training and testing data
index70 = pd.read_csv('F:/dataset/rain_data/index70.csv', header=None)
index70 = np.array(index70.values[0])
# print(len(index70)) #26 values
# print(index70[25])

# choosing index and incrementing
def calculation1(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80,
                 total_1, total_5, total_10, total_20, total_40, total_80, start, end):
    # read netcdf
    netcdf_entire_dataset = Dataset("F:/dataset/entire_dataset.nc", "r")
    rain_models = netcdf_entire_dataset.variables['rain_models']

    data_table_tt1 = np.frombuffer(data_table_1.get_obj()) #array for threshold 1
    data_table_tt5 = np.frombuffer(data_table_5.get_obj()) #array for threshold 5
    data_table_tt10 = np.frombuffer(data_table_10.get_obj()) #array for threshold 10
    data_table_tt20 = np.frombuffer(data_table_20.get_obj()) #array for threshold 20
    data_table_tt40 = np.frombuffer(data_table_40.get_obj()) #array for threshold 40
    data_table_tt80 = np.frombuffer(data_table_80.get_obj()) #array for threshold 80

    #reshaping arrays
    data_table_t1 = data_table_tt1.reshape((15, 10))
    data_table_t5 = data_table_tt5.reshape((15, 10))
    data_table_t10 = data_table_tt10.reshape((15, 10))
    data_table_t20 = data_table_tt20.reshape((15, 10))
    data_table_t40 = data_table_tt40.reshape((15, 10))
    data_table_t80 = data_table_tt80.reshape((15, 10))

    total_tt1 = np.frombuffer(total_1.get_obj())  # array for threshold 1
    total_tt5 = np.frombuffer(total_5.get_obj())  # array for threshold 5
    total_tt10 = np.frombuffer(total_10.get_obj())  # array for threshold 10
    total_tt20 = np.frombuffer(total_20.get_obj())  # array for threshold 20
    total_tt40 = np.frombuffer(total_40.get_obj())  # array for threshold 40
    total_tt80 = np.frombuffer(total_80.get_obj())  # array for threshold 80

    # reshaping arrays
    total_t1 = total_tt1.reshape((15, 10))
    total_t5 = total_tt5.reshape((15, 10))
    total_t10 = total_tt10.reshape((15, 10))
    total_t20 = total_tt20.reshape((15, 10))
    total_t40 = total_tt40.reshape((15, 10))
    total_t80 = total_tt80.reshape((15, 10))

    for i in index70[start:end]: # for all days
        for j in range(10): # for all times
            for k in range(1, 25): # for all models
                single_file = rain_models[i, j, k, :, :] # model data
                real_grid = rain_models[i, j, 0, :, :] # real data

                a = np.array(single_file)
                a[a > 10000] = np.nan
                if np.isnan(a).all(): # skipping models which don't have values for a particular day and time
                    print('########################## nan #############################')
                else:
                    for grid_y in range(1, 1149):  # for every y
                        for grid_x in range(1, 1682):  # for every x
                            print(i, j, k, grid_y, grid_x)
                            try:
                                taking_small_grid = single_file[grid_y - 1:grid_y + 2, grid_x - 1:grid_x + 2] # taking 3X3 grid
                                avg = int(np.average(taking_small_grid)) # taking int value of average

                                # selecting index for average
                                if avg <= 5: avg = 0
                                elif avg > 5 and avg <= 10: avg = 1
                                elif avg > 10 and avg <= 15: avg = 2
                                elif avg > 15 and avg <= 20: avg = 3
                                elif avg > 20 and avg <= 25: avg = 4
                                elif avg > 25 and avg <= 30: avg = 5
                                elif avg > 30 and avg <= 35: avg = 6
                                elif avg > 35 and avg <= 40: avg = 7
                                elif avg > 40 and avg <= 45: avg = 8
                                elif avg > 45 and avg <= 50: avg = 9
                                elif avg > 50 and avg <= 55: avg = 10
                                elif avg > 55 and avg <= 60: avg = 11
                                elif avg > 60 and avg <= 65: avg = 12
                                elif avg > 65 and avg <= 70: avg = 13
                                elif avg > 70: avg = 14

                                # print(avg)
                                # counting how many values are greater than the threshold
                                count_gt_threshold_t1 = len(taking_small_grid[np.where(taking_small_grid > 1)])
                                count_gt_threshold_t5 = len(taking_small_grid[np.where(taking_small_grid > 5)])
                                count_gt_threshold_t10 = len(taking_small_grid[np.where(taking_small_grid > 10)])
                                count_gt_threshold_t20 = len(taking_small_grid[np.where(taking_small_grid > 20)])
                                count_gt_threshold_t40 = len(taking_small_grid[np.where(taking_small_grid > 40)])
                                count_gt_threshold_t80 = len(taking_small_grid[np.where(taking_small_grid > 80)])

                                lock.acquire()
                                total_t1[avg, count_gt_threshold_t1] += 1
                                total_t5[avg, count_gt_threshold_t5] += 1
                                total_t10[avg, count_gt_threshold_t10] += 1
                                total_t20[avg, count_gt_threshold_t20] += 1
                                total_t40[avg, count_gt_threshold_t40] += 1
                                total_t80[avg, count_gt_threshold_t80] += 1
                                lock.release()

                                if real_grid[grid_y, grid_x] > single_file[grid_y, grid_x]: # if real value is greater than the model value
                                    lock.acquire()
                                    # incrementing
                                    data_table_t1[avg, count_gt_threshold_t1] += 1
                                    data_table_t5[avg, count_gt_threshold_t5] += 1
                                    data_table_t10[avg, count_gt_threshold_t10] += 1
                                    data_table_t20[avg, count_gt_threshold_t20] += 1
                                    data_table_t40[avg, count_gt_threshold_t40] += 1
                                    data_table_t80[avg, count_gt_threshold_t80] += 1
                                    #print('counting.................................')
                                    # name = multiprocessing.current_process().name
                                    # print(name, np.amax(data_table_t1))
                                    lock.release()
                            except:
                                print('pass..')
                                pass

if __name__ == "__main__":
    data_table_1 = multiprocessing.Array('d', np.zeros((15, 10)).flat) #array for threshold 1
    data_table_5 = multiprocessing.Array('d', np.zeros((15, 10)).flat) #array for threshold 5
    data_table_10 = multiprocessing.Array('d', np.zeros((15, 10)).flat) #array for threshold 10
    data_table_20 = multiprocessing.Array('d', np.zeros((15, 10)).flat) #array for threshold 20
    data_table_40 = multiprocessing.Array('d', np.zeros((15, 10)).flat) #array for threshold 40
    data_table_80 = multiprocessing.Array('d', np.zeros((15, 10)).flat) #array for threshold 80

    # counting total
    total_1 = multiprocessing.Array('d', np.zeros((15, 10)).flat)
    total_5 = multiprocessing.Array('d', np.zeros((15, 10)).flat)
    total_10 = multiprocessing.Array('d', np.zeros((15, 10)).flat)
    total_20 = multiprocessing.Array('d', np.zeros((15, 10)).flat)
    total_40 = multiprocessing.Array('d', np.zeros((15, 10)).flat)
    total_80 = multiprocessing.Array('d', np.zeros((15, 10)).flat)

    # Using MULTIPROCESSING here
    # MULTIPROCESSING STARTS
    lock = multiprocessing.Lock()

    t1 = multiprocessing.Process(name='First', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80,
              total_1, total_5, total_10, total_20, total_40, total_80, 0, 3))
    t2 = multiprocessing.Process(name='Second', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80,
              total_1, total_5, total_10, total_20, total_40, total_80, 3, 6))
    t3 = multiprocessing.Process(name='Second', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80,
              total_1, total_5, total_10, total_20, total_40, total_80, 6, 9))
    t4 = multiprocessing.Process(name='Second', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80,
              total_1, total_5, total_10, total_20, total_40, total_80, 9, 12))
    t5 = multiprocessing.Process(name='Second', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80,
              total_1, total_5, total_10, total_20, total_40, total_80, 12, 15))
    t6 = multiprocessing.Process(name='Second', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80,
              total_1, total_5, total_10, total_20, total_40, total_80, 15, 18))
    t7 = multiprocessing.Process(name='First', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80,
              total_1, total_5, total_10, total_20, total_40, total_80, 18, 21))
    t8 = multiprocessing.Process(name='Second', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80,
              total_1, total_5, total_10, total_20, total_40, total_80, 21, 26))

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()
    t8.join()
    # MULTIPROCESSING ENDS

    data_table_tt1 = np.frombuffer(data_table_1.get_obj()) #array for threshold 1
    data_table_tt5 = np.frombuffer(data_table_5.get_obj()) #array for threshold 5
    data_table_tt10 = np.frombuffer(data_table_10.get_obj()) #array for threshold 10
    data_table_tt20 = np.frombuffer(data_table_20.get_obj()) #array for threshold 20
    data_table_tt40 = np.frombuffer(data_table_40.get_obj()) #array for threshold 40
    data_table_tt80 = np.frombuffer(data_table_80.get_obj()) #array for threshold 80

    # reshaping arrays
    data_table_t1 = data_table_tt1.reshape((15, 10))
    data_table_t5 = data_table_tt5.reshape((15, 10))
    data_table_t10 = data_table_tt10.reshape((15, 10))
    data_table_t20 = data_table_tt20.reshape((15, 10))
    data_table_t40 = data_table_tt40.reshape((15, 10))
    data_table_t80 = data_table_tt80.reshape((15, 10))

    total_tt1 = np.frombuffer(total_1.get_obj())  # array for threshold 1
    total_tt5 = np.frombuffer(total_5.get_obj())  # array for threshold 5
    total_tt10 = np.frombuffer(total_10.get_obj())  # array for threshold 10
    total_tt20 = np.frombuffer(total_20.get_obj())  # array for threshold 20
    total_tt40 = np.frombuffer(total_40.get_obj())  # array for threshold 40
    total_tt80 = np.frombuffer(total_80.get_obj())  # array for threshold 80

    # reshaping arrays
    total_t1 = total_tt1.reshape((15, 10))
    total_t5 = total_tt5.reshape((15, 10))
    total_t10 = total_tt10.reshape((15, 10))
    total_t20 = total_tt20.reshape((15, 10))
    total_t40 = total_tt40.reshape((15, 10))
    total_t80 = total_tt80.reshape((15, 10))

    data_1 = (data_table_t1 / total_t1) * 100
    data_5 = (data_table_t5 / total_t5) * 100
    data_10 = (data_table_t10 / total_t10) * 100
    data_20 = (data_table_t20 / total_t20) * 100
    data_40 = (data_table_t40 / total_t40) * 100
    data_80 = (data_table_t80 / total_t80) * 100

    # storing into csv
    np.savetxt('tr_threshold_' + str(1) + '.csv', data_1, delimiter=',', fmt='%10.5f')
    np.savetxt('tr_threshold_' + str(5) + '.csv', data_5, delimiter=',', fmt='%10.5f')
    np.savetxt('tr_threshold_' + str(10) + '.csv', data_10, delimiter=',', fmt='%10.5f')
    np.savetxt('tr_threshold_' + str(20) + '.csv', data_20, delimiter=',', fmt='%10.5f')
    np.savetxt('tr_threshold_' + str(40) + '.csv', data_40, delimiter=',', fmt='%10.5f')
    np.savetxt('tr_threshold_' + str(80) + '.csv', data_80, delimiter=',', fmt='%10.5f')
