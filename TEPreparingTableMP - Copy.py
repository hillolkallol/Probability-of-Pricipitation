from netCDF4 import Dataset
import numpy as np
import pandas as pd
import multiprocessing

# divide training and testing data
index30 = pd.read_csv('F:/dataset/rain_data/index30.csv', header=None)
index30 = np.array(index30.values[0])
# print(len(index70)) #26 values
# print(index70[25])
# print(index70[25])


# read netcdf
netcdf_entire_dataset = Dataset("F:/dataset/entire_dataset.nc", "r")
rain_models = netcdf_entire_dataset.variables['rain_models']

# choosing index and incrementing
def calculation1(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80, start, end):
    data_table_tt1 = np.frombuffer(data_table_1.get_obj()) #array for threshold 1
    data_table_tt5 = np.frombuffer(data_table_5.get_obj()) #array for threshold 5
    data_table_tt10 = np.frombuffer(data_table_10.get_obj()) #array for threshold 10
    data_table_tt20 = np.frombuffer(data_table_20.get_obj()) #array for threshold 20
    data_table_tt40 = np.frombuffer(data_table_40.get_obj()) #array for threshold 40
    data_table_tt80 = np.frombuffer(data_table_80.get_obj()) #array for threshold 80

    #reshaping arrays
    data_table_t1 = data_table_tt1.reshape((101, 10))
    data_table_t5 = data_table_tt5.reshape((101, 10))
    data_table_t10 = data_table_tt10.reshape((101, 10))
    data_table_t20 = data_table_tt20.reshape((101, 10))
    data_table_t40 = data_table_tt40.reshape((101, 10))
    data_table_t80 = data_table_tt80.reshape((101, 10))

    for i in index30[start:end]: # for all days
        for j in range(20): # for all times
            for k in range(1, 39): # for all models
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
                                if avg > 100: avg = 100 # selecting index 100 for all the average greater than 100
                                # print(avg)
                                # counting how many values are greater than the threshold
                                count_gt_threshold_t1 = len(taking_small_grid[np.where(taking_small_grid > 1)])
                                count_gt_threshold_t5 = len(taking_small_grid[np.where(taking_small_grid > 5)])
                                count_gt_threshold_t10 = len(taking_small_grid[np.where(taking_small_grid > 10)])
                                count_gt_threshold_t20 = len(taking_small_grid[np.where(taking_small_grid > 20)])
                                count_gt_threshold_t40 = len(taking_small_grid[np.where(taking_small_grid > 40)])
                                count_gt_threshold_t80 = len(taking_small_grid[np.where(taking_small_grid > 80)])

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
    data_table_1 = multiprocessing.Array('d', np.zeros((101, 10)).flat) #array for threshold 1
    data_table_5 = multiprocessing.Array('d', np.zeros((101, 10)).flat) #array for threshold 5
    data_table_10 = multiprocessing.Array('d', np.zeros((101, 10)).flat) #array for threshold 10
    data_table_20 = multiprocessing.Array('d', np.zeros((101, 10)).flat) #array for threshold 20
    data_table_40 = multiprocessing.Array('d', np.zeros((101, 10)).flat) #array for threshold 40
    data_table_80 = multiprocessing.Array('d', np.zeros((101, 10)).flat) #array for threshold 80


    # Using MULTIPROCESSING here
    # MULTIPROCESSING STARTS
    lock = multiprocessing.Lock()

    t1 = multiprocessing.Process(name='First', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80, 0, 2))
    t2 = multiprocessing.Process(name='Second', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80, 2, 4))
    t3 = multiprocessing.Process(name='third', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80, 4, 6))
    t4 = multiprocessing.Process(name='fourth', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80, 6, 8))
    t5 = multiprocessing.Process(name='fifth', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80, 8, 10))
    t6 = multiprocessing.Process(name='sixth', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80, 10, 12))
    t7 = multiprocessing.Process(name='seventh', target=calculation1,
        args=(lock, data_table_1, data_table_5, data_table_10, data_table_20, data_table_40, data_table_80, 12, 13))

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()
    # MULTIPROCESSING ENDS

    data_table_tt1 = np.frombuffer(data_table_1.get_obj()) #array for threshold 1
    data_table_tt5 = np.frombuffer(data_table_5.get_obj()) #array for threshold 5
    data_table_tt10 = np.frombuffer(data_table_10.get_obj()) #array for threshold 10
    data_table_tt20 = np.frombuffer(data_table_20.get_obj()) #array for threshold 20
    data_table_tt40 = np.frombuffer(data_table_40.get_obj()) #array for threshold 40
    data_table_tt80 = np.frombuffer(data_table_80.get_obj()) #array for threshold 80

    # reshaping arrays
    data_table_t1 = data_table_tt1.reshape((101, 10))
    data_table_t5 = data_table_tt5.reshape((101, 10))
    data_table_t10 = data_table_tt10.reshape((101, 10))
    data_table_t20 = data_table_tt20.reshape((101, 10))
    data_table_t40 = data_table_tt40.reshape((101, 10))
    data_table_t80 = data_table_tt80.reshape((101, 10))

    # storing into csv
    np.savetxt('te_threshold_' + str(1) + '.csv', data_table_t1, delimiter=',', fmt='%10.5f')
    np.savetxt('te_threshold_' + str(5) + '.csv', data_table_t5, delimiter=',', fmt='%10.5f')
    np.savetxt('te_threshold_' + str(10) + '.csv', data_table_t10, delimiter=',', fmt='%10.5f')
    np.savetxt('te_threshold_' + str(20) + '.csv', data_table_t20, delimiter=',', fmt='%10.5f')
    np.savetxt('te_threshold_' + str(40) + '.csv', data_table_t40, delimiter=',', fmt='%10.5f')
    np.savetxt('te_threshold_' + str(80) + '.csv', data_table_t80, delimiter=',', fmt='%10.5f')
