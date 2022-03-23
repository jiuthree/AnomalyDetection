import statsmodels
from clickhouse_driver import Client
import matplotlib.pyplot as plt
import numpy as np
# print(statsmodels.__version__)
from pathlib import Path
import pandas as pd
import re

from tad.myutils import dparserfunc, do_S_H_ESD, df_concat_series, get_file_name_by_time

client = Client(host='xxx', database='xxx', user='xxx', password='xxx')


# 传入sql，返回dataframe对象
def read_sql(sql):
    data, columns = client.execute(sql, columnar=True, with_column_types=True)
    df = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})
    return df


# plt.figure()  # 创建图形
#
# x = range(0,100,1)
# y = [i/10 for i in x]
# z = list(map(lambda i: i/10,x))
# # 创建两个子图中的第一个，设置坐标轴
# plt.subplot(2, 1, 1)  # （行、列、子图编号）
# plt.plot(x, np.sin(y))
#
# # 创建两个子图中的第二个，设置坐标轴
# plt.subplot(2, 1, 2)
# plt.plot(z, np.cos(z))
#
# plt.show()

# fig = plt.figure()
# ax = plt.axes()
#
# x = np.linspace(0, 10, 1000)
# ax.plot(x, np.sin(x))
# ax.plot(x, np.cos(x))
# plt.xlim(-1, 11)
# plt.ylim(-1.5, 1.5)
# plt.axis('equal')
# plt.show()
#
# data = [{'a': i, 'b': 2 * i} for i in range(3)]
# data = pd.DataFrame(data)
#
# series= pd.Series(data['a'].values, index=data['b'])
#
# print(series.values)
#
# print(series.index)



TEST_DATA_DIR = Path('../resources/data')

data = pd.read_csv(TEST_DATA_DIR / 'test_data_1.csv', index_col='timestamp',
                       parse_dates=True, squeeze=True,
                       date_parser=dparserfunc)
do_S_H_ESD(data,True)
#print(get_file_name_by_time(".jpg"))

# A = np.random.randint(10, size=(3, 4))
# print(A)
# print(A-A[0])
# df = pd.DataFrame(A, columns=list('QRST'))
# print(df.subtract(df['R'], axis=0))


#
#
# df = pd.DataFrame([[np.nan, 2, np.nan, 3],
#                   [3, 4, np.nan, 1],
#                  [np.nan, np.nan, np.nan, 2],
#                 [np.nan, 3, np.nan, 4]],
#                  columns=list('ABCD'))
# series = pd.Series([1.0,2.0],index=[1,2])
# print(series)
# print(df)
#
#
#
# df = df_concat_series(df,series,"E")
# print(df)
#
# df['E']=df['D']-df['E']
# print(df)

# df.to_csv("tt.csv")