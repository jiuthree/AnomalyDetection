
from clickhouse_driver import Client

import pandas as pd
import re
import matplotlib.pyplot as plt
from tad import anomaly_detect_ts
import time

client = Client(host='xxx', database='xxx', user='xxx', password='xxx')

#传入sql，返回dataframe对象
def read_sql(sql):
    data, columns = client.execute(sql, columnar=True, with_column_types=True)
    df = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})
    return df

#传入一个dataframe的相关信息，返回一个series
def df_to_series(indexname,columnname,df):
    return  pd.Series(df[columnname].values,index=df[indexname])

#时间处理函数，有可能用的到，str转time
def dparserfunc(date):
    return pd.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')


#真正的计算逻辑，传入一个Series类型的data，后续去默认值处理
#longterm 是一个bool值，如果时间跨度超过一个月，就传True，否则选False
def do_S_H_ESD(data,longterm):
    results = anomaly_detect_ts(data,direction="both",alpha=0.05,e_value=True,max_anoms=0.02,longterm=longterm)




    #画图部分
    f, ax = plt.subplots(2, 1, sharex=True)
    ax[0].plot(data.index, data.values, 'b')
    ax[0].plot(results['anoms'].index, results['anoms'], 'r.')
    ax[0].set_title('Detected Anomalies')
    ax[1].set_xlabel('Time Stamp')
    ax[0].set_ylabel('Count')
    ax[1].plot(results['anoms'].index, results['anoms'], 'r.')
    ax[1].set_ylabel('Anomaly Magnitude')

    plt.savefig(get_file_name_by_time(".jpg"))
    plt.show()



# 给dataframe新加一列，新加的列是根据series里面和dataframe相同的索引来的，没有加上的位置默认值填0，
#  这个函数的用处在于填充dataframe，给数据补上一列标记了是否异常的，如果为0，即默认值，代表没有异常 ，如果不为0，代表进行了填充，有异常
# 最后这个df可以写回clickhouse（要先约定好有没有那一列），或者转成csv
def df_concat_series(df,series,newcolumnname):
    df.loc[series.index,newcolumnname] = series.values
    df = df.fillna(value=0)
    return df

def save_csv(df,filename):
    df.to_csv(filename)

def get_file_name_by_time(filenamesuffix):
    localtime = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    return localtime+filenamesuffix

# 流程梳理： 先写sql，获取dataframe，然后从df中提取出series，即data，把data传入算法，获得结果，是一个series
# 把这个series和最早的df进行拼接，给df加一列，然后将结果保存为csv或者写回clickhouse