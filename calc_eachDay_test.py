# coding:utf-8
import csv

import pandas as pd
import datetime

def dateRange(start, end, step=1, format="%Y-%m-%d"):
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    days = (strptime(end, format) - strptime(start, format)).days
    return [strftime(strptime(start, format) + datetime.timedelta(i), format) for i in xrange(0, days, step)]


def data_file_name(month):
    date_list_2 =  dateRange("2017-02-06", "2017-03-01")
    date_list_3 = dateRange("2017-03-01", "2017-04-03")
    date_list_4 = dateRange("2017-04-03", "2017-05-02")
    date_list_6 = dateRange("2017-06-01", "2017-06-13")

    if month == 2:
        return date_list_2
    if month == 3:
        return date_list_3
    if month == 4:
        return date_list_4
    if month == 6:
        return date_list_6


def output_final(data, Path):
    f = open(Path, "w")
    data.to_csv(Path, sep='\t', index=False, header=True)
    f.close()


def output_result(data, Path):
    title = ['label', 'phone', 'fk_label', 'call_times', 'connect_times', 'has_callin', 'has_staff_hangup',
             'avg_waittime',
             'min_waittime', 'max_waittime', 'avg_onlinetime', 'min_onlinetime', 'max_onlinetime', 'province',
             'callresult',
             'str_zhengxin', 'str_jujie', 'str_zhuce', 'str_mingtian', 'str_mendian', 'str_kaolv', 'str_feilv',
             'str_daka', 'str_guanji',
             'emotion', 'weekday', 'avg_comments_cnt', 'onlinetime_gap', 'online_ascending_num',
             'online_decsending_num',
             'waittime_ascending_num', 'waittime_decsending_num', 'month_nums_in', 'beta_online', 'beta_wait',
             'loanamount', 'sex', 'has_car', 'house',  'age', 'level']
    with open(Path, 'w') as output:
        output.write('%s\n' % '\t'.join(map(lambda x: str(x), title)))
        for item in data:
            output.write('%s\n' % '\t'.join(map(lambda x: str(x), item)))


######### 将字符串转换成datetime类型
def strtodatetime(datestr, format):
    return datetime.datetime.strptime(datestr, format)


######### 计算两个日期间相差的天数
def datediff(beginDate, endDate):
    format = "%Y-%m-%d"
    try:
        bd = strtodatetime(beginDate, format)
        ed = strtodatetime(endDate, format)
    except:
        return -1
    oneday = datetime.timedelta(days=1)
    count = 0
    if (bd > ed):
        count = -1
    while bd <= ed:
        ed = ed - oneday
        count += 1
    return count


###### 打标签 #########
def judge_label(calltime, sendtime):
    label = []
    records = zip(calltime, sendtime)
    for calltime, sendtime in records:
        jinjiandate = str(sendtime)[0:10]
        nowdate = str(calltime)[0:10]
        internal_days = datediff(nowdate, jinjiandate)
        flag = -1
        if (internal_days <= 30) and (internal_days >= 0):
            flag = 1
        elif internal_days > 30 or internal_days == -1:
            flag = 0
        label.append(flag)

    return label


####### 标记放款 ######
def istime(fktime):
    date = str(fktime)[0:10]
    format = "%Y-%m-%d"
    try:
        strtodatetime(date, format)
        return True
    except:
        return False


def judge_fk(label, fktime):
    fk = []
    records = zip(label, fktime)
    for label, fktime in records:
        if (label == 1) and (istime(fktime) is True):
            fk_label = 1
        else:
            fk_label = 0
        fk.append(fk_label)
    return fk


################## 将的测试数据拆成每天（1.3,2.6,3.1全量数据，根据每月第一天生成后续，标签动态） ################
def sub_eachday(date_list, month):
    callhisid_dict = {}

    ######## read data ######
    print 'reading...'
    data = pd.read_csv("/Users/yangsu/Documents/scorecard/dianxiao/C++_feature/test/jieqing_%s" % (month), sep='\t', quoting=csv.QUOTE_NONE,
                       low_memory=False)
    print 'done'
    ######## end ###########

    ##### 打标签 ########
    data['calldate'] = map(lambda x: x[0:10], data['calltime'])
    data['label'] = judge_label(data['calltime'], data['sendtime'])
    data['fk_label'] = judge_fk(data['label'], data['back_time'])

    start_day = date_list[0]
    start_day_data = data[data['calldate'] == start_day]

    start_day_data = start_day_data[['label', 'phone', 'fk_label', 'call_times', 'connect_times', 'has_callin', 'has_staff_hangup',
                 'avg_waittime','min_waittime', 'max_waittime', 'avg_onlinetime', 'min_onlinetime', 'max_onlinetime', 'province',
                 'callresult','str_zhengxin', 'str_jujie', 'str_zhuce', 'str_mingtian', 'str_mendian', 'str_kaolv', 'str_feilv',
                 'str_daka', 'str_guanji','emotion', 'weekday', 'avg_comments_cnt', 'onlinetime_gap', 'online_ascending_num',
                 'online_decsending_num','waittime_ascending_num', 'waittime_decsending_num', 'month_nums_in', 'beta_online',
                 'beta_wait', 'loanamount', 'sex', 'has_car', 'house',  'age', 'level']]


    ## 输出第一天 ##
    output_final(start_day_data, '/Users/yangsu/Documents/scorecard/dianxiao/feature_data/final_%s/%s' % (month, start_day))

    for i in range(len(start_day_data)):
        line = start_day_data.iloc[i]
        callhisid_dict[line['phone']] = line.values  ## 记录当天的拨打过的phone数据
    print 'len of first day',len(callhisid_dict)



    ## 循环处理余下n-1天 ##
    for i in range(1, len(date_list)):
        print date_list[i]
        now_day = date_list[i]
        now_day_data = data[data['calldate'] == now_day]
        for i in range(len(now_day_data)):
            line = now_day_data.iloc[i]
            line = line[['label', 'phone', 'fk_label', 'call_times', 'connect_times', 'has_callin', 'has_staff_hangup',
                         'avg_waittime','min_waittime', 'max_waittime', 'avg_onlinetime', 'min_onlinetime', 'max_onlinetime',
                         'province', 'callresult','str_zhengxin', 'str_jujie', 'str_zhuce', 'str_mingtian', 'str_mendian', 'str_kaolv',
                         'str_feilv', 'str_daka', 'str_guanji','emotion', 'weekday', 'avg_comments_cnt', 'onlinetime_gap', 'online_ascending_num',
                         'online_decsending_num','waittime_ascending_num', 'waittime_decsending_num', 'month_nums_in', 'beta_online',
                         'beta_wait', 'loanamount', 'sex', 'has_car', 'house',  'age', 'level']]
            callhisid_dict[line['phone']] = line.values  ##更新或新增数据字典
        print  len(callhisid_dict)

        ## 输出n-1天 ##
        final_data = []
        for phone in callhisid_dict:
            line_data = callhisid_dict[phone].tolist()
            final_data.append(line_data)

        output_result(final_data,  '/Users/yangsu/Documents/scorecard/dianxiao/feature_data/final_%s/%s' % (month,now_day))


def main():

    month = 4###  在这里改月份
    date_list = data_file_name(month)
    sub_eachday(date_list, month)


if __name__ == '__main__':
    main()
