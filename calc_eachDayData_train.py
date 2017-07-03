# coding:utf-8
import csv

import pandas as pd
import datetime


clomun_name_list = ['jinjiandate', 'label', 'phone', 'call_times', 'connect_times', 'has_callin', 'has_staff_hangup',
                    'avg_waittime', 'min_waittime', 'max_waittime', 'avg_onlinetime', 'min_onlinetime',
                    'max_onlinetime', 'province','callresult', 'str_zhengxin', 'str_jujie', 'str_zhuce', 'str_mingtian',
                    'str_mendian', 'str_kaolv','str_feilv','str_daka', 'str_guanji', 'emotion', 'weekday',
                    'avg_comments_cnt', 'onlinetime_gap','online_ascending_num','online_decsending_num',
                    'waittime_ascending_num', 'waittime_decsending_num', 'month_nums_in','beta_online',
                    'beta_wait', 'loanamount', 'sex', 'has_car', 'house', 'age', 'level']
output_clomun_name_list = ['label', 'phone','jinjiandate', 'call_times', 'connect_times', 'has_callin',
                           'has_staff_hangup','avg_waittime', 'min_waittime', 'max_waittime', 'avg_onlinetime',
                           'min_onlinetime','max_onlinetime', 'province','callresult', 'str_zhengxin', 'str_jujie',
                           'str_zhuce', 'str_mingtian', 'str_mendian','str_kaolv', 'str_feilv','str_daka', 'str_guanji',
                           'emotion', 'weekday', 'avg_comments_cnt', 'onlinetime_gap','online_ascending_num',
                           'online_decsending_num', 'waittime_ascending_num', 'waittime_decsending_num','month_nums_in',
                           'beta_online','beta_wait', 'loanamount', 'sex', 'has_car', 'house', 'age', 'level']
flag_false = 0
flag_true = 1


def dateRange(start, end, step=1, format="%Y-%m-%d"):
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    days = (strptime(end, format) - strptime(start, format)).days
    return [strftime(strptime(start, format) + datetime.timedelta(i), format) for i in xrange(0, days, step)]


def data_file_name(month):
    if month == 10:
        date_list_10 = dateRange("2016-10-08", "2016-11-01")
        return date_list_10
    if month == 11:
        date_list_11 = dateRange("2016-11-01", "2016-12-01")
        return date_list_11
    if month == 12:
        date_list_12 = dateRange("2016-12-01", "2017-01-03")
        return date_list_12
    if month == 2:
        date_list_2 = dateRange('2017-02-06', '2017-03-01')
        return date_list_2
    if month == 3:
        date_list_3 = dateRange('2017-03-01', '2017-04-03')
        return date_list_3
    if month == 4:
        date_list_4 = dateRange('2017-04-03', '2017-05-02')
        return date_list_4


def output_final(data, Path):
    f = open(Path, "w")
    data.to_csv(Path, sep='\t', index=False, header=True)
    f.close()


def output_result(data, Path):
    title = output_clomun_name_list
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


def transdate(sendtime):
    senddate = []
    for time in sendtime:
        try:
            time = time[0:10]
            senddate.append(time)
        except:
            senddate.append(0)
    return senddate

###### 打标签 #########
def judge_label(calltime, sendtime, start_day):
    label = []
    records = zip(calltime, sendtime)
    for calltime, sendtime in records:
        jinjiandate = str(sendtime)[0:10]
        nowdate = str(calltime)[0:10]
        if nowdate < start_day:
            nowdate = start_day
        internal_days = datediff(nowdate, jinjiandate)
        flag = -1
        if (internal_days <= 30) and (internal_days >= 0):
            flag = 1
        elif internal_days > 30 or internal_days == -1:
            flag = 0
        label.append(flag)
    return label


def judge_label_line(nowdate, jinjiandate):
    internal_days = datediff(nowdate, jinjiandate)
    label = -1
    if (internal_days <= 30) and (internal_days >= 0):
        label = 1
    elif internal_days > 30 or internal_days == -1:
        label = 0
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


####### 抽样函数 ##########
def rand_start_day(start_day_data, start_day, callhisid_dict, internal_num):
    today_boda = start_day_data[start_day_data['calldate'] == start_day]
    follow_day_boda = start_day_data[start_day_data['calldate'] < start_day]
    follow_day_boda.is_copy = False
    follow_day_boda.sort_values(by=['calldate'], ascending=[False], inplace=True)
    final_data = []
    for i in range(len(today_boda)):
        line = today_boda.iloc[i]
        output_line = line[output_clomun_name_list]  ###去掉多余列
        final_data.append(output_line.values)
        temp_line = line[clomun_name_list]
        temp_line['flag'] = flag_false  ###不是n+1天拨打的电话
        callhisid_dict[line['phone']] = temp_line

    for j in range(len(follow_day_boda)):
        line = follow_day_boda.iloc[j]
        if (j + 1) % internal_num == 0:
            output_line = line[output_clomun_name_list]  ###去掉多余列
            final_data.append(output_line.values)
        temp_line = line[clomun_name_list]
        temp_line['flag'] = flag_false  ###不是n+1天拨打的电话
        callhisid_dict[line['phone']] = temp_line
    return callhisid_dict, final_data


def rand_follow_day(now_day, now_day_data, callhisid_dict, internal_num):
    count = 0
    final_data = []
    for i in range(len(now_day_data)):
        line = now_day_data.iloc[i]
        temp_line = line[clomun_name_list]
        temp_line['flag'] = flag_true  ###是n+1天拨打的电话
        callhisid_dict[line['phone']] = temp_line  ##更新或新增数据字典

    ## 遍历字典, 输出n-1天 ##
    for phone in callhisid_dict:
        line_data = callhisid_dict[phone]
        if line_data['flag'] == 1:
            output_line = line_data[output_clomun_name_list]  ###去掉多余列
            final_data.append(output_line.values)
            line_data['flag'] = flag_false  ###不是n+1天拨打的电话
            callhisid_dict[phone] = line_data
        elif line_data['flag'] == 0:
            count += 1
            if count % internal_num == 0:
                line_data['label'] = judge_label_line(now_day, line_data['jinjiandate'])
                output_line = line_data[output_clomun_name_list]  ###去掉多余列
                final_data.append(output_line.values)
    return callhisid_dict, final_data


################## 将的测试数据拆成每天（1.3,2.6,3.1全量数据，根据每月第一天生成后续，标签动态） ################
def sub_eachday(date_list, month, internal_num):
    callhisid_dict = {}

    ######## read data ######
    print 'reading...'
    data = pd.read_csv("../C++_feature/trial", sep='\t', quoting=csv.QUOTE_NONE, low_memory=False)
    # data = pd.read_csv("../C++_feature/jieqing_%s_id" % month, sep='\t', quoting=csv.QUOTE_NONE, low_memory=False)
    print 'done'
    ######## end ###########

    ##### 打标签 ########
    start_day = date_list[0]
    data['calldate'] = map(lambda x: x[0:10], data['calltime'])
    data['jinjiandate'] = transdate(data['sendtime'])
    data['label'] = judge_label(data['calltime'], data['sendtime'], start_day)
    ### data['fk_label'] = judge_fk(data['label'], data['back_time'])  ###放款

    start_day_data = data[data['calldate'] <= start_day]

    ## 输出第一天 ##
    callhisid_dict, start_day_data = rand_start_day(start_day_data, start_day, callhisid_dict, internal_num)
    output_result(start_day_data, './final_%s/%s' % (month, start_day))

    ## 循环处理余下n-1天 ##
    for i in range(1, len(date_list)):
        print date_list[i]
        now_day = date_list[i]
        now_day_data = data[data['calldate'] == now_day]
        callhisid_dict, follow_day_data = rand_follow_day(now_day, now_day_data, callhisid_dict, internal_num)
        output_result(follow_day_data, './final_%s/%s' % (month, now_day))


def main():
    month = 10
    datelist = data_file_name(10)  ###  在这里改月份
    sub_eachday(datelist, month, 10)  ###  改抽取倍数


if __name__ == '__main__':
    main()
