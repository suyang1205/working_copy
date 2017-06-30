import csv
import pandas as pd
import math
import datetime


def read_data(Path):
    day_data = pd.read_csv(Path, sep=',', quoting=csv.QUOTE_NONE, low_memory=False)
    return day_data


def output_table(data, Path):
    f = open(Path, "w")
    data.to_csv(Path, sep=',', index=False, header=True)
    f.close()


def trans_to_score(predicts):
    purity_score = []
    for predict in predicts:
        if (predict / (1 - predict)) > 1 * (10 ** (-8)):
            value = 1.0 / (1.0 + math.e ** (-1.07201816474 * math.log(predict / (1 - predict)) + 3.21423064832))
            purity_score.append(value)
        else:
            value = 1 * (10 ** (-8))
            purity_score.append(value)
    return purity_score


def get_fenwei(purity_score, level_nums):
    fenwei = []
    scores = sorted(purity_score)
    length = len(scores)
    bin_length = math.floor(length / level_nums)
    for i in range(level_nums + 1):
        if i == 0:
            fenwei.append("%.14f" % 0.0)
        elif i != level_nums and i != 0:
            pos = i * bin_length
            fenwei.append("%.14f" % scores[pos])
        else:
            fenwei.append("%.14f" % 1.1)
    return fenwei


def get_fenji(purity_score, level_nums):
    fenwei = get_fenwei(purity_score, level_nums)
    grade = []
    for score in purity_score :
        for i in range(len(fenwei)-1):
            if (float(score) >= float(fenwei[level_nums-(i+1)]) and float(score) < float(fenwei[level_nums-i])):
                grade.append(i+1)
                break
    return grade


def main():
    data = read_data('') #补充文件路径
    data['score'] = trans_to_score(data['predict'])
    data['grade'] = get_fenji(data['score'], 20)  #分20级
    data = data[['phone', 'id_number', 'score', 'grade']]
    output_table(data, '') #补充输出路径

