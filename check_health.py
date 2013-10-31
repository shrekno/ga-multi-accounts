#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import json
import codecs


def load_json_data(file_path):
    if not os.path.exists(file_path):
        print u'Error:', file_path, 'is not exists.'
        exit(1)
    f = codecs.open(file_path, encoding='utf-8', mode='r')
    line = f.readline()
    data_json = ''
    while line:
        data_json = data_json + line
        line = f.readline()
    return json.loads(data_json)


def check_total_visits(data, account, web_property, summary):
    ret = {}
    for i in xrange(0, len(data)):
        if data[i][u'name'] == u'Track Total Visits':
            result = {}
            if u'result' in data[i]:
                result = data[i][u'result']
            else:
                ret[data[i][u'name']] = u'NoResult'
                continue
            if (u'rows' in result):
                if len(result[u'rows']) == 1 and len(result[u'rows'][0]) == 1:
                    total_visits = int(result[u'rows'][0][0])
                    if total_visits <= 5000:
                        ret[data[i][u'name']] = u'Fine'
                    elif total_visits > 5000 and int(result[u'rows'][0][0]) <= 8000:
                        ret[data[i][u'name']] = u'Danger'
                        print u'Danger:', total_visits, u'in', account, web_property
                        summary[u'detail'][u'check_total_visits'] = u'Danger: ' + str(
                            total_visits) + u' in ' + account + u' ' + web_property
                    elif total_visits > 8000:
                        ret[data[i][u'name']] = u'Error'
                        print u'Error:', total_visits, u'in', account, web_property
                        summary[u'detail'][u'check_total_visits'] = u'Error: ' + str(
                            total_visits) + u' in ' + account + u' ' + web_property
            else:
                ret[data[i][u'name']] = u'NoRows'
            break
    return ret


def check_sampled(data, account, web_property, summary):
    ret = {}
    for i in xrange(0, len(data)):
        result = {}
        if u'result' in data[i]:
            result = data[i][u'result']
        else:
            ret[data[i][u'name']] = u'NoResult'
            continue
        if u'containsSampledData' in result:
            if result[u'containsSampledData']:
                ret[data[i][u'name']] = u'Error'
                print u'Error in', account, web_property
                summary[u'detail'][u'check_sampled'] = u'Error in ' + \
                    account + u' ' + web_property
            else:
                ret[data[i][u'name']] = u'Fine'
        else:
            ret[data[i][u'name']] = u'NoContainsSampledData'
    return ret


def main(argv):
    collect_json_dir = argv[0]
    output_file_path = argv[1]
    summarizing_file_path = argv[2]

    if not os.path.exists(collect_json_dir):
        print collect_json_dir, u'is not exist.'
        exit(1)

    check_funcs = [check_sampled, check_total_visits]

    health_status = {}
    for f in os.listdir(collect_json_dir):
        account_dir = os.path.join(collect_json_dir, f)
        if os.path.isdir(account_dir):
            health_status[f] = {}
            for f1 in os.listdir(account_dir):
                web_property_file = os.path.join(account_dir, f1)
                if os.path.isfile(web_property_file) and f1.find(u'.json') > 0:
                    health_status[f][f1] = {}
        else:
            print account_dir, u'is not a dir.'
            continue

    summarizing = {u'detail': {}}
    for func in check_funcs:
        summarizing[func.__name__] = {}
        for account_key in health_status.keys():
            for web_property_key in health_status[account_key].keys():
                data = load_json_data(
                    os.path.join(collect_json_dir, account_key, web_property_key))
                health_status[account_key][web_property_key][func.__name__] = func(
                    data, account_key, web_property_key, summarizing)
                column = health_status[account_key][
                    web_property_key][func.__name__]
                for key in column.keys():
                    if column[key] in summarizing[func.__name__]:
                        summarizing[func.__name__][column[key]] += 1
                    else:
                        summarizing[func.__name__][column[key]] = 1
    print summarizing

    f = codecs.open(summarizing_file_path, encoding='utf-8', mode='w')
    f.write(json.JSONEncoder().encode(summarizing))
    f.close()

    f = codecs.open(output_file_path, encoding='utf-8', mode='w')
    f.write(json.JSONEncoder().encode(health_status))
    f.close()


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print u'agrv error.'
        print u'Usage: check_health.py collect_json_dir_path output_file_path summary_file_path'
        exit(1)
    main(sys.argv[1:])
