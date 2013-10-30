#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import json
import random
import codecs
import datetime
import argparse
import httplib2

from apiclient import discovery
from oauth2client import file
from oauth2client import client
from oauth2client import tools
from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError


parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter,
    parents=[tools.argparser])


def initialize_service(token_file_path, client_secrets_file_path):
    flow = client.flow_from_clientsecrets(client_secrets_file_path,
                                          scope=[
                                          'https://www.googleapis.com/auth/analytics',
                                          'https://www.googleapis.com/auth/analytics.readonly',
                                          ],
                                          message=tools.message_if_missing(client_secrets_file_path))

    storage = file.Storage(token_file_path)
    credentials = storage.get()
    flags = parser.parse_args([])
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = httplib2.Http()
    http = credentials.authorize(http)
    return discovery.build('analytics', 'v3', http=http)


def parse_data_from_results(out_data, results, cmd):
    if results:
        print u'NetCallBack [', cmd[u'name'], ']'
        data = {}
        data[u'name'] = cmd[u'name']
        data[u'result'] = results
        out_data.append(data)
    else:
        print u'No results found'


def get_results(service,
                _profile_id,
                _start_date,
                _end_date,
                _metrics,
                _dimensions='',
                _filters='',
                _segment='',
                _sort='',
                _max_results='10000',
                _start_index='1',
                _user_uuid=''
                ):
    param = u"""
      ids='ga:' + _profile_id,
      start_date=_start_date,
      end_date=_end_date,
      metrics=_metrics,
      max_results=_max_results,
      start_index=_start_index
  """
    if len(_dimensions) > 0 and _dimensions != u'':
        param += u',dimensions=_dimensions'
    if len(_filters) > 0 and _filters != u'':
        param += u',filters=_filters'
    if len(_segment) > 0 and _segment != u'':
        param += u',segment=_segment'
    if len(_sort) > 0 and _sort != u'':
        param += u',sort=_sort'
    if len(_user_uuid) > 0 and _user_uuid != u'':
        param += u',quotaUser=_user_uuid'
    cmd_pre = u'service.data().ga().get('
    cmd_post = u').execute()'
    cmd = cmd_pre + param + cmd_post
    return eval(cmd)


def get_cmds(cmd_file_path, collect_date):
    cmds = []
    if not os.path.exists(cmd_file_path):
        print cmd_file_path, 'is not exists'
        exit(1)
    f = codecs.open(cmd_file_path, encoding='utf-8', mode='r')
    line = f.readline()
    cmd = {}
    while line:
        if line[-2:] == u'\r\n':
            line = line[:-2]
        if line.find(u'[') >= 0 and line.find(u']') >= 0:
            if u'name' in cmd:
                cmds.append(cmd)
                cmd = {}
            cmd[u'name'] = line[line.find(u'[') + 1: line.find(u']')]
        if line.find(u'_') == 0 and line.find(u'=') >= 0:
            prefix = line[:line.find(u'=')]
            reg_date = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
            if prefix == u'_sta':
                if re.match(reg_date, line[line.find(u'=') + 1:]):
                    cmd[u'start_date'] = line[line.find(u'=') + 1:]
                else:
                    cmd[u'start_date'] = eval(line[line.find(u'=') + 1:])
            if prefix == u'_end':
                if re.match(reg_date, line[line.find(u'=') + 1:]):
                    cmd[u'end_date'] = line[line.find(u'=') + 1:]
                else:
                    cmd[u'end_date'] = eval(line[line.find(u'=') + 1:])
            if prefix == u'_met':
                cmd[u'metrics'] = line[line.find(u'=') + 1:]
            if prefix == u'_dim':
                cmd[u'dimensions'] = line[line.find(u'=') + 1:]
            if prefix == u'_fil':
                cmd[u'filters'] = line[line.find(u'=') + 1:]
            if prefix == u'_seg':
                cmd[u'segment'] = line[line.find(u'=') + 1:]
            if prefix == u'_sor':
                cmd[u'sort'] = line[line.find(u'=') + 1:]
            if prefix == u'_max':
                cmd[u'max_results'] = line[line.find(u'=') + 1:]
            if prefix == u'_ind':
                cmd[u'start_index'] = line[line.find(u'=') + 1:]
        line = f.readline()
    if u'name' in cmd:
        cmds.append(cmd)
    f.close()
    return cmds


def main(argv):
    argv_start_index = 0
    profile = argv[argv_start_index]
    cmd_file_path = argv[argv_start_index + 1]
    client_secrets_file_path = argv[argv_start_index + 2]
    token_file_path = argv[argv_start_index + 3]
    output_dir_path = argv[argv_start_index + 4]

    reg_date = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
    if not re.match(reg_date, argv[argv_start_index + 5]):
        print u'Error: collect date error, reg:[0-9]{4}-[0-9]{2}-[0-9]{2}'
        return
    collect_date_array = argv[argv_start_index + 5].split('-')
    collect_date = datetime.date(int(collect_date_array[0]), int(
        collect_date_array[1]), int(collect_date_array[2]))
    user_uuid = argv[argv_start_index + 6]

    service = initialize_service(token_file_path, client_secrets_file_path)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)

    cmds = get_cmds(cmd_file_path, collect_date)
    print u'Init succ.'
    print u'\nID:', profile
    profile_data = []
    out_file_path = os.path.join(output_dir_path, profile + '.json')
    if os.path.exists(out_file_path):
        os.remove(out_file_path)
    for cmd in cmds:
        print u'--------'
        print u'Start [', cmd[u'name'], ']'
        dimensions = ''
        filters = ''
        segment = ''
        sort = ''
        max_results = ''
        start_index = ''
        if u'dimensions' in cmd:
            dimensions = cmd[u'dimensions']
        if u'filters' in cmd:
            filters = cmd[u'filters']
        if u'segment' in cmd:
            segment = cmd[u'segment']
        if u'sort' in cmd:
            sort = cmd[u'sort']
        if u'max_results' in cmd:
            max_results = cmd[u'max_results']
        if u'start_index' in cmd:
            start_index = cmd[u'start_index']
        if max_results == u'':
            max_results = u'10000'
        if start_index == u'':
            start_index = u'1'
        results = {}
        for n in range(0, 5):
            try:
                results = get_results(service,
                                      profile,
                                      cmd[u'start_date'],
                                      cmd[u'end_date'],
                                      cmd[u'metrics'],
                                      dimensions,
                                      filters,
                                      segment,
                                      sort,
                                      max_results,
                                      start_index,
                                      user_uuid)
                break
            except TypeError, error:
                # Handle errors in constructing a query.
                print (
                    u'Except: There was an error in constructing your query : %s' % error)
                exit(1)

            except HttpError, error:
                # Handle API errors.
                print (u'Except: Arg, there was an API error : %s : %s' %
                       (error.resp.status, error._get_reason()))
                if error.resp.reason in ['userRateLimitExceeded', 'quotaExceeded']:
                    time.sleep((2 ** n) + random.random())

            except AccessTokenRefreshError:
                # Handle Auth errors.
                print (u'Except: The credentials have been revoked or expired, please re-run '
                       u'the application to re-authorize')
                exit(1)
        parse_data_from_results(profile_data, results, cmd)
    f = codecs.open(out_file_path, encoding='utf-8', mode='w')
    f.write(json.JSONEncoder().encode(profile_data))
    f.close()

    print u'\nAll complete.'


if __name__ == '__main__':
    if len(sys.argv) < 7:
        print u'agrv error.'
        print u'Usage: collect.py profile_id cmd_file_path client_secrets_file_path token_file_path output_dir_path collect_date user_uuid'
        exit(1)
    main(sys.argv[1:])
