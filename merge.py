#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import json
import codecs
import shutil

profile_id = []

def pre_avg_op(lval, rval):
  return lval * rval

def post_avg_op(lval, rval):
  if rval - 0 < 1:
    print u'zero!'
    exit(1)
  return lval / rval

def avg_process(dat_arr, op):
  for dat in dat_arr:
    avg_result = dat[u'result']
    if u'rows' in avg_result:
        # find avg data member
        avg_data_flag = False
        avg_rows = []
        avg_heads = avg_result[u'columnHeaders']
        for head in avg_heads:
          if head[u'name'] == u'ga:avgEventValue':
            avg_data_flag = True
            avg_rows = avg_result[u'rows']
            break
        if avg_data_flag:
          # find total events data member
          event_data_flag = False
          event_rows = []
          evnet_heads = []
          avg_query = avg_result[u'query']
          for event_dat in dat_arr:
            event_result = event_dat[u'result']
            if u'rows' in event_result:
              event_query = event_result[u'query']
              start_date_name = u'start-date'
              end_date_name = u'end-date'
              dimensions_name = u'dimensions'
              filters_name = u'filters'
              metrics_name = u'metrics'
              if avg_query[start_date_name] == event_query[start_date_name] and \
                avg_query[end_date_name] == event_query[end_date_name] and \
                avg_query[filters_name] == event_query[filters_name] and \
                len(avg_query[metrics_name]) == len(event_query[metrics_name]):
                if dimensions_name in avg_query and dimensions_name in event_query:
                  if not avg_query[dimensions_name] == event_query[dimensions_name]:
                      continue
                metrics_same = True
                for m in avg_query[metrics_name]:
                  if (not m == u'ga:avgEventValue') and \
                    (not m in event_query[metrics_name]):
                    metrics_same = False
                    break
                  elif m == u'ga:avgEventValue' and (not u'ga:totalEvents' in event_query[metrics_name]):
                    metrics_same = False
                    break
                if metrics_same:
                  event_data_flag = True
                  event_rows = event_result[u'rows']
                  event_heads = event_result[u'columnHeaders']
                  break
            else:
              continue

          if event_data_flag:
            total_events_map = {}
            event_dimensions_count = 0
            total_events_index = 0
            for head in event_heads:
              if head[u'columnType'] == u'DIMENSION':
                event_dimensions_count += 1
              if head[u'name'] == u'ga:totalEvents':
                break
              total_events_index += 1

            for row in event_rows:
              key_name = u''
              for index in xrange(0, event_dimensions_count):
                key_name += row[index]
              total_events_map[key_name] = row[total_events_index]

            if not len(total_events_map) == len(avg_rows):
              print u'data error 1 in avg_pre_process'
              exit(1)

            avg_dimensions_count = 0
            avg_event_value_index = 0
            for head in avg_heads:
              if head[u'columnType'] == u'DIMENSION':
                avg_dimensions_count += 1
              if head[u'name'] == u'ga:avgEventValue':
                break
              avg_event_value_index += 1

            print dat[u'name']
            for row in avg_rows:
              key_name = u''
              for index in xrange(0, avg_dimensions_count):
               key_name += row[index]
              if not key_name in total_events_map:
                print u'data error 2 in avg_pre_process'
                exit(1)
              print float(row[avg_event_value_index]), u'op' ,float(total_events_map[key_name]), u'=', 
              row[avg_event_value_index] = str(op(float(row[avg_event_value_index]), float(total_events_map[key_name])))
              print row[avg_event_value_index]
              print row
    else:
        continue


def get_profile_ids(profile_id_file_path):
  if not os.path.exists(profile_id_file_path):
    print profile_id_file_path, u'is not exists'
    exit(1)
  f = codecs.open(profile_id_file_path, encoding='utf-8', mode='r')
  line = f.readline()
  while line:
    if line[-2:] == u'\r\n':
      line = line[:-2]
    profile_id.append(line)
    line = f.readline()
  f.close()

def two_way_merge(left, right):
  if len(left) == 0:
    exit(1)
  if len(right) == 0:
    print 'right is empty'
    return
  avg_process(left, pre_avg_op)
  avg_process(right, pre_avg_op)
  for i in xrange(0, len(left)):
    l_result = {}
    r_result = {}
    if left[i]['name'] == right[i]['name']:
      if u'result' in left[i]:
        l_result = left[i][u'result']
      else:
        print u'left no result'
        exit(1)
      if u'result' in right[i]:
        r_result = right[i][u'result']
      else:
        print u'right no result'
        exit(1)
      if (u'containsSampledData' in l_result) and (u'containsSampledData' in r_result):
        l_result[u'containsSampledData'] = l_result[u'containsSampledData'] or r_result[u'containsSampledData']
        if l_result[u'containsSampledData']:
          print u'sampled data!'
          exit(1)
      else:
        print u'result error'
        exit(1)
      dimensions_count = 0
      if u'columnHeaders' in l_result:
        for item in l_result[u'columnHeaders']:
          if item[u'columnType'] == u'DIMENSION':
            dimensions_count += 1
          else:
            break
      if not u'rows' in r_result:
        print left[i]['name'], 'no rows'
        continue
      elif not u'rows' in l_result:
        l_result[u'rows'] = r_result[u'rows']
        continue
      else:
        if dimensions_count == 0:
          for index in xrange(0, len(l_result[u'rows'][0])):
            metric_data_type = l_result[u'columnHeaders'][index]['dataType']
            if metric_data_type == u'CURRENCY' or metric_data_type == u'FLOAT' or metric_data_type == u'PERCENT' or metric_data_type == u'US_CURRENCY':
              l_result[u'rows'][0][index] = str(float(l_result[u'rows'][0][index]) + float(r_result[u'rows'][0][index]))
            elif metric_data_type == u'INTEGER':
              l_result[u'rows'][0][index] = str(int(l_result[u'rows'][0][index]) + int(r_result[u'rows'][0][index]))
            else: # TIME or other
              if l_result[u'rows'][0][index] != r_result[u'rows'][0][index]:
                l_result[u'rows'][0][index] = l_result[u'rows'][0][index] + u' | ' + r_result[u'rows'][0][index]
        else:
          dimensions_map = {}
          for dim in l_result[u'rows']:
            key_name = u''
            for dim_index in xrange(0, dimensions_count):
              key_name += dim[dim_index]
            dimensions_map[key_name] = dim
          for dim in r_result[u'rows']:
            key_name = u''
            for dim_index in xrange(0, dimensions_count):
              key_name += dim[dim_index]
            if key_name in dimensions_map:
              for metric_index in xrange(dimensions_count, len(dimensions_map[key_name])):
                metric_data_type = l_result[u'columnHeaders'][metric_index]['dataType']
                if metric_data_type == u'CURRENCY' or metric_data_type == u'FLOAT' or metric_data_type == u'PERCENT' or metric_data_type == u'US_CURRENCY':
                  dimensions_map[key_name][metric_index] =  str(float(dimensions_map[key_name][metric_index]) + float(dim[metric_index]))
                elif metric_data_type == u'INTEGER':
                  dimensions_map[key_name][metric_index] =  str(int(dimensions_map[key_name][metric_index]) + int(dim[metric_index]))
                else: # TIME or other
                  if dimensions_map[key_name][metric_index] != dim[metric_index]:
                    dimensions_map[key_name][metric_index] = dimensions_map[key_name][metric_index] + ' | ' + dim[metric_index]
                    continue
            else:
              l_result[u'rows'].append(dim)
    else:
      print u'data error'
      exit(1)
  avg_process(left, post_avg_op)

def load_json_data(file_path):
  if not os.path.exists(file_path):
    print file_path, 'is not exists.'
    exit(1)
  f = codecs.open(file_path, encoding='utf-8', mode='r')
  line = f.readline()
  data_json = ''
  while line:
    data_json = data_json + line
    line = f.readline()
  return json.loads(data_json)

def main(argv):
  if len(argv) < 3:
    print u'agrv error.'
    print u'Usage: merge.py profile_id_file_path data_dir_path output_dir_path'
    return

  argv_start_index = 0
  if __name__ == '__main__': 
    argv_start_index = 1
  profile_id_file_path = argv[argv_start_index]
  data_dir_path = argv[argv_start_index+1]
  output_dir_path = argv[argv_start_index+2]

  if not os.path.exists(output_dir_path):
    os.makedirs(output_dir_path)

  get_profile_ids(profile_id_file_path)
  if not len(profile_id) > 0:
    print u'no profile'
    exit(1)

  profile_id_file_name = os.path.basename(profile_id_file_path)
  output_file_path = os.path.join(output_dir_path,  profile_id_file_name[:profile_id_file_name.find(u'.txt')] + u'.json')
  if os.path.exists(output_file_path):
    os.remove(output_file_path)

  init_data_file_index = 0
  init_data_file_path = ''
  for profile_index in xrange(0, len(profile_id)): 
    init_data_file_index = profile_index
    init_data_file_path = os.path.join(data_dir_path, profile_id[init_data_file_index] + u'.json')
    merged_data = load_json_data(init_data_file_path)
    if len(merged_data) != 0: 
      break
  print init_data_file_path
  merge_queue = profile_id[init_data_file_index+1:]
  if len(merge_queue) == 0:
    if not os.path.exists(init_data_file_path):
      print init_data_file_path, u'is not exists.'
      exit(1)
    shutil.copyfile(init_data_file_path, output_file_path)
  else:
    for profile in merge_queue:
      data_file_path = os.path.join(data_dir_path, profile + u'.json')
      print data_file_path
      need_merge_data = load_json_data(data_file_path)
      two_way_merge(merged_data, need_merge_data)
    f = codecs.open(output_file_path, encoding='utf-8', mode='w')
    f.write(json.JSONEncoder().encode(merged_data))
    f.close()

  print u'\nAll complete.'

if __name__ == '__main__':
  main(sys.argv)
