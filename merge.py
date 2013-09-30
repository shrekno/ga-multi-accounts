#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import json
import codecs
import shutil

profile_id = []

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
        print 'no rows'
        return
      elif not u'rows' in l_result:
        l_result[u'rows'] = r_result[u'rows']
        return
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
