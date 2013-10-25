#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import re
import sys
import uuid
import time
import codecs
import shutil
import datetime

import util
import collect
from proj import conf


def main(argv):
  current_dir = os.path.dirname(__file__)
  collect_file_name = os.path.join(current_dir, u'collect.py')
  merge_file_name = os.path.join(current_dir, u'merge.py')
  profile_dir = os.path.join(current_dir, u'proj/profile')
  command_dir = os.path.join(current_dir, u'proj/command')
  cs_conf_dir = os.path.join(current_dir, u'proj/client_secrets')
  cs_data_dir = os.path.join(current_dir, u'proj/client_secrets')
  output_dir = os.path.join(current_dir, u'proj/output')
  collect_output_dir = os.path.join(current_dir, u'proj/output/collect')
  merge_output_dir = os.path.join(current_dir, u'proj/output/merge')
  log_dir = os.path.join(current_dir, u'proj/log')
  collect_log_dir = os.path.join(current_dir, u'proj/log/collect')
  merge_log_dir = os.path.join(current_dir, 'proj/log/merge')

  # the day before yesterday
  collect_date = datetime.date.today() - datetime.timedelta(days = 2)

  if len(argv) > 0:
    argv_start_index = 0
    reg_date = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
    if not re.match(reg_date, argv[argv_start_index]):
      print u'collect date error, reg:[0-9]{4}-[0-9]{2}-[0-9]{2}'
      exit(1)
    collect_date_array = argv[argv_start_index].split('-')
    collect_date = datetime.date(int(collect_date_array[0]), int(collect_date_array[1]), int(collect_date_array[2]))

  # collect
  if os.path.exists(os.path.join(collect_log_dir, str(collect_date))):
    shutil.rmtree(os.path.join(collect_log_dir, str(collect_date)))
  for profile_file in os.listdir(profile_dir):
    if os.path.isfile(os.path.join(profile_dir, profile_file)) and profile_file.find(u'.txt') > 0:
      cs_file_name = u'cs_1'
      if profile_file in conf._cs_conf:
        cs_file_name = conf._cs_conf[profile_file]
      cs_conf_file = os.path.join(cs_conf_dir, cs_file_name + u'.json')
      cs_data_file = os.path.join(cs_data_dir, cs_file_name + u'.dat')
      cmd_file = os.path.join(command_dir, u'cmd.txt')
      profile_file_path = os.path.join(profile_dir, profile_file)
      output_dir = os.path.join(collect_output_dir, str(collect_date), profile_file[:profile_file.find('.txt')])
      if not os.path.exists(output_dir):
        os.makedirs(output_dir)
      print u'Start Collect', profile_file
      collect.main(profile_file_path, cmd_file, cs_conf_file, cs_data_file, output_dir, str(collect_date), str(uuid.uuid4()))
  print u'All Collect Succ.'

  # merge
  if os.path.exists(os.path.join(merge_log_dir, str(collect_date))):
    shutil.rmtree(os.path.join(merge_log_dir, str(collect_date)))
  merge_commands = []
  for profile_file in os.listdir(profile_dir):
    if os.path.isfile(os.path.join(profile_dir, profile_file)) and profile_file.find(u'.txt') > 0:
      profile_file_path = os.path.join(profile_dir, profile_file)
      data_dir = os.path.join(collect_output_dir, str(collect_date), profile_file[:profile_file.find('.txt')])
      output_dir = os.path.join(merge_output_dir, str(collect_date))
      if not os.path.exists(output_dir):
        os.makedirs(output_dir)
      comp_command = [u'python', merge_file_name, profile_file_path, data_dir, output_dir]
      log_file = util.create_log_file_path(os.path.join(merge_log_dir, str(collect_date)), profile_file[:profile_file.find('.')])
      merge_commands.append({u'command': comp_command, u'log': log_file})

  print u'Start Merge'
  util.process_pump(merge_commands, 3, 5, 0.5)
  print u'All Merge Succ.'

  # last merge
  merged_data_dir = os.path.join(merge_output_dir, str(collect_date))
  last_merge_dir = os.path.join(merge_output_dir, str(collect_date), u'last_merge')
  if os.path.exists(last_merge_dir):
    shutil.rmtree(last_merge_dir)
  os.makedirs(last_merge_dir)
  last_merge_index_file_path = os.path.join(last_merge_dir, u'last_merge.txt')
  if os.path.exists(last_merge_index_file_path):
    os.remove(last_merge_index_file_path)
  f = codecs.open(last_merge_index_file_path, encoding='utf-8', mode='w')
  for merged_file in os.listdir(merged_data_dir):
    if os.path.isfile(os.path.join(merged_data_dir, merged_file)) and merged_file.find(u'.json') > 0:
      f.write(merged_file[:merged_file.find(u'.')])
      f.write(u'\r\n')
  f.close()

  if os.path.exists(os.path.join(merge_log_dir, str(collect_date), u'last_merge')):
    shutil.rmtree(os.path.join(merge_log_dir, str(collect_date), u'last_merge'))
  last_merge_process = []
  comp_command = [u'python', merge_file_name, last_merge_index_file_path, merged_data_dir, last_merge_dir]
  log_file = util.create_log_file_path(os.path.join(merge_log_dir, str(collect_date), u'last_merge'), u'last_merge')
  print u'Start Last Merge'
  util.process_pump([{u'command': comp_command, u'log': log_file}], 3, 5, 0.5)
  print u'Last Merge Succ.'

  print u'All Succ.'


if __name__ == '__main__':
  if len(sys.argv) > 1:
    main(sys.argv[1:])
  else:
    main([])
