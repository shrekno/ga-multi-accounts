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
import subprocess

from proj import conf

collect_file_name = 'collect.py'
merge_file_name = 'merge.py'

profile_dir = u'./proj/profile'
command_dir = u'./proj/command'
cs_conf_dir = u'./proj/client_secrets'
cs_data_dir = u'./proj/client_secrets'
output_dir = u'./proj/output'
collect_output_dir = u'./proj/output/collect'
merge_output_dir = u'./proj/output/merge'
log_dir = u'./proj/log'
collect_log_dir = u'./proj/log/collect'
merge_log_dir = u'./proj/log/merge'

def start_proc(command, log_file_path, proc_list):
  if os.path.exists(log_file):
    os.remove(log_file)
  f = codecs.open(log_file, encoding='utf-8', mode='w')
  if sys.platform.find('win') == 0:
    proc_list.append({u'proc':subprocess.Popen(command, stderr=f, stdout=f), u'stop':False})
  elif sys.platform.find('linux') == 0:
    proc_list.append({u'proc':subprocess.Popen(command, stderr=f, stdout=f, close_fds=True), u'stop':False})

def check_process_running_state(proc_list, time_interval):
  check_count = 1
  while True:
    time.sleep(time_interval)
    print u'Checking...' + str(check_count)
    check_count += 1
    all_stop = True
    for sp in proc_list:
      if not sp[u'stop']:
        sp[u'proc'].poll()
        all_stop = False
      if sp[u'proc'].returncode != None:
        sp[u'stop'] = True
    if all_stop:
      break
  print u'Run Time:', str((check_count-1)*time_interval), u'Seconds.'

def create_log_file_path(log_dir, file_name_without_suffix):
  if not os.path.exists(log_dir):
    os.makedirs(log_dir)
  return os.path.join(log_dir, u'log_' + file_name_without_suffix + u'.txt')

def check_log(log_dir):
  for profile_file in os.listdir(profile_dir):
    if os.path.isfile(os.path.join(profile_dir, profile_file)) and profile_file.find(u'.txt') > 0:
      log_file = create_log_file_path(log_dir, profile_file[:profile_file.find('.')])
      if os.path.exists(log_file):
        f = codecs.open(log_file, encoding='utf-8', mode='r')
        line = f.readline()
        single_succ = False
        while line:
          if line.find(u'All complete.') >= 0:
            single_succ = True
            break
          line = f.readline()
        f.close()
        if not single_succ:
          print log_file, u'Has Error.'
          exit(1)
      else:
        print log_file, u'Is Not Exists.'
        exit(1)

if __name__ == '__main__':
  # the day before yesterday
  collect_date = datetime.date.today() - datetime.timedelta(days = 1)

  if len(sys.argv) > 1:
    argv_start_index = 1
    reg_date = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
    if not re.match(reg_date, sys.argv[argv_start_index]):
      print u'collect date error, reg:[0-9]{4}-[0-9]{2}-[0-9]{2}'
      exit(1) 
    collect_date_array = sys.argv[argv_start_index].split('-')
    collect_date = datetime.date(int(collect_date_array[0]), int(collect_date_array[1]), int(collect_date_array[2]))

  # collect
  if os.path.exists(os.path.join(collect_log_dir, str(collect_date))):
    shutil.rmtree(os.path.join(collect_log_dir, str(collect_date)))
  collect_processes = []
  for profile_file in os.listdir(profile_dir):
    if os.path.isfile(os.path.join(profile_dir, profile_file)):
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
      comp_command = [u'python', collect_file_name, profile_file_path, cmd_file, cs_conf_file, cs_data_file, output_dir, str(collect_date), str(uuid.uuid4())]
      log_file = create_log_file_path(os.path.join(collect_log_dir, str(collect_date)), profile_file[:profile_file.find('.')])
      start_proc(comp_command, log_file, collect_processes)
      print u'Collect Proc For', profile_file, u'Is Starting.'
      if len(collect_processes) % 5 == 0:
        # wait
        print u'Check Collect Proc Running States.'
        check_process_running_state(collect_processes, 2)
        print u'All Collect Proc Quit.'

  # verify
  print u'Check Collect Result Logs.'
  check_log(os.path.join(collect_log_dir, str(collect_date)))
  print u'All Collect Succ.'

  # merge
  if os.path.exists(os.path.join(merge_log_dir, str(collect_date))):
    shutil.rmtree(os.path.join(merge_log_dir, str(collect_date)))
  merge_processes = []
  for profile_file in os.listdir(profile_dir):
    if os.path.isfile(os.path.join(profile_dir, profile_file)) and profile_file.find(u'.txt') > 0:
      profile_file_path = os.path.join(profile_dir, profile_file)
      data_dir = os.path.join(collect_output_dir, str(collect_date), profile_file[:profile_file.find('.txt')])
      output_dir = os.path.join(merge_output_dir, str(collect_date))
      if not os.path.exists(output_dir):
        os.makedirs(output_dir)
      comp_command = [u'python', merge_file_name, profile_file_path, data_dir, output_dir]
      log_file = create_log_file_path(os.path.join(merge_log_dir, str(collect_date)), profile_file[:profile_file.find('.')])
      start_proc(comp_command, log_file, merge_processes)
      print u'Merge Proc For', profile_file, u'Is Starting.'

  # wait
  print u'Check Merge Proc Running States.'
  check_process_running_state(merge_processes, 1)
  print u'All Merge Proc Quit.'

  # verify
  print u'Check Merge Result Logs.'
  check_log(os.path.join(merge_log_dir, str(collect_date)))
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
  log_file = create_log_file_path(os.path.join(merge_log_dir, str(collect_date), u'last_merge'), u'last_merge')
  start_proc(comp_command, log_file, last_merge_process)
  print u'Last Merge Proc Is Starting.'

  # wait
  print u'Check Last Merge Proc Running States.'
  check_process_running_state(last_merge_process, 1)
  print u'All Merge Proc Quit.'

  # verify
  print u'Check Last Merge Result Log.'
  if os.path.exists(log_file):
    f = codecs.open(log_file, encoding='utf-8', mode='r')
    line = f.readline()
    single_succ = False
    while line:
      if line.find(u'All complete.') >= 0:
        single_succ = True
        break
      line = f.readline()
    f.close()
    if not single_succ:
      print log_file, u'Has Error.'
      exit(1)
  else:
    print log_file, u'Is Not Exists.'
    exit(1)
  print u'Last Merge Succ.'

  print u'All Succ.'
