#!/usr/bin/python
# -*- coding: utf-8 -*-


import os
import sys
import time
import codecs
import subprocess

def start_proc(command, log_file_path, proc_list):
  if os.path.exists(log_file_path):
    os.remove(log_file_path)
  f = codecs.open(log_file_path, encoding='utf-8', mode='w')
  if sys.platform.find('win') == 0:
    proc_list.append({u'proc':subprocess.Popen(command, stderr=f, stdout=f), u'status':u'running', u'log':log_file_path, u'command':command})
  elif sys.platform.find('linux') == 0:
    proc_list.append({u'proc':subprocess.Popen(command, stderr=f, stdout=f, close_fds=True), u'status':u'running', u'log':log_file_path, u'command':command})
  print proc_list


def process_pump(command_list, retry_limit_count, process_limit_count, time_interval):
  command_index = 0
  processes = []
  check_count = 0
  retry = {}
  while True:
    peek_process_running_state(processes)
    running_count = 0
    for proc in processes:
      if proc[u'status'] == u'running':
        running_count += 1
      elif proc[u'status'] == u'fail':
        if proc[u'command'][2] in retry:
          if retry[proc[u'command'][2]] >= retry_limit_count:
            for proc in processes:
              if proc[u'status'] == u'running':
                proc[u'proc'].terminate()
            print 'Error:', proc[u'command'][2], 'has touch the retry limits.'
            exit(1)
          retry[proc[u'command'][2]] += 1
        else:
          retry[proc[u'command'][2]] = 1
        retry_command = proc[u'command']
        retry_log_file = proc[u'log']
        del processes[processes.index(proc)]
        print u'Retry: ', str(retry_command)
        util.start_proc(retry_command, retry_log_file, processes)
      elif proc[u'status'] == u'succ':
        print u'Run Time:', str((check_count)*time_interval), u'seconds.'
      else:
        print u'Error: Unknown Status -', proc[u'status']
        exit(1)

    if running_count == 0 and len(processes) == len(command_list):
      break

    if running_count < process_limit_count and command_index < len(command_list):
      start_proc(command_list[command_index][u'command'], command_list[command_index][u'log'], processes)
      print 'Start command: ', str(command_list[command_index])
      command_index += 1
      continue
    
    print u'Checking...' + str(check_count)
    time.sleep(time_interval)
    check_count += 1


def peek_process_running_state(proc_list):
  for sp in proc_list:
    if sp[u'status'] == u'running':
      sp[u'proc'].poll()
    if sp[u'proc'].returncode != None:
      if check_log_file(sp[u'log']):
        sp[u'status'] = u'succ'
      else:
        sp[u'status'] = u'fail'


def create_log_file_path(log_dir, file_name_without_suffix):
  if not os.path.exists(log_dir):
    os.makedirs(log_dir)
  return os.path.join(log_dir, u'log_' + file_name_without_suffix + u'.txt')


def check_log_file(log_file_path):
  if os.path.exists(log_file_path):
    f = codecs.open(log_file_path, encoding='utf-8', mode='r')
    line = f.readline()
    while line:
      if line.find(u'All complete.') >= 0:
        f.close()
        return True
      line = f.readline()
    f.close()
    print u'Error:', log_file_path, u'Has Error.'
    return False
  else:
    print u'Error:', log_file_path, u'Is Not Exists.'
    return False
