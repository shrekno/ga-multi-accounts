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

import util


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
  return profile_id


def main(argv):
  current_dir = os.path.dirname(__file__)
  collect_file_name = os.path.join(current_dir, u'collect_web_property.py')

  argv_start_index = 0
  profile_id_file_path = argv[argv_start_index]
  cmd_file_path = argv[argv_start_index +1]
  client_secrets_file_path = argv[argv_start_index+2]
  token_file_path = argv[argv_start_index+3]
  output_dir_path = argv[argv_start_index+4]
  reg_date = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
  if not re.match(reg_date, argv[argv_start_index+5]):
    print u'Error: collect date error, reg:[0-9]{4}-[0-9]{2}-[0-9]{2}'
    return
  collect_date_array = argv[argv_start_index+5].split('-')
  collect_date = datetime.date(int(collect_date_array[0]), int(collect_date_array[1]), int(collect_date_array[2]))
  user_uuid = argv[argv_start_index+6]
  if not os.path.exists(output_dir_path):
    os.makedirs(output_dir_path)

  profile_id = get_profile_ids(profile_id_file_path)

  process_commands = []
  for profile in profile_id:
    comp_command = [u'python', collect_file_name, profile, cmd_file, cs_conf_file, cs_data_file, output_dir, str(collect_date), str(uuid.uuid4())]
    log_file = util.create_log_file_path(os.path.join(collect_log_dir, str(collect_date), profile_file[:profile_file.find('.')]), profile)
    process_commands.append({u'command': comp_command, u'log': log_file})

  print u'len of process command: ', len(process_commands)
  util.process_pump(process_commands, 3, 5, 2)
  print u'\nAll complete.'


if __name__ == '__main__':
  if len(sys.argv) < 7:
    print u'agrv error.'
    print u'Usage: collect.py profile_id_file_path cmd_file_path client_secrets_file_path token_file_path output_dir_path collect_date user_uuid'
    exit(1)
  main(sys.argv[1:])
