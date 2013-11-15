#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import re
import sys
import uuid
import codecs
import datetime

import util


def get_profile_ids(profile_id_file_path):
    profile_id = []
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
    argv_start_index = 0
    collect_file_name = argv[argv_start_index]
    profile_id_file_path = argv[argv_start_index + 1]
    cmd_file_path = argv[argv_start_index + 2]
    client_secrets_file_path = argv[argv_start_index + 3]
    token_file_path = argv[argv_start_index + 4]
    collect_log_dir = argv[argv_start_index + 5]
    output_dir_path = argv[argv_start_index + 6]
    reg_date = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
    if not re.match(reg_date, argv[argv_start_index + 7]):
        print u'Error: collect date error, reg:[0-9]{4}-[0-9]{2}-[0-9]{2}'
        return
    collect_date_array = argv[argv_start_index + 7].split('-')
    collect_date = datetime.date(int(collect_date_array[0]), int(
        collect_date_array[1]), int(collect_date_array[2]))

    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)

    profile_id = get_profile_ids(profile_id_file_path)

    process_commands = []
    for profile in profile_id:
        comp_command = [
            u'python', collect_file_name, profile, cmd_file_path, client_secrets_file_path,
            token_file_path, output_dir_path, str(collect_date), str(uuid.uuid4())]
        profile_file = os.path.basename(profile_id_file_path)
        log_file = util.create_log_file_path(
            os.path.join(collect_log_dir, str(collect_date), profile_file[:profile_file.find('.')]), profile)
        process_commands.append({u'command': comp_command, u'log': log_file})

    print u'len of process command: ', len(process_commands)
    util.process_pump(process_commands, 1, 3, 2)
    print u'\nAll complete.'


if __name__ == '__main__':
    if len(sys.argv) < 8:
        print u'agrv error.'
        print u'Usage: collect.py collect_web_property_file_path profile_id_file_path cmd_file_path client_secrets_file_path token_file_path log_dir output_dir_path collect_date'
        exit(1)
    main(sys.argv[1:])
