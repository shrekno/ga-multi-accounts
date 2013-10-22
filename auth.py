#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
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

cmds = []
profile_id = []
parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter,
    parents=[tools.argparser])

def auth(token_file_path, client_secrets_file_path, parameters):
  if not os.path.exists(client_secrets_file_path):
    print client_secrets_file_path, u'is not exists.'
    exit(1)
  print client_secrets_file_path
  flow = client.flow_from_clientsecrets(client_secrets_file_path,
    scope=[
        'https://www.googleapis.com/auth/analytics',
        'https://www.googleapis.com/auth/analytics.readonly',
      ],
    message=tools.message_if_missing(client_secrets_file_path))

  storage = file.Storage(token_file_path)
  credentials = storage.get()
  flags = parser.parse_args(parameters)
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)

def main(argv):
  argv_start_index = 0
  client_secrets_dir_path = argv[argv_start_index]
  output_token_dir_path = argv[argv_start_index+1]
  other_param = argv[argv_start_index+2:]

  if not os.path.exists(client_secrets_dir_path):
    print client_secrets_dir_path, u'is not exists.'
    exit(1)
  if not os.path.exists(output_token_dir_path):
    os.makedirs(output_token_dir_path)

  for cs_file in os.listdir(client_secrets_dir_path):
    if os.path.isfile(os.path.join(client_secrets_dir_path, cs_file)) and cs_file.find(u'.json') > 0:
      token_file_path = os.path.join(client_secrets_dir_path, cs_file[:cs_file.find(u'.json')] + u'.dat')
      client_secrets_file_path = os.path.join(client_secrets_dir_path, cs_file)
      auth(os.path.abspath(token_file_path), os.path.abspath(client_secrets_file_path), other_param)

  print u'\nAll complete.'

if __name__ == '__main__':
  if len(sys.argv) < 3:
    print u'agrv error.'
    print u'Usage: auth.py client_secrets_dir_path output_token_dir_path [other parameters for run_flow()]'
    exit(1)
  main(sys.argv[1:])
