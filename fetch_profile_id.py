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

def fetch_all_profile_id(service, profile_ids):
  accounts = service.management().accounts().list().execute()
  if accounts.get('items'):
    items = accounts.get('items')
    for item in items:
      ids = item.get('id')
      name = item.get('name')
      print name
      webproperties = service.management().webproperties()\
              .list(accountId=ids).execute()
      if webproperties.get('items'):
        elem = []
        for web_item in webproperties['items']:
          firstWebpropertyId = web_item.get('id')
          profiles = service.management().profiles().list(
            accountId=ids,
            webPropertyId=firstWebpropertyId).execute()
          for profile_item in profiles['items']:
            print profile_item['webPropertyId'], profile_item['id']
            elem.append({profile_item['webPropertyId'] : profile_item['id']})
          profile_ids.append({name : elem})
      else:
          continue

def main(argv):
  argv_start_index = 0
  client_secrets_file_path = argv[argv_start_index]
  token_file_path = argv[argv_start_index+1]
  output_dir_path = argv[argv_start_index+2]

  service = initialize_service(token_file_path, client_secrets_file_path)
  print u'Init succ.'
  try:
    if not os.path.exists(output_dir_path):
      os.makedirs(output_dir_path)
    profile_ids = []
    fetch_all_profile_id(service, profile_ids)
    for account_info in profile_ids:
      all_keys = account_info.keys()
      for key in all_keys:
        out_file_path = os.path.join(output_dir_path, key+u'.txt')
        f = codecs.open(out_file_path, encoding='utf-8', mode='w')
        for e in account_info[key]:
          all_sub_keys = e.keys() 
          for sub_key in all_sub_keys:
            f.write(e[sub_key])
            f.write(u'\r\n')
        f.close()
    print u'\nAll complete.'
  except TypeError, error:
    # Handle errors in constructing a query.
    print (u'There was an error in constructing your query : %s' % error)

  except HttpError, error:
    # Handle API errors.
    print (u'Arg, there was an API error : %s : %s' %
           (error.resp.status, error._get_reason()))

  except AccessTokenRefreshError:
    # Handle Auth errors.
    print (u'The credentials have been revoked or expired, please re-run '
           u'the application to re-authorize')

if __name__ == '__main__':
  if len(argv) < 4:
    print u'agrv error.'
    print u'Usage: fetch_profile_id.py client_secrets_file_path token_file_path output_dir_path'
    return
  main(sys.argv[1:])
