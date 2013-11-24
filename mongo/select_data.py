#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

import sys
import datetime
import traceback
import pymongo
import json
import bson.json_util


REPEAT = 10


collection = pymongo.Connection('localhost').test.json_docs


def select_once(fields=None):
    start = datetime.datetime.now()
    try:
        count = 0
        for doc in collection.find(fields=fields):
            print(json.dumps(doc, default=bson.json_util.default))
            count += 1
    except:
        traceback.print_exc(file=sys.stderr)
    end = datetime.datetime.now()
    print('selected', count, 'docs in', end - start, file=sys.stderr)
    return count, end - start


def select(fields, label=''):
    print('selecting', label, '...', file=sys.stderr)
    total_rows = 0
    total_time = datetime.timedelta()
    min_time = datetime.timedelta(days=1)
    max_time = datetime.timedelta()
    for i in range(REPEAT):
        (rows, time) = select_once(fields)
        total_rows += rows
        total_time += time
        min_time = min(min_time, time)
        max_time = max(max_time, time)
    print(label, ':', total_time / REPEAT, 'average time of the experiment', file=sys.stderr)
    print(label, ':', min_time, 'min time of the experiment', file=sys.stderr)
    print(label, ':', max_time, 'max time of the experiment', file=sys.stderr)
    print(label, ':', total_time / total_rows, 'average time for a row', file=sys.stderr)


if __name__ == '__main__':
    select(None, 'whole doc')
    select({'_id': False}, 'without _id')
    select({'_id': False, 'data0': True, 'data1': True, 'data2': True, 'data3': True, 'data4': True}, 'half of values')
    # select({'_id': False, 'data0': True}, '1 field')
    # select({'_id': False, 'data0': True, 'data1': True}, '2 fields')
    # select({'_id': False, 'data0': True, 'data1': True, 'data2': True}, '3 fields')
    # select({'_id': False, 'data0': True, 'data1': True, 'data2': True, 'data3': True}, '4 fields')
    # select({'_id': False, 'data0': True, 'data1': True, 'data2': True, 'data3': True, 'data4': True}, '5 fields')
    # select({'_id': False, 'data0': True, 'data1': True, 'data2': True, 'data3': True, 'data4': True, 'data5': True},
    #        '6 fields')
    # select({'_id': False, 'data0': True, 'data1': True, 'data2': True, 'data3': True, 'data4': True, 'data5': True,
    #         'data6': True}, '7 fields')
    # select({'_id': False, 'data0': True, 'data1': True, 'data2': True, 'data3': True, 'data4': True, 'data5': True,
    #         'data6': True, 'data7': True}, '8 fields')
    # select({'_id': False, 'data0': True, 'data1': True, 'data2': True, 'data3': True, 'data4': True, 'data5': True,
    #         'data6': True, 'data7': True, 'data8': True}, '9 fields')
    # select({'_id': False}, '10 fields')
    # select(None, 'whole doc')