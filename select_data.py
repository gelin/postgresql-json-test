#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

import sys
import datetime
import json
import psycopg2 as db
import traceback


PGHOST = 'localhost'
PGUSER = 'gelin'
PGPASSWORD = 'gelin'
PGDATABASE = 'json_test'

REPEAT = 10


connection = db.connect(host=PGHOST, user=PGUSER, password=PGPASSWORD, database=PGDATABASE)


def select_once(query, format=None):
    if not format:
        format = lambda row: row
    start = datetime.datetime.now()
    try:
        count = 0
        cursor = connection.cursor()
        cursor.execute(query)
        for row in cursor:
            print(format(row))
            count += 1
        cursor.close()
        connection.commit()
    except:
        traceback.print_exc(file=sys.stderr)
    end = datetime.datetime.now()
    print('selected', count, 'rows in', end - start, file=sys.stderr)
    return count, end - start


def select(query, format=None, label=''):
    print('selecting', label, '...', file=sys.stderr)
    total_rows = 0
    total_time = datetime.timedelta()
    min_time = datetime.timedelta(days=1)
    max_time = datetime.timedelta()
    for i in range(REPEAT):
        (rows, time) = select_once(query, format)
        total_rows += rows
        total_time += time
        min_time = min(min_time, time)
        max_time = max(max_time, time)
    print(label, ':', total_time / REPEAT, 'average time of the experiment', file=sys.stderr)
    print(label, ':', min_time, 'min time of the experiment', file=sys.stderr)
    print(label, ':', max_time, 'max time of the experiment', file=sys.stderr)
    print(label, ':', total_time / total_rows, 'average time for a row', file=sys.stderr)


def get_json(row):
    return row[0]


def format_json(row):
    obj = {}
    idx = 0
    for value in row:
        obj['data%i' % idx] = value
        idx += 1
    return json.dumps(obj)


if __name__ == '__main__':
    select('SELECT * FROM test_values;', label='all values as values')
    select('SELECT data FROM test_json;', get_json, label='all json as json')
    select('SELECT * FROM test_values;', format_json, label='all values as json (serialization)')
    select('SELECT row_to_json(test_values) FROM test_values;', get_json, label='all values as json (row_to_json)')
    select('SELECT data0, data1, data2, data3, data4 FROM test_values;', label='values as values')
    select("SELECT data->'data1', data->'data2', data->'data3', data->'data4', data->'data5' FROM test_json;", label='json as values')
    select('SELECT data0, data1, data2, data3, data4 FROM test_values;', format_json, label='values as json (serialization)')
    select('''WITH r AS ( SELECT data0, data1, data2, data3, data4 FROM test_values )
              SELECT row_to_json(r.*) FROM r;''', get_json, label='values as json (row_to_json)')
    select("""WITH r AS (
              SELECT data->'data1' data1, data->'data2' data2, data->'data3' data3, data->'data4' data4, data->'data5' data5 FROM test_json
              ) SELECT row_to_json(r.*) FROM r;""", get_json, label='json as json (row_to_json)')
    # select('SELECT data0 FROM test_values;', label='1 column')
    # select('SELECT data0, data1 FROM test_values;', label='2 columns')
    # select('SELECT data0, data1, data2 FROM test_values;', label='3 columns')
    # select('SELECT data0, data1, data2, data3 FROM test_values;', label='4 columns')
    # select('SELECT data0, data1, data2, data3, data4 FROM test_values;', label='5 columns')
    # select('SELECT data0, data1, data2, data3, data4, data5 FROM test_values;', label='6 columns')
    # select('SELECT data0, data1, data2, data3, data4, data5, data6 FROM test_values;', label='7 columns')
    # select('SELECT data0, data1, data2, data3, data4, data5, data6, data7 FROM test_values;', label='8 columns')
    # select('SELECT data0, data1, data2, data3, data4, data5, data6, data7, data8 FROM test_values;', label='9 columns')
    # select('SELECT * FROM test_values;', label='whole row')
    # select("""WITH r AS ( SELECT data->'data0' data0 FROM test_json
    #            ) SELECT row_to_json(r.*) FROM r;""", get_json, label='1 json field')
    # select("""WITH r AS ( SELECT data->'data0' data0, data->'data1' data1 FROM test_json
    #            ) SELECT row_to_json(r.*) FROM r;""", get_json, label='2 json fields')
    # select("""WITH r AS ( SELECT data->'data0' data0, data->'data1' data1, data->'data2' data2 FROM test_json
    #            ) SELECT row_to_json(r.*) FROM r;""", get_json, label='3 json fields')
    # select("""WITH r AS ( SELECT data->'data0' data0, data->'data1' data1, data->'data2' data2,
    #                              data->'data3' data3 FROM test_json
    #            ) SELECT row_to_json(r.*) FROM r;""", get_json, label='4 json fields')
    # select("""WITH r AS ( SELECT data->'data0' data0, data->'data1' data1, data->'data2' data2,
    #                              data->'data3' data3, data->'data4' data4 FROM test_json
    #            ) SELECT row_to_json(r.*) FROM r;""", get_json, label='5 json fields')
    # select("""WITH r AS ( SELECT data->'data0' data0, data->'data1' data1, data->'data2' data2,
    #                              data->'data3' data3, data->'data4' data4, data->'data5' data5 FROM test_json
    #            ) SELECT row_to_json(r.*) FROM r;""", get_json, label='6 json fields')
    # select("""WITH r AS ( SELECT data->'data0' data0, data->'data1' data1, data->'data2' data2,
    #                              data->'data3' data3, data->'data4' data4, data->'data5' data5,
    #                              data->'data6' data6 FROM test_json
    #            ) SELECT row_to_json(r.*) FROM r;""", get_json, label='7 json fields')
    # select("""WITH r AS ( SELECT data->'data0' data0, data->'data1' data1, data->'data2' data2,
    #                              data->'data3' data3, data->'data4' data4, data->'data5' data5,
    #                              data->'data6' data6, data->'data7' data7 FROM test_json
    #            ) SELECT row_to_json(r.*) FROM r;""", get_json, label='8 json fields')
    # select("""WITH r AS ( SELECT data->'data0' data0, data->'data1' data1, data->'data2' data2,
    #                              data->'data3' data3, data->'data4' data4, data->'data5' data5,
    #                              data->'data6' data6, data->'data7' data7, data->'data8' data8 FROM test_json
    #            ) SELECT row_to_json(r.*) FROM r;""", get_json, label='9 json fields')
    # select('SELECT data FROM test_json;', get_json, label='whole json doc')