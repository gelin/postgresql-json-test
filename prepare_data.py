#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

import sys
import random
import string
import datetime
import json
import psycopg2 as db


PGHOST = 'localhost'
PGUSER = 'gelin'
PGPASSWORD = 'gelin'
PGDATABASE = 'json_test'

ROWS = 100000
#ROWS = 10
COLUMNS = 10
VALUE_SIZE = 100

VALUE_CHARS = string.ascii_letters + string.digits

connection = db.connect(host=PGHOST, user=PGUSER, password=PGPASSWORD, database=PGDATABASE)


def create_tables():
    cursor = connection.cursor()
    cols_def = ', '.join('data%i varchar(%i)' % (i, VALUE_SIZE) for i in range(COLUMNS))
    cursor.execute('CREATE TABLE test_values (' + cols_def + ');')
    cursor.execute('CREATE TABLE test_json (data json);')
    cursor.close()
    connection.commit()


def gen_value():
    return ''.join(random.choice(VALUE_CHARS) for x in range(VALUE_SIZE))


def insert_values(cursor):
    values_def = ', '.join("'" + gen_value() + "'" for i in range(COLUMNS))
    cursor.execute('INSERT INTO test_values VALUES (' + values_def + ');')


def gen_json():
    obj = {}
    for i in range(COLUMNS):
        obj['data%i' % i] = gen_value()
    return json.dumps(obj)


def insert_json(cursor):
    json = gen_json()
    cursor.execute("INSERT INTO test_json VALUES ('" + json + "');")


def insert(insert_fun, label=''):
    print('inserting', label, '...', file=sys.stderr)
    start = datetime.datetime.now()
    cursor = connection.cursor()
    for i in range(ROWS):
        insert_fun(cursor)
    cursor.close()
    connection.commit()
    end = datetime.datetime.now()
    print('inserted', ROWS, label, 'rows', 'in', end - start, file=sys.stderr)
    print((end - start) / ROWS, 'average time for a row', file=sys.stderr)


if __name__ == '__main__':
    create_tables()
    insert(insert_values, 'values')
    insert(insert_json, 'json')