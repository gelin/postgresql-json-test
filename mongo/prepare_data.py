#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

import sys
import random
import string
import datetime
import pymongo


ROWS = 100000
#ROWS = 10
COLUMNS = 10
VALUE_SIZE = 100

VALUE_CHARS = string.ascii_letters + string.digits

collection = pymongo.Connection('localhost').test.json_docs


def gen_value():
    return ''.join(random.choice(VALUE_CHARS) for x in range(VALUE_SIZE))


def gen_doc():
    doc = {}
    for i in range(COLUMNS):
        doc['data%i' % i] = gen_value()
    return doc


if __name__ == '__main__':
    print('inserting', '...', file=sys.stderr)
    start = datetime.datetime.now()
    for i in range(ROWS):
        collection.insert(gen_doc())
    end = datetime.datetime.now()
    print('inserted', ROWS, 'docs', 'in', end - start, file=sys.stderr)
    print((end - start) / ROWS, 'average time for a doc', file=sys.stderr)
