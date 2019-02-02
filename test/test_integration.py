# -*- coding: latin-1  -*-
# """Integrations tests for Data Weaver"""
from __future__ import print_function

import json
import os
import shlex
import shutil
import subprocess
import sys
from imp import reload

from retriever.lib.defaults import ENCODING

encoding = ENCODING.lower()

reload(sys)
if hasattr(sys, 'setdefaultencoding'):
    sys.setdefaultencoding(encoding)
import pytest
from weaver.lib.load_json import read_json
from weaver.lib.defaults import HOME_DIR
from weaver.engines import engine_list
from weaver.lib.engine_tools import file_2list
from weaver.lib.engine_tools import create_file

# Set postgres password, Appveyor service needs the password given
# The Travis service obtains the password from the config file.
if os.name == "nt":
    os_password = "Password12!"
else:
    os_password = ""

postgres_engine, sqlite_engine = engine_list

table_one = {
    'name': 'table_one',
    'raw_data': ['a,b,c',
                 '1,3,5',
                 '2,4,6'],
    'script': {"name": "table_one",
               "resources": [
                   {"dialect": {"do_not_bulk_insert": "True"},
                    "name": "table_one",
                    "schema": {
                      "fields": [
                         {
                          "name": "a",
                          "type": "int"
                         },
                         {
                          "name": "b",
                          "type": "int"
                         },
                         {
                          "name": "c",
                          "type": "int"
                         }
                      ]
                    },
                    "url": "http://example.com/table_one.txt"}
               ],
               "retriever": "True",
               "version": "1.0.0",
               "urls":
                   {"table_one": "http://example.com/table_one.txt"}
               }
}

table_two = {
    'name': 'table_two',
    'raw_data': ['a,d,e',
                 '1,r,UV',
                 '2,s,WX',
                 '3,t,YZ'],
    'script': {"name": "table_two",
               "resources": [
                   {"dialect": {"do_not_bulk_insert": "True"},
                    "name": "table_two",
                    "schema": {
                      "fields": [
                         {
                          "name": "a",
                          "type": "int"
                         },
                         {
                          "name": "d",
                          "size": "4",
                          "type": "char"
                         },
                         {
                          "name": "e",
                          "size": "4",
                          "type": "char"
                         }
                      ]
                    },
                    "url": "http://example.com/table_two.txt"}
               ],
               "retriever": "True",
               "version": "1.0.0",
               "urls":
                   {"table_two": "http://example.com/table_two.txt"}
               }
}

table_three = {
    'name': 'table_three',
    'raw_data': ['a,b,e',
                 '1,2,UV',
                 '1,3,WX',
                 '1,0,YZ',
                 '2,4,OP',
                 '2,5,QR'],
    'script': {"name": "table_three",
               "resources": [
                   {"dialect": {"do_not_bulk_insert": "True"},
                    "name": "table_three",
                    "schema": {
                    "fields": [
                      {
                        "name": "a",
                        "type": "int"
                      },
                      {
                        "name": "b",
                        "type": "int"
                      },
                      {
                        "name": "e",
                        "size": "4",
                        "type": "char"
                      }
                    ]
                    },
                    "url": "http://example.com/table_three.txt"}
               ],
               "retriever": "True",
               "version": "1.0.0",
               "urls":
                   {"table_three": "http://example.com/table_three.txt"}
               }
}

table_four = {
    'name': 'table_four',
    'raw_data': ['a,f,g',
                 '4,1,4',
                 '2,2,5',
                 '1,3,6'],
    'script': {"name": "table_four",
               "resources": [
                   {"dialect": {"do_not_bulk_insert": "True"},
                    "name": "table_four",
                    "schema": {
                      "fields": [
                        {
                          "name": "a",
                          "type": "int"
                        },
                        {
                          "name": "f",
                          "size": "4",
                          "type": "char"
                        },
                        {
                          "name": "g",
                          "size": "4",
                          "type": "char"
                        }
                      ]
                    },
                    "url": "http://example.com/table_four.txt"}
               ],
               "retriever": "True",
               "version": "1.0.0",
               "urls":
                   {"table_four": "http://example.com/table_four.txt"}
               }
}

table_five = {
    'name': 'table_five',
    'raw_data': ['id,a,b,f',
                 '1,1,3,PL',
                 '2,2,4,PT',
                 '3,2,4,PX'],
    'script': {"name": "table_five",
               "resources": [
                   {"dialect": {"do_not_bulk_insert": "True"},
                    "name": "table_five",
                    "schema": {
                      "fields": [
                         {
                          "name": "id",
                          "type": "int"
                         },
                         {
                          "name": "a",
                          "type": "int"
                         },
                         {
                          "name": "b",
                          "type": "int"
                         },
                         {
                          "name": "f",
                          "size": "4",
                          "type": "char"
                         } 
                        ]
                    },
                    "url": "http://example.com/table_five.txt"}
               ],
               "retriever": "True",
               "version": "1.0.0",
               "urls":
                   {"table_five": "http://example.com/table_five.txt"}
               },
}

tests = [
    table_one,
    table_two,
    table_three,
    table_four,
    table_five
]

# Create a tuple of all test scripts with their expected values
# test_parameters = [(test, test['expect_out']) for test in tests]

file_location = os.path.dirname(os.path.realpath(__file__))
weaver_root_dir = os.path.abspath(os.path.join(file_location, os.pardir))
for test in tests:
    if not os.path.exists(os.path.join(HOME_DIR, "raw_data", test['name'])):
        os.makedirs(os.path.join(HOME_DIR, "raw_data", test['name']))
    rd_path = os.path.join(HOME_DIR,
                           "raw_data", test['name'], test['name'] + '.txt')
    create_file(test['raw_data'], rd_path)

    path_js = os.path.join(HOME_DIR, "scripts", test['name'] + '.json')
    with open(path_js, 'w') as js:
        json.dump(test['script'], js, indent=2)
    read_json(os.path.join(HOME_DIR, "scripts", test['name']))