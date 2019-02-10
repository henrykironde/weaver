# -*- coding: latin-1  -*-
# """Integrations tests for Data Weaver"""
from __future__ import print_function

import imp
import json
import os
import shlex
import shutil
import subprocess
import sys

from retriever.lib.defaults import ENCODING

encoding = ENCODING.lower()

if hasattr(sys, 'setdefaultencoding'):
    sys.setdefaultencoding(encoding)
import pytest

from weaver.lib.load_json import read_json
# from weaver.lib.defaults import HOME_DIR
# from weaver.engines import engine_list
# from weaver.lib.engine_tools import file_2list
from weaver.lib.engine_tools import create_file
from retriever import dataset_names as dt

# Set postgres password, Appveyor service needs the password given
# The Travis service obtains the password from the config file.

os_password = ""
pgdb = "localhost"

docker_or_travis = os.environ.get("IN_DOCKER")

# Check if the environment variable "IN_DOCKER" is set to "true"
if docker_or_travis == "true":
    os_password = 'Password12!'
    pgdb = "pgdb"

# postgres_engine, sqlite_engine = engine_list

table_one = {
    'name': 'table-one',
    'raw_data': ['a,b,c',
                 '1,3,5',
                 '2,4,6'],
    'script': {"name": "table-one",
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
                    "url": "http://example.com/table-one.txt"}
               ],
               "retriever": "True",
               "version": "1.0.0"
               }
}

table_two = {
    'name': 'table-two',
    'raw_data': ['a,d,e',
                 '1,r,UV',
                 '2,s,WX',
                 '3,t,YZ'],
    'script': {"name": "table-two",
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
                    "url": "http://example.com/table-two.txt"}
               ],
               "retriever": "True",
               "version": "1.0.0"
               }
}

table_three = {
    'name': 'table-three',
    'raw_data': ['a,b,e',
                 '1,2,UV',
                 '1,3,WX',
                 '1,0,YZ',
                 '2,4,OP',
                 '2,5,QR'],
    'script': {"name": "table-three",
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
                    "url": "http://example.com/table-three.txt"}
               ],
               "retriever": "True",
               "version": "1.0.0"
               }
}

table_four = {
    'name': 'table-four',
    'raw_data': ['a,f,g',
                 '4,1,4',
                 '2,2,5',
                 '1,3,6'],
    'script': {"name": "table-four",
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
                    "url": "http://example.com/table-four.txt"}
               ],
               "retriever": "True",
               "version": "1.0.0"
               }
}

table_five = {
    'name': 'table-five',
    'raw_data': ['id,a,b,f',
                 '1,1,3,PL',
                 '2,2,4,PT',
                 '3,2,4,PX'],
    'script': {"name": "table-five",
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
                    "url": "http://example.com/table-five.txt"}
               ],
               "retriever": "True",
               "version": "1.0.0"
               },
}

tests = [
    table_one,
    table_two,
    table_three,
    table_four,
    table_five
]


db_md5 = [
    ('table-one', '89c8ae47fb419d0336b2c22219f23793'),
    ('table-two', '98dcfdca19d729c90ee1c6db5221b775'),
    ('table-three', '6fec0fc63007a4040d9bbc5cfcd9953e'),
    ('table-four', '98dcfdca19d729c90ee1c6db5221b775'),
    ('table-five', '98dcfdca19d729c90ee1c6db5221b775')
]

# Create a tuple of all test scripts with their expected values
# test_parameters = [(test, test['expect_out']) for test in tests]

file_location = os.path.dirname(os.path.realpath(__file__))
# USe the retriever to install data into the databases

retriever_root_dir = os.path.abspath(os.path.join(file_location, os.pardir))


RETRIEVER_HOME_DIR = os.path.expanduser('~/.retriever/')


def setup_scripts():
    for test in tests:
        if not os.path.exists(os.path.join(RETRIEVER_HOME_DIR, "raw_data", test['name'])):
            os.makedirs(os.path.join(RETRIEVER_HOME_DIR, "raw_data", test['name']),exist_ok=True)
        rd_path = os.path.join(RETRIEVER_HOME_DIR,
                               "raw_data", test['name'], test['name'] + '.txt')
        create_file(test['raw_data'], rd_path)
        path_js = os.path.join(RETRIEVER_HOME_DIR, "scripts", test['name'] + '.json')
        if not os.path.exists(os.path.join(RETRIEVER_HOME_DIR, "scripts")):
            os.makedirs(os.path.join(RETRIEVER_HOME_DIR, "scripts"))
        with open(path_js, 'w') as js:
            json.dump(test['script'], js, indent=2)
        read_json(os.path.join(RETRIEVER_HOME_DIR, "scripts", test['name']))

def test_weaver_test_scripts():
    # Test if local files are there
    data_packages_exists = True
    test_directory = ['tables-a-b-columns-a',
                      'tables-a-c-columns-a-b',
                      'tables-a-b-columns-a-custom',
                      'tables-a-c-e-columns-a-b',
                      'tables-a-b-d-columns-a']
    for item in test_directory:
        file_paths = os.path.join(test_data_packages, item.replace("-","_") + '.json')
        if not file_exists(file_paths):
            data_packages_exists = False

if __name__ == '__main__':
    sc = dt()
    sc = dt()
    file_location = os.path.dirname(os.path.realpath(__file__))
    print(file_location)
    # setup_scripts()
    # from retriever import reload_scripts
    # reload_scripts()
    # cs = dt()
    # print(cs)

