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
from imp import reload

from retriever import install_postgres
from retriever import install_sqlite

from weaver.lib.defaults import ENCODING

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
            os.makedirs(os.path.join(RETRIEVER_HOME_DIR, "raw_data", test['name']))
        rd_path = os.path.join(RETRIEVER_HOME_DIR,
                               "raw_data", test['name'], test['name'] + '.txt')
        create_file(test['raw_data'], rd_path)
        path_js = os.path.join(RETRIEVER_HOME_DIR, "scripts", test['name'] + '.json')
        with open(path_js, 'w') as js:
            json.dump(test['script'], js, indent=2)
        read_json(os.path.join(RETRIEVER_HOME_DIR, "scripts", test['name']))


def teardown_scripts():
    """Remove test data and scripts from .retriever directories."""
    for test in tests:
        shutil.rmtree(os.path.join(RETRIEVER_HOME_DIR, "raw_data", test['name']))
        os.remove(os.path.join(RETRIEVER_HOME_DIR, "scripts", test['name'] + '.json'))


def get_csv_md5(dataset, install_function, config):
    # install_function(dataset.replace('_', '-'), **config)
    install_function(dataset, **config)
    return


def install_dataset_postgres(dataset):
    """Install test dataset into postgres ."""
    # cmd = 'psql -U postgres -d testdb -h localhost -c ' \
    #       '"DROP SCHEMA IF EXISTS testschema CASCADE"'
    # subprocess.call(shlex.split(cmd))
    postgres_engine.opts = {'engine': 'postgres',
                            'user': 'postgres',
                            'password': os_password,
                            'host': 'localhost',
                            'port': 5432,
                            'database': 'testdb',
                            'database_name': 'testschema',
                            'table_name': '{db}.{table}'}
    interface_opts = {"user": 'postgres',
                      "password": postgres_engine.opts['password'],
                      "database": postgres_engine.opts['database'],
                      "database_name": postgres_engine.opts['database_name'],
                      "table_name": postgres_engine.opts['table_name']}
    assert get_csv_md5(dataset, install_postgres, interface_opts) is None

def install_sqlite_regression(dataset):
    """Install test dataset into sqlite."""
    dbfile = os.path.normpath(os.path.join(os.getcwd(), 'testdb.sqlite'))
    sqlite_engine.opts = {
        'engine': 'sqlite',
        'file': dbfile,
        'table_name': '{db}_{table}'}
    interface_opts = {'file': dbfile}
    assert get_csv_md5(dataset, install_sqlite, interface_opts) is None


def teardown_postgres_db():
    cmd = 'psql -U postgres -d testdb -h localhost -c ' \
          '"DROP SCHEMA IF EXISTS testschema CASCADE"'
    subprocess.call(shlex.split(cmd))


def setup_postgres_db():
    teardown_postgres_db()
    for i in db_md5:
        install_dataset_postgres(i[0])


def teardown_sqlite_db():
    dbfile = os.path.normpath(os.path.join(os.getcwd(), 'testdb.sqlite'))
    subprocess.call(['rm', '-r', dbfile])


def setup_sqlite_db():
    teardown_sqlite_db()
    for i in db_md5:
        install_sqlite_regression(i[0])



setup_scripts()
setup_postgres_db()
setup_sqlite_db

################
# Weaver Testing
################





##############################
# Clean up Testing Environment
##############################

# After tests Clean up
# teardown_scripts()
# teardown_sqlite_db()
# teardown_postgres_db()


