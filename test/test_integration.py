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
import pytest
from retriever import dataset_names
from retriever import install_postgres
from retriever import install_sqlite
from retriever import reload_scripts
from weaver.lib.defaults import ENCODING

encoding = ENCODING.lower()

reload(sys)
if hasattr(sys, 'setdefaultencoding'):
    sys.setdefaultencoding(encoding)
from weaver.lib.load_json import read_json
from weaver.engines import engine_list
from weaver.lib.engine_tools import create_file

# Set postgres password, Appveyor service needs the password given
# The Travis service obtains the password from the config file.

os_password = ""
pgdb = "localhost"

docker_or_travis = os.environ.get("IN_DOCKER")

# Check if the environment variable "IN_DOCKER" is set to "true"
if docker_or_travis == "true":
    os_password = 'Password12!'
    pgdb = "pgdb"
file_location = os.path.normpath(os.path.dirname(os.path.realpath(__file__)))
TEST_DATA_PACKAEGES = os.path.normpath(os.path.join(file_location, "test_data_packages"))
TESTS_SCRIPTS = [
        "table-one",
        "table-two",
        "table-three",
        "table-four",
        "table-five"]
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

test_directory = ['multi_columns_multi_tables.json',
                  'simple_join_one_column_custom.json',
                  'one_column_multi_tables.json',
                  'simple_join_two_column.json',
                  'simple_join_one_column.json']

# Create a tuple of all test scripts with their expected values
# test_parameters = [(test, test['expect_out']) for test in tests]

file_location = os.path.dirname(os.path.realpath(__file__))
# USe the retriever to install data into the databases

retriever_root_dir = os.path.abspath(os.path.join(file_location, os.pardir))


RETRIEVER_HOME_DIR = os.path.normpath(os.path.expanduser('~/.retriever/'))
WEAVER_HOME_DIR = os.path.normpath(os.path.expanduser('~/.weaver/'))
WEAVER_SCRIPT_DIR = os.path.normpath(os.path.expanduser('~/.weaver/scripts/'))



def setup_module():
    setup_scripts()
    setup_postgres_db()
    setup_sqlite_db()

    setup_weaver_data_packages()

def teardown_sqlite_db():
    dbfile = os.path.normpath(os.path.join(os.getcwd(), 'testdb.sqlite'))
    subprocess.call(['rm', '-r', dbfile])


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
    reload_scripts()

    # reload(retriever)
    # reload(install_postgres)
    # reload(i)


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
                            'host': pgdb,
                            'port': 5432,
                            'database': 'testdb',
                            'database_name': 'testschema',
                            'table_name': '{db}.{table}'}
    interface_opts = {"user": 'postgres',
                      "password": postgres_engine.opts['password'],
                      "host": postgres_engine.opts['host'],
                      "database": postgres_engine.opts['database'],
                      "database_name": postgres_engine.opts['database_name'],
                      "table_name": postgres_engine.opts['table_name']}
    get_csv_md5(dataset, install_postgres, interface_opts)


def install_sqlite_regression(dataset):
    """Install test dataset into sqlite."""
    dbfile = os.path.normpath(os.path.join(os.getcwd(), 'testdb.sqlite'))
    sqlite_engine.opts = {
        'engine': 'sqlite',
        'file': dbfile,
        'table_name': '{db}_{table}'}
    interface_opts = {'file': dbfile}
    get_csv_md5(dataset, install_sqlite, interface_opts)


def teardown_postgres_db():
    cmd = 'psql -U postgres -d testdb -h ' + pgdb + ' -w -c \"DROP SCHEMA IF EXISTS testschema CASCADE\"'
    subprocess.call(shlex.split(cmd))


def setup_postgres_db():
    teardown_postgres_db()
    for i in db_md5:
        install_dataset_postgres(i[0])


def setup_sqlite_db():
    teardown_sqlite_db()
    for i in db_md5:
        install_sqlite_regression(i[0])



################
# Weaver Testing
################


def file_exists(path):
    """Return true if a file exists and its size is greater than 0."""
    return os.path.isfile(path) and os.path.getsize(path) > 0


def test_test_scripts():
    scrpts_and_raw_data = True
    db_md5
    # ToDOs: Change tests the db_md5 and make it Global
    tests_scripts = [
        "table-one",
        "table-two",
        "table-three",
        "table-four",
        "table-five"]

    for items in tests_scripts:
        retriever_raw_data_path = os.path.normpath(
            os.path.join(RETRIEVER_HOME_DIR, 'raw_data', items, items + '.txt'))
        if not file_exists(retriever_raw_data_path):
            scrpts_and_raw_data = False
        retriever_script_path = os.path.normpath(
            os.path.join(RETRIEVER_HOME_DIR, 'scripts', items + '.json'))
        if not file_exists(retriever_script_path):
            scrpts_and_raw_data = False
    assert scrpts_and_raw_data is True

def setup_directories():
    # ToDos all directories are should be down well
    print("# ToDos all directories are should be down well")
    # exit()


def setup_weaver_data_packages():
    if not WEAVER_SCRIPT_DIR:
        setup_directories()
    # Copy all the files to weaver scripts to be able to install them
    for i in test_directory:
        # print(i)
        pack_path = os.path.normpath(os.path.join(TEST_DATA_PACKAEGES, i))
    # print(['cp', '-r', pack_path, WEAVER_SCRIPT_DIR])
    #     subprocess.call(['cp', '-r', pack_path, WEAVER_SCRIPT_DIR])


def test_weaver_test_data_packages():
    # Test if local files are there
    data_packages_exists = True
    test_directory = ['multi_columns_multi_tables.json',
                      'simple_join_one_column_custom.json',
                      'one_column_multi_tables.json',
                      'simple_join_two_column.json',
                      'simple_join_one_column.json']

    for item in test_directory:
        file_paths = os.path.join(WEAVER_SCRIPT_DIR, item.replace("-", "_") + '.json')
        print(file_paths)
    #     if not file_exists(file_paths):
    #         data_packages_exists = False
    # assert data_packages_exists is True


def test_scripts():
    TESTS_SCRIPTS = [
        "table-one",
        "table-two",
        "table-three",
        "table-four",
        "table-five"]
    assert set(TESTS_SCRIPTS).issubset(set(dataset_names()))

#######################
# To csv

#######################
#######################
#######################

test_parameters = [(test, test[1]) for test in db_md5]


def get_script_module(script_name):
    """Load a script module."""
    print(os.path.join(WEAVER_HOME_DIR, "scripts", script_name))
    return read_json(os.path.join(WEAVER_HOME_DIR, "scripts", script_name))



# def get_output_asg_csv(dataset, engines, tmpdir, db):
#     """Install dataset and return the output as a string version of the csv."""
#     workdir = tmpdir.mkdtemp()
#     workdir.chdir()
#
#     # Since we are writing scripts to the .retriever directory,
#     # we don't have to change to the main source directory in order
#     # to have the scripts loaded
#     script_module = get_script_module(dataset["name"])
#     engines.script_table_registry = {}
#     script_module.download(engines)
#     script_module.engine.final_cleanup()
#     script_module.engine.to_csv()
#     # get filename and append .csv
#     csv_file = engines.opts['table_name'].format(db=db, table=dataset["name"])
#     # csv engine already has the .csv extension
#     if engines.opts["engine"] != 'csv':
#         csv_file += '.csv'
#     obs_out = file_2list(csv_file)
#     os.chdir(retriever_root_dir)
#     return obs_out

def get_output_as_csv(dataset, engines, tmpdir, db):
    """Install dataset and return the output as a string version of the csv."""

    # Since we are writing scripts to the .retriever directory,
    # we don't have to change to the main source directory in order
    # to have the scripts loaded
    script_module = get_script_module(dataset)
    csv_file = script_module.engines.to_csv()
    return csv_file, dataset

# def test_sqlite_join:
#     pass


@pytest.mark.parametrize("dataset, expected", test_parameters)
def test_postgres(dataset, expected=None, tmpdir=None):
    postgres_engine.opts = {'engine': 'postgres',
                            'user': 'postgres',
                            'password': os_password,
                            'host': pgdb,
                            'port': 5432,
                            'database': 'testdb',
                            'database_name': 'testschema',
                            'table_name': '{db}.{table}'}
    interface_opts = {"user": 'postgres',
                      "password": postgres_engine.opts['password'],
                      "host": postgres_engine.opts['host'],
                      "database": postgres_engine.opts['database'],
                      "database_name": postgres_engine.opts['database_name'],
                      "table_name": postgres_engine.opts['table_name']}
    get_output_as_csv(dataset, postgres_engine, tmpdir,
                             db=postgres_engine.opts['database_name'])
    # assert get_output_as_csv(dataset, postgres_engine, tmpdir,
    #                          db=postgres_engine.opts['database_name']) == expected
    # assert f
    # os.chdir(retriever_root_dir)
    # return obs_out


if __name__ == '__main__':
    setup_weaver_data_packages()
    print(test_directory[0])
    test_postgres(test_directory[0])
    # print(test_postgres(test_directory[0]))
    print("done.....")
# test_test_scripts()
##############################
# Clean up Testing Environment
##############################

# After tests Clean up
# teardown_scripts()
# teardown_sqlite_db()
# teardown_postgres_db()
