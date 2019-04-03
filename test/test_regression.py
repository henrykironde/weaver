# -*- coding: latin-1  -*-
# """Integrations tests for Data Weaver"""
from __future__ import print_function
import json
import os
import shlex
import subprocess
import sys
from collections import OrderedDict
from imp import reload
import shutil
import pandas
import pytest


import time


import retriever as rt
from weaver import reload_scripts as weaver_reload_scripts
from weaver.engines import engine_list
from weaver.lib.defaults import ENCODING
from weaver.lib.load_json import read_json
from weaver.lib.engine_tools import getmd5
from weaver.lib.engine_tools import create_file
import weaver as wt
encoding = ENCODING.lower()

reload(sys)
if hasattr(sys, 'setdefaultencoding'):
    sys.setdefaultencoding(encoding)

FILE_LOCATION = os.path.normpath(os.path.dirname(os.path.realpath(__file__)))
RETRIEVER_HOME_DIR = os.path.normpath(os.path.expanduser('~/.retriever/'))
RETRIEVER_DATA_DIR = os.path.normpath(
    os.path.expanduser('~/.retriever/raw_data/'))
RETRIEVER_SCRIPT_DIR = os.path.normpath(
    os.path.expanduser('~/.retriever/scripts/'))
WEAVER_HOME_DIR = os.path.normpath(os.path.expanduser('~/.weaver/'))
WEAVER_SCRIPT_DIR = os.path.normpath(os.path.expanduser('~/.weaver/scripts/'))
WEAVER_TEST_DATA_PACKAEGES_DIR = os.path.normpath(
    os.path.join(FILE_LOCATION, "test_data_packages"))

RETRIEVER_GIS_REPO = "https://raw.githubusercontent.com/weecology/retriever/master/test/raw_data_gis/scripts/test_vector.json"
RETRIEVER_GIS_REPO = "https://raw.githubusercontent.com/weecology/retriever/master/test/raw_data_gis/scripts/{script_names}.json"
# Set postgres password, Appveyor service needs the password given
# The Travis service obtains the password from the config file.
os_password = ""
pgdb_host = "localhost"
mysqldb_host = "localhost"
testdb = "testdb_weaver"
testschema = "testschema_weaver"

if os.name == "nt":
    os_password = "Password12!"

docker_or_travis = os.environ.get("IN_DOCKER")
if docker_or_travis == "true":
    os_password = 'Password12!'
    pgdb_host = "pgdb_weaver"
    mysqldb_host = "mysqldb_weaver"

test_sample_wgs84 = {
    'name': 'testsurveryone',
    'raw_data': ['site_id, state, longitude, latitude, habitat_code',
                 '1,QP,-117.20,40.04, H1',
                 '2,QR,-112.10,41.00, H2',
                 '3,QS,-115.58,34.50, HA',
                 '4,QT,-114.62,36.78, H3',
                 '5,QU,-111.70,32.97, H4',
                 '6,Qv,-120.09,35.62, H5',
                 '7,QX,-120.68,38.84, H6'
                 ],

    'script': {"name": "testsurveryone",
               "resources": [
                   {"dialect": {"do_not_bulk_insert": "True"},
                    "name": "sites",
                    "schema": {
                        "fields": [
                            {
                                "name": "site_id",
                                "type": "int"
                            },
                            {
                                "name": "state",
                                "size": "4",
                                "type": "char"
                            },
                            {
                                "name": "longitude",
                                "type": "double"
                            },
                            {
                                "name": "latitude",
                                "type": "double"
                            },
                            {
                                "name": "habitat_code",
                                "size": "4",
                                "type": "char"
                            }
                        ]
                    },
                    "url": "http://example.com/testsurveryone.txt"}
               ],
               "retriever": "True",
               "version": "1.0.0"
               }
}

test_sample_nad83 = {
    'name': 'testsurverytwo',
    'raw_data': ['site_id, state, longitude, latitude, habitat_code',
                 '1,QP,-2152956,2033827, HA',
                 '2,QR,-2001329,1867986, HA',
                 '3,QS,-1598571,1221204, HA',
                 '4,QT,-1735983,2180715, HA',
                 '5,QU,-1240827,2064625, HA'
                 ],

    'script': {"name": "testsurverytwo",
               "resources": [
                   {"dialect": {"do_not_bulk_insert": "True"},
                    "name": "sites",
                    "schema": {
                        "fields": [
                            {
                                "name": "site_id",
                                "type": "int"
                            },
                            {
                                "name": "state",
                                "size": "4",
                                "type": "char"
                            },
                            {
                                "name": "longitude",
                                "type": "double"
                            },
                            {
                                "name": "latitude",
                                "type": "double"
                            },
                            {
                                "name": "habitat_code",
                                "size": "4",
                                "type": "char"
                            }
                        ]
                    },
                    "url": "http://example.com/testsurverytwo.txt"}
               ],
               "retriever": "True",
               "version": "1.0.0"
               }
}

RETRIEVER_TESTS_DATA = [test_sample_wgs84, test_sample_nad83]
RETRIEVER_SPATIAL_DATA = [
    "test-eco-level-four",
    "test-raster-bio1",
    "test-raster-bio2",
    "test-us-eco",
]

WEAVER_TEST_SCRIPTS = [
    # ("test-multi-vector-raster", "csvfile_name", "dd"),
    # ("test-vector-multi-raster", "csvfile_name", "dd"),
    ("test-raster", "csvfile_name", "dd"),
    ("test-vector", "csvfile_name", "dd"),
]
WEAVER_TEST_DATA_PACKAGE_FILES = [file_base_names[0].replace("-", "_")
                                  for file_base_names in WEAVER_TEST_SCRIPTS]


def set_weaver_data_packages(resources_up=True):
    """Setup or tear down weaver test scripts

    Copy or delete weaver test scripts from test_data directory,
    WEAVER_TEST_DATA_PACKAEGES_DIR
    to ~/.weaver script directory WEAVER_SCRIPT_DIR
    """
    if resources_up:
        if not WEAVER_SCRIPT_DIR:
            os.makedirs(WEAVER_SCRIPT_DIR)
    for file_name in WEAVER_TEST_DATA_PACKAGE_FILES:
        if resources_up:
            scr_pack_path = os.path.join(WEAVER_TEST_DATA_PACKAEGES_DIR, file_name + ".json")
            pack_path = os.path.normpath(scr_pack_path)
            shutil.copy(pack_path, WEAVER_SCRIPT_DIR)
            weaver_reload_scripts()
        else:
            dest_pack_path = os.path.join(WEAVER_SCRIPT_DIR, file_name + ".json")
            dest_path = os.path.normpath(dest_pack_path)
            if os.path.exists(dest_path):
                os.remove(dest_path)
    wt.reload_scripts()


def set_retriever_res(resource_up=True):
    """Create or tear down retriever data and scripts

    if resource_up =True, set up retriever else tear down
    Data directory uses "-", data file names uses "_"
    script file names uses "_"
    """
    for file_names in RETRIEVER_TESTS_DATA:
        data_dir_path = (os.path.join(RETRIEVER_DATA_DIR, file_names['name']))
        data_dir_path = os.path.normpath(data_dir_path)
        data_file_name = file_names['name'].replace("-", "_") + '.txt'
        data_file_path = os.path.normpath(os.path.join(data_dir_path, data_file_name))
        script_name = file_names['script']["name"] + '.json'
        script_file_path = os.path.normpath(os.path.join(RETRIEVER_SCRIPT_DIR, script_name.replace("-", "_")))

        # Set or tear down raw data files
        # in '~/.retriever/raw_data/data_dir_path/data_file_name'
        if resource_up:
            if not os.path.exists(data_dir_path):
                os.makedirs(data_dir_path)
                create_file(file_names['raw_data'], data_file_path)
            if not os.path.exists(RETRIEVER_SCRIPT_DIR):
                os.makedirs(RETRIEVER_SCRIPT_DIR)
            with open(script_file_path, 'w') as js:
                json.dump(file_names['script'], js, indent=2)
        else:
            shutil.rmtree(data_dir_path)
            os.remove(script_file_path)
    for script_name in RETRIEVER_SPATIAL_DATA:
        file_name = script_name.replace("-", "_")
        url = RETRIEVER_GIS_REPO.format(script_names=file_name)
        import requests
        from urllib.request import urlretrieve
        from requests.exceptions import InvalidSchema
        script_file_path = os.path.normpath(os.path.join(RETRIEVER_SCRIPT_DIR, script_name.replace("-", "_") + ".json"))
        if not os.path.exists(RETRIEVER_SCRIPT_DIR):
            os.makedirs(RETRIEVER_SCRIPT_DIR)
        urlretrieve(url, script_file_path)
    rt.reload_scripts()




# Weaver defaults
# Engines
postgres_engine, sqlite_engine = engine_list


# Weaver test data(Tuple)
# (Script file name with no extensio, script name, result table, expected)

# # File names without `.json` extension
# WEAVER_TEST_DATA_PACKAGE_FILES = [file_base_names[0]
#                                   for file_base_names in WEAVER_TEST_DATA]
# weaver_test_parameters = [(test[1], test[2], test[3])
#                           for test in WEAVER_TEST_DATA]


def file_exists(path):
    """Return true if a file exists and its size is greater than 0."""
    return os.path.isfile(path) and os.path.getsize(path) > 0


# Install RETRIEVER_SPATIAL_DATA using default db and schema names:
def install_to_database(dataset, install_function, config):
    install_function(dataset, **config)
    return


def install_dataset_postgres(dataset):
    postgres_engine.opts = {'engine': 'postgres',
                            'user': 'postgres',
                            'password': os_password,
                            'host': pgdb_host,
                            'port': 5432,
                            'database': testdb,
                            'database_name': testschema,
                            'table_name': '{db}.{table}'}
    interface_opts = {"user": 'postgres',
                      "password": postgres_engine.opts['password'],
                      "host": postgres_engine.opts['host'],
                      "database": postgres_engine.opts['database'],
                      # "database_name": postgres_engine.opts['database_name'],
                      "table_name": postgres_engine.opts['table_name']
                      }
    install_to_database(dataset, rt.install_postgres, interface_opts)


def setup_postgres_retriever_db():
    # Install the requited retriever datasets
    for test_data in RETRIEVER_SPATIAL_DATA:
        install_dataset_postgres(test_data)
    # This is the main table]
    for test_data in RETRIEVER_TESTS_DATA:
        install_dataset_postgres(test_data["script"]["name"])


def teardown_postgres_db():
    # Retriever database
    cmd = 'psql -U postgres -d ' + testdb + ' -h ' + pgdb_host + ' -w -c \"DROP SCHEMA IF EXISTS '+ testschema +' CASCADE\"'
    # subprocess.call(shlex.split(cmd))
    #
    # # Weaver database
    # for file_base_names in WEAVER_TEST_DATA:
    #     dataset = file_base_names[1]
    #     sql_stm = "DROP SCHEMA IF EXISTS " + dataset.replace("-", "_") + " CASCADE"
    #     cmd = 'psql -U postgres -d testdb -h ' + pgdb_host + ' -w -c \"{sql_stm}\"'
    #     dfd = cmd.format(sql_stm=sql_stm)
    #     subprocess.call(shlex.split(dfd))


def setup_module():
    # set up postgres database
    teardown_postgres_db()
    set_weaver_data_packages(resources_up=True)
    set_retriever_res(resource_up=True)
    setup_postgres_retriever_db()


# # Test Retriever resources
# def test_retriever_test_resources():
#     """Test retriever resource files"""
#     pass


# Weaver integration
def get_output_as_csv(dataset, engines, db):
    """integrate datasets and return the output as a csv."""
    wt.reload_scripts()
    eng = wt.join_postgres(dataset, database=testdb, database_name= testschema,host=pgdb_host, password=os_password)
    # Wait for 5 seconds
    time.sleep(5)
    csv_file = eng.to_csv()
    return csv_file

# def test_yess():
#     assert 1==1

@pytest.mark.parametrize("dataset, csv_file, expected", WEAVER_TEST_SCRIPTS)
def test_postgres(dataset, csv_file, expected):
    tmpdir = None
    postgres_engine.opts = {'engine': 'postgres',
                            'user': 'postgres',
                            'password': os_password,
                            'host': pgdb_host,
                            'port': 5432,
                            'database': testdb,
                            'database_name': testschema,
                            'table_name': '{db}.{table}'}
    interface_opts = {"user": 'postgres',
                      "password": postgres_engine.opts['password'],
                      "host": postgres_engine.opts['host'],
                      'port': postgres_engine.opts['port'],
                      "database": postgres_engine.opts['database'],
                      "database_name": postgres_engine.opts['database_name'],
                      "table_name": postgres_engine.opts['table_name']}
    res_csv = get_output_as_csv(dataset, postgres_engine, db=postgres_engine.opts['database_name'])
    # assert getmd5(res_csv) ==expected
    assert file_exists(res_csv)
    assert 1 == 1

