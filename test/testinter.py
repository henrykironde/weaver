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

from retriever import dataset_names as dt
from retriever import reload_scripts
from weaver.lib.defaults import ENCODING
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
