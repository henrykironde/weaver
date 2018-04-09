from __future__ import print_function

import shutil

from weaver.lib.models import *
from weaver.engines import choose_engine
from weaver.lib.defaults import DATA_DIR

from retriever import dataset_names
from weaver.lib.templates import TEMPLATES


class Processor(object):
    def __init__(self, name=None, dictt =None):
        name = name
        config = dictt
        scripts = []

        scripts_needed = []


        for i in config["tables"]:
            scripts_needed.append(i["dataset"])
        print (set(scripts_needed))

        set()


