from __future__ import print_function

import shutil

from weav.lib.models import *
from weav.engines import choose_engine
from weav.lib.defaults import DATA_DIR

from retriever import dataset_names
from weav.lib.templates import TEMPLATES

class Processor(object):
     def __init__(self, name=None, **kwargs):
         name = name
         config = kwargs
         scripts = []

         for i in config["tables"]:
             i["dataset"]
         scrip_needed =name for name[] config["tables"]
         for i_scpts in dataset_names():

