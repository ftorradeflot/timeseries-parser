#!/usr/envs/eyecode/bin/python
"""
.. module:: api/data.py
	:platform: Unix, Windows
	:synopsis: Cassandra util methods
.. moduleauthor:: Quim Castella <quim@nomorecode.com>
"""
from common.server_info import Cassandra
from pycassa.system_manager import *
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import *
from pycassa.types import *
from pycassa import *
import time
import uuid
import re


def load_pool():
   return ConnectionPool(Cassandra.KEYSPACE,[Cassandra.IP_PORT]) 

