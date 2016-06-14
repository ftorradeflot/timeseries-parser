#!/usr/envs/eyecode/bin/python
"""
.. module:: api/util.py
    :platform: Unix, Windows
    :synopsis: Api util methods
.. moduleauthor:: Quim Castella <quim@nomorecode.com>
"""
import json
import re
from sqlalchemy.dialects.postgresql import JSON, INET
from IPy import IP
from datetime import datetime

def generic_conversion(data, data_type):
    # Check type
    try:
        val = data_type(data)
        return {'success': val}
    except:
        return {'error': data_type}


def string_conversion(data):
    try:
        val = unicode(data)
        return {'success': val}
    except:
        return {'error': str}


def type_conversion(data, postgres_type):

    ''' This function converts received data to the expected
    data type in the Postgres DB. If it is not possible, it
    gives an error'''

    str_type = str(postgres_type)

    if re.match('VARCHAR', str_type):
        return string_conversion(data)
    elif str_type == 'JSON':
        # Check type
        # We want to send a jsonable object to Postgres DB
        # If we receive a string we will try to load it
        # If we receive another data type, we will check if it is dumpable
        if type(data) in (str, unicode):
            try:
                # load the string
                val = json.loads(data)
                #return {'success':data}
                return {'success':val}
            except:
                return {'error': 'json'}
        else:
            try:
                # Check if the value is dumpable to a json string
                val = json.dumps(data)
                #return {'success': val}
                return {'success':data}
            except:
                return json_error_type(key, 'json')
    elif str_type == 'INTEGER':
        return generic_conversion(data, int)
    elif str_type == 'TEXT':
        return string_conversion(data)
    elif str_type == 'UUID':
        if re.match('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', str(data)):
            return {'success': data}
        else:
            return {'error': 'uuid'}
    elif str_type == 'FLOAT':
        return generic_conversion(data, float)
    elif str_type == 'DATETIME':
        pass
    elif str_type == 'BOOLEAN':
        if data in [1, 0, '1', '0', True, False, 'True', 'False', 'true', 'false']:
            if data in ['1', 1, 'True', 'true']:
                return  {'success':True}
            if data in ['0', 0, 'False', 'false']:
                return {'success':False}
        else:
            return {'error': bool}
    elif str_type == 'INET':
        try:
            val = IP(data)
            return {'success': data}
        except:
            return {'error': 'inet'}
    elif str_type == 'TIME':
        try:
            val = datetime.strptime(data, '%H:%M')
            return {'success': val.time()}
        except:
            return {'error': 'time'}
    elif str_type == 'DATE':
        try:
            val = datetime.strptime(data, '%Y-%m-%d')
            return {'success': val.date()}
        except:
            return {'error': 'date'}
    else:
        return {'error': 'unknown type'}


