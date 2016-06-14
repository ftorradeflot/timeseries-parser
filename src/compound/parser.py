# --------------------------------------------------------------------
# Author: Francesc Torradeflot - <ciscu@nomorecode.com>
#
# Description:
# Given a text representing a timeseries function call
# Understand and call it
#
# TODO: Optimize the function parser. The parser will read the text
# inside a function as much times as its level of embeddement. It should be
# read only once.
#
# --------------------------------------------------------------------
# Copyright (c) 2014 - All Rights Reserved.
#
# This source is subject to the Nomorecode Source License.
# Please see the License.md file for more information, which is
# part of this source code package.
# --------------------------------------------------------------------

# Imports and defines.
import re

import analysis.timeseries_functions as tu

# ------------------------------ Function parser -------------------------------------
def parser(text):

    ''' Given a text representing a call to timeseries functions
        parse the text and call the functions

    .. arguments:
    - (text) string : string containing the formula we want to compute
        with a structure as following:
        function(arg1; ....; argn; kwarg1 = kw1; ...; kwargm = kwm)
        each arg can be a function call himself but not the kwargs

    .. returns:
    - (new_ts) Pandas DataFrame containing a timeserie distributed to "seconds" intervals
        and filtering the last "count" values from e_from to e_to 

    '''

    if not text:
        return {'error': 'Not valid formula'}

    # Delete blanks
    text = text.replace(' ', '')

    # Identify the text representing the outer function being called
    out, val_1, val_2 = find_func(text)
    if out == 'error':
        return {'error': val_1}
    elif val_1 == '':
        return text

    # Import the function from timeseries_functions module
    try:
        func = tu.__getattribute__(val_1)
    except:
        return {'error': 'Unknown function: %s' %val_1}

    # Get the args and kwargs of the function
    args, kwargs = parse_args(val_2)

    if args == 'error':
        return {args: kwargs}

    # Recursive call to parser for computing the args
    new_args = []
    for arg in args:
        a = parser(arg)
        if type(a) == dict and 'error' in a:
            return a
        new_args.append(a)
    
    # Compute the function and return the result

    try:
        f = func(*new_args, **kwargs)
        return f
    except:
        return {'error': 'Unable to compute function'}



def find_func(text):

    ''' Given a string representing a call to a function with the structure:
        function_name(function_arguments)
        return function_name and function_arguments separatedly

    .. arguments:
    - (text) string : text containing the call to the function

    .. returns:
    - (out, val_1, val_2):
        (out): success or error message
        (val_1): function_name or error message
        (val_2): function_arguments or None

    '''

    s_1 = re.search('\(', text)
    s_2 = re.search('\)$', text)

    if not s_1 and not s_2:
        return ('success', '', text)

    elif s_1 and s_2:
        par_1 = s_1.start(0)        
        par_2 = s_2.start(0)
        return ('success', text[:par_1], text[par_1 + 1 : par_2])

    else:
        return ('error', 'Incorrect syntax', None)


def parse_args(args_text):

    ''' Given a string representing the arguments of a function
        identify the args or kwargs

    .. arguments:
    - (args_text) string : string we want to check

    .. returns:
    - (val_1, val_2):
        (val_1): error or args string list
        (val_2): error description or kwargs string dict

    '''

    args = []
    kwargs = {}
    level = 0
    st = ''

    # iterate over the characters of the text
    for i, l in enumerate(args_text):
        if l == '(':
            level += 1
            st += l
        elif l == ')':
            level -= 1
            st += l
        elif l == ';':
            if level != 0:
                st += l
            else:
                tf, kwarg, value = is_kwarg(st)
                if tf == 'error':
                    return tf, kwarg 
                elif tf == 'arg':
                    args.append(st)
                elif tf == 'kwarg':
                    kwargs[kwarg] = value
                st = ''
        else:
            st += l

    # check text after the last semicolon
    if level != 0:
        return 'error', 'Invalid syntax'
    else:
        tf, kwarg, value = is_kwarg(st)
        if tf == 'error':
            return tf, kwarg 
        elif tf == 'arg':
            args.append(st)
        elif tf == 'kwarg':
            kwargs[kwarg] = value
      
    return args, kwargs


def is_kwarg(st):

    ''' Given a string identify if it is a kwarg or not

    .. arguments:
    - (st) string : string we want to check

    .. returns:
    - ((id, val_1, val_2):
        (id) string: arg, kwarg or error
        (val_1) string: when st is a kwarg, its name, when it is an arg, None, and when
            it is an error, description of the error
        (val_2) string: value of the kwarg if kwarg, None if arg or error

    '''

    for ind, elem in enumerate(st):
        if elem in ['(', ')']:
            return 'arg', st, None
        if elem == '=':
            if ind == 0:
                return 'error', 'Invalid syntax', None                
            elif len(st) < ind + 2:
                return 'error', 'Invalid syntax', None
            else:
                return 'kwarg', st[:ind], st[ind + 1:]

    return 'arg', None, None


