# --------------------------------------------------------------------
# Author: Francesc Torradeflot - <ciscu@nomorecode.com>
#
# Description:
# Define methods for loading and managing data using
# Pandas DataFrames
#
# --------------------------------------------------------------------
# Copyright (c) 2014 - All Rights Reserved.
#
# This source is subject to the Nomorecode Source License.
# Please see the License.md file for more information, which is
# part of this source code package.
# --------------------------------------------------------------------

# Imports and defines.

import pandas as pd
import inspect as ip
import numpy as np
from copy import deepcopy
import json
import time

import analysis_functions as af
import analysis_utils as au
from common.util import type_conversion
from common.constants import TimeInSeconds


# ----------------------- Wrapper for functions on lists of timeseries --------------------

def ts_list_function():
    def decorate(func):
        def call(*args, **kwargs):

            for elem in args:

                cts = check_ts_list(elem)
                if 'error' in cts:
                    return cts

            for elem in kwargs:
                if elem not in ip.getargspec(func)[0]:
                    return {'error': 'unknown argument %s' %elem}

            result = func(*args, **kwargs)            
            return result
        return call
    return decorate


def check_ts_list(ts_list):

    if type(ts_list) != list:
        if 'error' in ts_list:
            return ts_list
        else:
            return {'error': 'Not a list of timeseries'}

    for ts in ts_list:
        #check if it is a timeserie
        ct = check_ts(ts)
        if 'error' in ct:
            return ct
    
    return {'success': 1}


def check_ts(ts):

    if not type(ts) == pd.core.frame.DataFrame:
        return {'error': 'Element is not a timeserie: DataFrame expected'}

    if ts.index.values.dtype not in [np.int64]:
        return {'error': 'Element is not a timeserie: Integer index required'}

    if not ts.index.is_unique:
        return {'error': 'Non unique index'}

    if not np.greater_equal(ts.index.values, 0).all():
        return {'error': 'Element is not a timeserie: Non positive values in index'}

    if not len(ts.columns) == 1:
        return {'error': 'Element is not a timeseries: One column required'}

    if not 'value' in ts.columns:
        return {'error': 'Element is not a timeseries: value column required'}

    return {'success': 1}


def call_ts_func(ts_func):

    def call(ts_list, **kwargs):
        output = []
        for ts in ts_list:
            result = ts_func(ts, **kwargs)

            if 'error' in result:
                return result
            output.append(result)

        return output
    return call

# ----------------------- Basic functionalities of timeseries -------------------------------

def get_variable(id_variable, time_int = 300, expand = True, now = None, 
        distr = True, int_type = 'left_open', fill_value = None, **kwargs):

    ''' Given the id of an eyecode variable return a list containing
        one timeseries DataFrame meeting the arguments and the kwargs

    .. arguments:
    - (id_variable) integer : id of the eyecode variable
    - (time_int) integer: length of the time intervals of the timeseries in seconds
    - (expand) boolean: expand the data of timeseries to the whole range required 
        or restrict the output to the range of data received
    - (now) integer: epoch that represents the moment when the query is performed
    - (distr) boolean: indicates if we want the data to be distributed among intervals
        of length time_int or not
    - (int_type) string: type of the interval of data we want to get
    - kwargs: arguments of the column_range function in analysis_utils

    .. returns:
    - (new_ts) Pandas DataFrame containing a timeserie distributed to "seconds" intervals
        and filtering the last "count" values from e_from to e_to 

    '''
    # Convert input parameters to their data formats
    try:
        time_int = int(time_int)
        expand = type_conversion(expand, 'BOOLEAN')['success']
        distr = type_conversion(distr, 'BOOLEAN')['success']
    except:
        return {'error': 'parameters do not have required format'}


    # the restriction to the number of values will be applied once we 
    # have the timeseries. We arrange the count parameter to a value
    # that we can be sure that is not restrictive.
    cc = kwargs.get('count', False)
    if cc:
        try:
            cc = int(cc)
        except:
            return {'error': 'count argument is not an integer: {!s}'.format(cc)}
        kwargs['count'] = cc*time_int

    if now == None:
        now = int(time.time())
    else:
        try:
            now = int(now)
        except:
            return {'error': 'time reference received is not an epoch'}
    time_ref = time_int*int(now/time_int) #now - time_int

    # translate the arguments given to column_start, column_finish, column_count format
    column_range = au.column_range(kwargs, now = time_ref, int_type = int_type)
    
    if 'error' in column_range: return column_range

    # Get the data of the given variable in the time interval wanted
    data_list = af.get_variable_data(id_variable, column_range)
    if 'error' in data_list: return data_list

    # Convert the cassandra timeserie to a list containing a panda's dataframe
    ts_list = cassandra_to_ts_list(data_list, 'value')

    # expand results to the whole range demanded
    cs = column_range.get('column_start', False)
    if (not cs == False) and expand:
        # Handle possible huge values
        qTo = min(cs[1], time.time() + TimeInSeconds.YEAR)
    else:
        qTo = False

    cf = column_range.get('column_finish', False)
    if (not cf == False) and expand:
        # Handle possible very small values
        qFrom = max(cf[1], 1356994800) # 1/1/2013
    else:
        qFrom = False

    # If we have not defined a column_count and there's 
    # a column_count established by the column_range function
    # we use it
    ccount = column_range.get('column_count', False)
    if cc == False and ccount != False:
        cc = ccount

    # Distribute the timeseries
    if distr:
        ts_list = distribute_ts_list(ts_list, seconds = time_int, e_to = qTo, e_from = qFrom, fill_value = fill_value)
    
    # Return last cc number of values
    if cc:
        ts_list = last(ts_list, number = cc)

    return ts_list


def cassandra_to_ts_list(ts, column_name = 'value'):
    ''' Converts a collection of 1 timeserie from cassandra, that is 
    [[(epoch, value),... (epoch, value)]], to a list containing 
    one pandas DataFrame'''

    df = []
    
    for i in range(len(ts)):
    
        ts_epoch = [elem[0] for elem in ts[i]]
        ts_values = [elem[1] for elem in ts[i]]

        df.append(pd.DataFrame({column_name : ts_values}, index = ts_epoch))
        
    return df


# --------------------------------- Timeseries distribution -----------------------------------

@ts_list_function()
def distribute_ts_list(ts_list, seconds = 300, e_to = False, e_from = False, fill_value = None):

    ''' Apply distribute_ts to each of the timeseries in ts_list'''
    if e_to:
        try:
            e_to = int(e_to)
        except:
            return {'error': 'e_to must be an epoch'}
        
    if e_from:
        try:
            e_from = int(e_from)
        except:
            return {'error': 'e_from must be an epoch'}

    try:
        seconds = int(seconds)
    except:
        return {'error': 'seconds must be an integer'}        

    distributed_ts_list = []
    for elem in ts_list:
        new_elem = distribute_ts(elem, seconds, e_to, e_from, fill_value)
        if 'error' in new_elem:
            return new_elem
        distributed_ts_list.append(new_elem)

    return distributed_ts_list


def distribute_ts(ts, seconds = 300, e_to = False, e_from = False, fill_value = None):

    ''' Given a timeseries Dataframe we will reindex it to epochs 
        that are multiples of "seconds" the values will be in 
        first place distributed forward and then backwards      

    .. arguments:
    - (ts) DataFrame : timeseries DataFrame
    - (seconds) integer: number of seconds to which we want to distribute the timeserie
    - (e_to) integer: epoch of the first moment to which expand the timeserie. If
        not given we will use the first epoch of the timeserie
    - (e_from) integer: epoch of the last moment to which expand the timeserie.
        If not given we will use the last epoch of the timeserie

    .. returns:
    - (new_ts) Pandas DataFrame containing a timeserie distributed to "seconds" intervals
        and filtering the last "count" values from e_from to e_to 

    '''

    new_ts = ts.copy()

    # pick initial and final epochs if given
    if not e_from:
        e_from = new_ts.index.values[0]
    
    if not e_to:
        e_to = new_ts.index.values[-1]

    # truncate them to multiples of seconds
    new_e_from = seconds*(int(e_from/seconds))
    if (e_from % seconds) != 0: new_e_from += seconds
    new_e_to = seconds*(int(e_to/seconds) + 1)
    if (e_to % seconds) != 0: new_e_to += seconds

    # build new index
    new_index = [i for i in range(new_e_from, new_e_to, seconds)]

    # apply new index, forward
    if fill_value != None:
        new_ts = new_ts.reindex(index = new_index, fill_value = fill_value)
    else:
        new_ts = new_ts.reindex(index = new_index, method= 'pad')
    
        # and backwards
        new_ts.fillna(method = 'bfill', inplace = True)
    
    return new_ts


# --------------------------------- Timeseries increments -----------------------------------

@ts_list_function()
def increments(ts_list, monotony = 'increasing', max_value = None, reset_value = 0.):

    output = []

    for elem in ts_list:
        inc = ts_increments(elem, monotony = monotony, max_value = max_value, reset_value = reset_value)
        if 'error' in inc:
            return inc
        output.append(inc)

    return output


def ts_increments(ts, monotony = 'increasing', max_value = None, reset_value = 0.):

    '''Return a timeserie with the increments registered in the 
        input timeserie

    .. arguments:
    - (list) ts: pandas DataFrame containing a timeserie
    - (string) monotony: increasing / decreasing / non_monotonous
    - (float) max_value: value from which the meter is reseted
    - (float) reset_value: value to which the meter is reseted

    .. returns:
    - on success: timeseries of increments. The output timeseries contains 
        one value less than the original one. The diference between 
        two values, is assigned to the epoch of the second one.'''

    new_ts = ts_to_float(ts)

    if 'error' in new_ts:
        return new_ts

    if len(new_ts) <= 1:
        return {'error': 'timeserie must have length greater than 1 to compute increments'}

    if max_value != None:
        try:
            max_value = float(max_value)
        except:
            return {'error': 'max_value is not a number'}

    try:
        reset_value = float(reset_value)
    except:
        return {'error': 'reset_value is not a number'}

    if monotony == 'increasing':
        if not np.greater_equal(new_ts['value'], reset_value).all():
            return {'error': 'value lower than reset_value'}
        elif max_value and not np.less_equal(new_ts['value'], max_value).all():
            return {'error': 'value greater than max_value'}
    elif monotony == 'decreasing':
        if not np.less_equal(new_ts['value'].values, reset_value).all():
            return {'error': 'value greater than reset value'}
        elif max_value and not np.greater_equal(new_ts['value'], max_value).all():
            return {'error': 'value lower than max_value'}

    new_ts['old_value'] = new_ts['value'].shift()

    new_ts = new_ts.drop(new_ts.index[0])

    new_ts['increments'] = new_ts.apply(single_inc, axis = 1, monotony = monotony, \
        max_value = max_value, reset_value = reset_value)

    output_ts = pd.DataFrame()
    output_ts['value'] = new_ts['increments']

    return output_ts


def single_inc(row, monotony = 'increasing', max_value = None, reset_value = 0.):

    # Handle the meter resets in an incremental meter
    if row['old_value'] > row['value'] and monotony == 'increasing':
        value = row['value'] - reset_value
        if max_value != None:
            value += max_value - row['old_value']
    # Handle the meter resets in a decremental meter
    elif row['old_value'] < row['value'] and monotony == 'decreasing':
        value = row['value'] - reset_value
        if max_value != None:
            value += max_value - row['old_value']
    else:
        value = row['value'] - row['old_value']
    
    return value


def ts_to_float(ts):

    new_ts = pd.DataFrame()
    try:
        new_ts['value'] = ts.apply(lambda x: float(x['value']), axis = 1)
        return new_ts
    except:
        return {'error': 'Non scalar values found'}



# ------------------------------Aggregate functions --------------------------------
# ------------------------ wrappers for aggregate functions ------------------------

def aggregate_func():

    def wrapper(agg_func):

        def call(ts):

            new_ts = ts_to_float(ts)

            if 'error' in new_ts:
                return new_ts

            value = agg_func(new_ts)

            epoch = new_ts.index.values[-1]

            output_ts = pd.DataFrame([value], columns = ['value'], index = [epoch])

            return output_ts
        return call
    return wrapper

def merge_agg_func(func):

    def call(ts_list, *args, **kwargs):
        output = pd.DataFrame(columns = ['value'], dtype = 'float64')
        for elem in ts_list:
            result = func(elem, *args, **kwargs)
            if 'error' in result:
                return result
            output = output.append(result)

        if not output.index.is_unique:
            return {'error': 'Non unique index'}

        return [output]
    return call

            

# --------------------------------- inner_sum -----------------------------------
@ts_list_function()
def inner_sum(ts_list):

    return merge_agg_func(ts_inner_sum)(ts_list)

@aggregate_func()
def ts_inner_sum(ts):

    '''Perform the sum of all the elements in a timeserie

    .. arguments:
    - (DataFrame) ts: pandas DataFrame containing a timeserie

    .. returns:
    - on success: timeseries with only one row, with the index of
        the last element of the original serie and the value of the sum'''

    return np.sum(ts['value'])


# --------------------------------- max -----------------------------------
@ts_list_function()
def inner_max(ts_list):

    return merge_agg_func(ts_max)(ts_list)

@aggregate_func()
def ts_max(ts):

    '''Find the maximum of the elements in a timeserie

    .. arguments:
    - (DataFrame) ts: pandas DataFrame containing a timeserie

    .. returns:
    - on success: timeseries with only one row, with the index of
        the last element of the original serie and the value of the maximum'''

    return np.amax(ts['value'])



# --------------------------------- min -----------------------------------
@ts_list_function()
def inner_min(ts_list):

    return merge_agg_func(ts_min)(ts_list)

@aggregate_func()
def ts_min(ts):

    '''Find the minimum of the elements in a timeserie

    .. arguments:
    - (DataFrame) ts: pandas DataFrame containing a timeserie

    .. returns:
    - on success: timeseries with only one row, with the index of
        the last element of the original serie and the value of the minimum'''

    return np.amin(ts['value'])


# --------------------------------- mean -----------------------------------
@ts_list_function()
def inner_mean(ts_list):

    return merge_agg_func(ts_mean)(ts_list)

@aggregate_func()
def ts_mean(ts):

    '''Perform the mean of the elements in a timeserie

    .. arguments:
    - (DataFrame) ts: pandas DataFrame containing a timeserie

    .. returns:
    - on success: timeseries with only one row, with the index of
        the last element of the original serie and the value of the mean'''

    return np.mean(ts['value'])


# ------------------------- standard deviation -----------------------------------
@ts_list_function()
def inner_std(ts_list):

    return merge_agg_func(ts_std)(ts_list)

@aggregate_func()
def ts_std(ts):

    '''Perform the mean of the elements in a timeserie

    .. arguments:
    - (DataFrame) ts: pandas DataFrame containing a timeserie

    .. returns:
    - on success: timeseries with only one row, with the index of
        the last element of the original serie and the value of the mean'''

    return np.std(ts['value'])


# ------------------------- last -----------------------------------
@ts_list_function()
def last(ts_list, number = 1):

    return merge_agg_func(ts_last)(ts_list, number = number)


def ts_last(ts, number = 1):

    '''Return the last element in a timeserie

    .. arguments:
    - (DataFrame) ts: pandas DataFrame containing a timeserie

    .. returns:
    - on success: timeseries with only one row, with the last element of the original timeserie'''

    if len(ts) < number:
        return ts

    value = ts.iloc[-number:]['value'].values

    epoch = ts.index.values[-number:]

    output_ts = pd.DataFrame(value, columns = ['value'], index = epoch)

    return output_ts


# --------------------------------- Functions with numbers ----------------------------------
# ---------------------------- Wrapper for functions with numbers-----------------------------
def scalar_func():

    def wrapper(func):
        def call(ts, number):

            new_ts = ts_to_float(ts)

            if 'error' in new_ts:
                return new_ts

            try:
                number = float(number)
            except:
                return {'error': 'number is not numeric'}

            new_ts['value'] = func(new_ts, number)

            new_ts.replace([np.inf, -np.inf], np.nan, inplace = True)
            new_ts.dropna(inplace = True)

            return new_ts
        return call
    return wrapper

        
# --------------------------------- Timeseries scalar product -----------------------------------
@ts_list_function()
def scalar_product(ts_list, number = 1.):

    return call_ts_func(ts_scalar_product)(ts_list, number = number)


@scalar_func()
def ts_scalar_product(ts, number = 1.):

    '''Perform the product of a timeserie and a number

    .. arguments:
    - (list) ts: pandas DataFrame containing a timeserie
    - (float) number

    .. returns:
    - on success: timeseries'''

    return ts['value']*number


# --------------------------------- Timeseries scalar sum -----------------------------------
@ts_list_function()
def scalar_sum(ts_list, number = 0.):

    return call_ts_func(ts_scalar_sum)(ts_list, number = number)


@scalar_func()
def ts_scalar_sum(ts, number = 0.):

    '''Perform the sum of a timeserie and a number

    .. arguments:
    - (list) ts: pandas DataFrame containing a timeserie
    - (float) number

    .. returns:
    - on success: timeseries'''

    return number + ts['value']


# --------------------------------- Timeseries scalar division -----------------------------------
@ts_list_function()
def scalar_division(ts_list, number = 1.):

    return call_ts_func(ts_scalar_division)(ts_list, number = number)


@scalar_func()
def ts_scalar_division(ts, number = 1.):

    '''Perform the division of a timeserie by a number

    .. arguments:
    - (list) ts: pandas DataFrame containing a timeserie
    - (float) number

    .. returns:
    - on success: timeseries'''

    return ts['value']/number


# ----------------------------- Timeseries scalar subtraction -------------------------------
@ts_list_function()
def scalar_sub(ts_list, number = 0.):

    return call_ts_func(ts_scalar_sub)(ts_list, number = number)


@scalar_func()
def ts_scalar_sub(ts, number = 0.):

    '''Perform the subtraction of a number from a timeserie

    .. arguments:
    - (list) ts: pandas DataFrame containing a timeserie
    - (float) number

    .. returns:
    - on success: timeseries'''

    return ts['value'] - number


# ----------------------------- Timeseries scalar power -------------------------------
@ts_list_function()
def scalar_power(ts_list, number = 1.):

    number = int(number)
    return call_ts_func(ts_scalar_power)(ts_list, number = number)


@scalar_func()
def ts_scalar_power(ts, number = 1.):

    '''Raise timeserie to the exponent number

    .. arguments:
    - (list) ts: pandas DataFrame containing a timeserie
    - (float) number

    .. returns:
    - on success: timeseries'''

    return np.power(ts['value'], number)


# -------------------- Basic mathematical operations between timeseries lists -----------------
# ---------------------- Addition, subtraction, product and division --------------------------

def ts_pair_operation():

    def wrapper(func):
        def f(ts_1, ts_2):

            ts_1 = ts_to_float(ts_1)
            if 'error' in ts_1: return ts_1

            ts_2 = ts_to_float(ts_2)
            if 'error' in ts_2: return ts_2

            new_ts = pd.DataFrame()

            l_1 = len(ts_1['value'])
            l_2 = len(ts_2['value'])

            if (l_1 == 1 and l_2 == 1) or (l_1 != 1 and l_2 != 1):
                new_ts['value'] = func(ts_1['value'], ts_2['value'])
            elif l_1 == 1:
                number = ts_1['value'].iloc[0]
                new_ts['value'] = func(number, ts_2['value']) 
            elif l_2 == 1:
                number = ts_2['value'].iloc[0]
                new_ts['value'] = func(ts_1['value'], number)

            new_ts.dropna(inplace = True)

            return new_ts
        return f
    return wrapper


# --------------------------------- addition -----------------------------------
@ts_list_function()
def addition(*ts_lists):

    ''' Perform a sum of timeseries lists

        Given a set of timeseries lists, this function
        will return a timeseries list where each timeserie 
        is the sum of the timeseries of each input timeseries list
        placed in the same position of the list
        
        Rows with non-coincident indexes will be discarded

    .. arguments:
        ts_lists = ts_list_1, ..., ts_list_n a list of timeseries lists 
            with the same length
            ts_list_1 = [ts_1_1, ..., ts_1_m]
            ...
            ts_list_n = [ts_n_1, ..., ts_n_m]
    
    .. returns:
        on success: ts_list = [ts_1_1 + ... + ts_n_1, ..., ts_1_m + ... + ts_n_m]
    '''

    l = len(ts_lists)

    if l <= 1:
        return {'error': 'Addition requires at least two arguments'}
    else:
        ts_list_ref = deepcopy(ts_lists[0])
        l_ref = len(ts_list_ref)

        for i in range(1, l):
            if len(ts_lists[i]) != l_ref:
                return {'error': 'Timeseries lists must have the same dimension'}
            else:
                for j in range(l_ref):
                    r = ts_addition(ts_list_ref[j], ts_lists[i][j])
                    if 'error' in r:
                        return r
                    ts_list_ref[j] = r

        return ts_list_ref


@ts_pair_operation()
def ts_addition(ts_1, ts_2):

    '''Perform the sum of two timeseries
        If only one of the timeseries has length == 1,
        this will be treated as a scalar sum

    .. arguments:
    - (DataFrame) ts_1: pandas DataFrame containing a timeserie
    - (DataFrame) ts_2: pandas DataFrame containing a timeserie

    .. returns:
    - on success: timeseries containing the sum of ts_1 and ts_2'''

    return ts_1 + ts_2


# --------------------------------- subtraction -----------------------------------
@ts_list_function()
def subtraction(ts_list_1, ts_list_2):

    ''' Perform a subtraction of two timeseries lists: ts_list_1 - ts_list_2

        Given two timeseries lists, this function
        will return a timeseries list where each timeserie 
        is the difference between the timeseries
        placed in the same position of the list
        
        Rows with non-coincident indexes will be discarded

    .. arguments: two timeseries lists with the same length
        ts_list_1 = [ts_1_1, ..., ts_1_m]
        ts_list_2 = [ts_2_1, ..., ts_2_m]
    
    .. returns:
        on success: ts_list = [ts_1_1 - ts_2_1, ..., ts_1_m - ts_2_m]
    
    '''

    l_1 = len(ts_list_1)
    l_2 = len(ts_list_2)

    if l_1 != l_2:
        return {'error': 'Subtraction - Timeseries list must have same dimension'}
    
    new_ts_list = []

    for i in range(l_1):
        r = ts_subtraction(ts_list_1[i], ts_list_2[i])
        if 'error' in r:
            return r
        new_ts_list.append(r)

    return new_ts_list


@ts_pair_operation()
def ts_subtraction(ts_1, ts_2):

    '''Perform the subtraction of two timeseries

    .. arguments:
    - (DataFrame) ts_1: pandas DataFrame containing a timeserie
    - (DataFrame) ts_2: pandas DataFrame containing a timeserie

    .. returns:
    - on success: timeseries containing the difference of ts_1 and ts_2'''

    return ts_1 - ts_2


# --------------------------------- product -----------------------------------
@ts_list_function()
def product(ts_list_1, ts_list_2):

    ''' Perform a product of two timeseries lists

        Given two timeseries lists, this function
        will return a timeseries list where each timeserie 
        is the sum of the timeseries of each input timeseries list
        placed in the same position of the list
        
        Rows with non-coincident indexes will be discarded

    .. arguments: two timeseries lists with the same length
        ts_list_1 = [ts_1_1, ..., ts_1_m]
        ts_list_2 = [ts_2_1, ..., ts_2_m]
    
    .. returns:
        on success: ts_list = [ts_1_1 * ts_2_1, ..., ts_1_m * ts_2_m]
    
    '''

    l_1 = len(ts_list_1)
    l_2 = len(ts_list_2)

    if l_1 != l_2:
        return {'error': 'Product - Timeseries list must have same dimension'}
    
    new_ts_list = []

    for i in range(l_1):
        r = ts_product(ts_list_1[i], ts_list_2[i])
        if 'error' in r:
            return r
        new_ts_list.append(r)

    return new_ts_list


@ts_pair_operation()
def ts_product(ts_1, ts_2):

    '''Perform the product of two timeseries

    .. arguments:
    - (DataFrame) ts_1: pandas DataFrame containing a timeserie
    - (DataFrame) ts_2: pandas DataFrame containing a timeserie

    .. returns:
    - on success: timeseries containing the product of ts_1 and ts_2'''

    return ts_1 * ts_2


# --------------------------------- division -----------------------------------
@ts_list_function()
def division(ts_list_1, ts_list_2):

    ''' Perform a division of two timeseries lists

        Given two timeseries lists, this function
        will return a timeseries list where each timeserie 
        is the division of the timeseries of each input timeseries list
        placed in the same position of the list
        
        Rows with non-coincident indexes will be discarded
        Rows with NaN values because ts_list_2 is zero will be discarded

    .. arguments: two timeseries lists with the same length
        ts_list_1 = [ts_1_1, ..., ts_1_m]
        ts_list_2 = [ts_2_1, ..., ts_2_m]
    
    .. returns:
        on success: ts_list = [ts_1_1 / ts_2_1, ..., ts_1_m / ts_2_m]
    
    '''

    l_1 = len(ts_list_1)
    l_2 = len(ts_list_2)

    if l_1 != l_2:
        return {'error': 'Division - Timeseries list must have same dimension'}
    
    new_ts_list = []

    for i in range(l_1):
        r = ts_division(ts_list_1[i], ts_list_2[i])
        if 'error' in r:
            return r
        new_ts_list.append(r)

    return new_ts_list


@ts_pair_operation()
def ts_division(ts_1, ts_2):

    '''Perform the division of two timeseries
        infinity values are replaced by NaN and dropped

    .. arguments:
    - (DataFrame) ts_1: pandas DataFrame containing a timeserie
    - (DataFrame) ts_2: pandas DataFrame containing a timeserie

    .. returns:
    - on success: timeseries containing the division of ts_1 and ts_2'''

    ts_3 = ts_1 / ts_2

    ts_3.replace([np.inf, -np.inf], np.nan, inplace = True)

    return ts_3


# -------------------------------------------------------------------------------------
# ------------------------------------- Timeseries splitting function ------------------

@ts_list_function()
def split(ts_list_1, period = 'day'):

    ''' Split each of the timeseries in a timeseries list
        in smaller time series corresponding to periods
        and return only one timeseries list with the result

    .. arguments: 
        (list) ts_list_1: list with the timeseries we want to split
        (string) period: name of the periods in which we want to split the data.
            year, month, week, day and hour supported
    
    .. returns:
        on success: splitted timeseries list
    
    '''
    if period not in ['year', 'month', 'week', 'day', 'hour']:
        return {'error': 'Invalid period given: %s' %str(period)}

    l_1 = len(ts_list_1)

    new_ts_list = []

    for i in range(l_1):
        r = ts_split(ts_list_1[i], period = period)
        cts = check_ts_list(r)
        if 'error' in cts:
            return cts
        new_ts_list += r

    return new_ts_list


def ts_split(ts, period = 'day'):

    ''' Split one single timeseries in the periods specified.
        returns a timeseries list containing the splitted timeseries

    .. arguments:
        (DataFrame) ts: pandas DataFrame containing a timeserie
        (string) period: name of the periods in which we want to split the data.
            year, month, week, day and hour supported

    .. returns:
    - on success: timeseries list containing the splitted timeseries'''

    splitted_ts = []

    ts['split'] = ts.index.map(lambda x: au.time_interval_beginning(period, epoch_ref = x))

    grouped_ts = ts.groupby(['split'])

    for name, group in grouped_ts:
        help_ts = pd.DataFrame(group)
        help_ts = help_ts.drop('split', axis = 1)
        splitted_ts.append(help_ts)

    return splitted_ts
    

# --------------------------- Data generation for testing purposes ------------------------------
def generate_ts_list(data):

    ts_list = []

    try:
        tsl_data = json.loads(data)
    except:
        return {'error': 'Unable to load : %s' %data}

    if type(tsl_data) != list:
        return {'error': 'data received is not a list'}

    for ts_data in tsl_data:
        ts_value = ts_data.get('value', False)
        ts_index = ts_data.get('index', False)
        if not ts_value or not ts_index:
            return {'error': 'incorrect data received'}
        ts_list.append(pd.DataFrame(ts_value, columns = ['value'], index = ts_index))

    return ts_list


# ---------------------------- timeseries list to list ------------------------
@ts_list_function()
def ts_list_to_list(ts_list):

    ''' Iterate over the DataFrames in ts_list and convert them to lists
        as specified in df_to_list'''

    output = []

    for df in ts_list:
        output.append(df_to_list(df))

    return output


def df_to_list(df):

    ''' Given a DataFrame as the ones contained in timeseries lists,
        convert it to a list with the following structure:
        [[epoch_1, value_1], ..., [epoch_n, value_n]]

    .. arguments:
        (DataFrame) df: pandas DataFrame containing a timeserie

    .. returns:
        - on success: list containing the splitted timeseries'''


    l1 = df.index.values.tolist()
    l2 = df['value'].values.tolist()

    l = [[l1[i], l2[i]] for i in range(len(l1))]

    return l
    
# ---------------------------- usage ------------------------
# --------------------------------------------------------------------
def get_increments(id_variable, time_int = 300, expand = True, now = None, 
        distr = True, **kwargs):

    ''' Given the id of an eyecode variable return a list containing
        one timeseries DataFrame with the increments registerd by that variable,
        meeting the arguments and the kwargs

    .. arguments:
    - (id_variable) integer : id of the eyecode variable
    - (time_int) integer: length of the time intervals of the timeseries in seconds
    - (expand) boolean: expand the data of timeseries to the whole range required 
        or restrict the output to the range of data received
    - (now) integer: epoch that represents the moment when the query is performed
    - (distr) boolean: indicates if we want the data to be distributed among intervals
        of length time_int or not
    - kwargs: arguments of the column_range function in analysis_utils

    .. returns:
    - (new_ts) Pandas DataFrame containing a timeserie distributed to "seconds" intervals
        and filtering the last "count" values from e_from to e_to 

    '''
    # Convert input parameters to their data formats
    try:
        time_int = int(time_int)
        expand = type_conversion(expand, 'BOOLEAN')['success']
        distr = type_conversion(distr, 'BOOLEAN')['success']
    except:
        return {'error': 'parameters do not have required format'}


    # the restriction to the number of values will be applied once we 
    # have the timeseries. We arrange the count parameter to a value
    # that we can be sure that is not restrictive.
    cc = kwargs.get('count', False)
    if cc:
        try:
            cc = int(cc)
        except:
            return {'error': 'count argument is not an integer: {!s}'.format(cc)}
        kwargs['count'] = cc*time_int

    if now == None:
        now = int(time.time())
    else:
        try:
            now = int(now)
        except:
            return {'error': 'time reference received is not an epoch'}
    time_ref = time_int*int(now/time_int) #now - time_int

    # translate the arguments given to column_start, column_finish, column_count format
    column_range = au.column_range(kwargs, now = time_ref, int_type = 'closed')
    if 'error' in column_range: return column_range

    # Get the data of the given variable in the time interval wanted
    data_list = af.get_variable_data(id_variable, column_range)
    if 'error' in data_list: return data_list

    # Get extra value when first value is not coincident with column_finish
    # If we did it always we might get an extra increment when not distributing
    extra_cf = data_list[0][0][0]
    if extra_cf != column_range.get('column_finish', ('timeseries', False))[1]:
        extra_cf -= 1
        extra_column_range = {'column_start':('timeseries', extra_cf), 'column_count': 1}
        extra_value = af.get_variable_data(id_variable, extra_column_range)
        if not 'error' in extra_value:
            data_list[0].insert(0, extra_value[0][0])

    # Convert the cassandra timeserie to a list containing a panda's dataframe
    ts_list = cassandra_to_ts_list(data_list, 'value')

    # expand results to the whole range demanded
    cs = column_range.get('column_start', False)
    if (not cs == False) and expand:
        qTo = cs[1]
    else:
        qTo = False

    cf = column_range.get('column_finish', False)
    if (not cf == False) and expand:
        qFrom = cf[1]
    else:
        qFrom = False

    # If we have not defined a column_count and there's 
    # a column_count established by the column_range function
    # we use it
    ccount = column_range.get('column_count', False)
    if cc == False and ccount != False:
        cc = ccount

    # Distribute the timeseries
    if distr:
        ts_list = distribute_ts_list(ts_list, seconds = time_int, e_to = qTo, e_from = qFrom)

    # Compute the increments
    ts_list = increments(ts_list)
    
    # Return last cc number of values
    if cc:
        ts_list = last(ts_list, number = cc)

    return ts_list


def usage(id_variable, **kwargs):

    if 'group_by' in kwargs:
        return inner_sum(split(get_increments(id_variable, **kwargs), period = kwargs['group_by']))
    else:
        return inner_sum(get_increments(id_variable, **kwargs))


    
